"""Event stream handling and internal object model updates."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, AsyncIterator

import betterproto

from distvirt._proto.distvirt.client.v1 import (
    NamespaceEvent,
    NamespaceStatusReport,
    WorkloadState,
    ServiceState,
)


def _workload_state_name(state: WorkloadState) -> str:
    """Extract the state name from a WorkloadState oneof."""
    variant, _ = betterproto.which_one_of(state, "state")
    if variant == "":
        return "unknown"
    # Map proto variant names to SDK state names
    _MAP = {
        "dormant": "dormant",
        "waiting_for_spec": "dormant",
        "launching": "launching",
        "running": "running",
        "suspending": "suspending",
        "suspended": "suspended",
        "retry_backoff": "failed",
        "failed": "failed",
        "completed": "completed",
    }
    return _MAP.get(variant, "unknown")


def _service_state_name(state: ServiceState) -> str:
    """Extract the state name from a ServiceState oneof."""
    variant, _ = betterproto.which_one_of(state, "state")
    if variant == "":
        return "unknown"
    _MAP = {
        "pending": "idle",
        "idle": "idle",
        "need_backend": "active",
        "active": "active",
    }
    return _MAP.get(variant, "unknown")


@dataclass
class WorkloadModel:
    """Live state model for a workload, updated from event stream."""

    workload_id: str
    state: str = "unknown"
    pod_id: str | None = None
    worker_id: str | None = None
    spliced: bool = False


@dataclass
class ServiceModel:
    """Live state model for a service, updated from event stream."""

    service_id: str
    workload_id: str
    state: str = "unknown"
    ip: str | None = None
    activation_enabled: bool = False
    spliced: bool = False


@dataclass
class NamespaceModel:
    """Live object model for a namespace, maintained from event stream."""

    namespace_id: str
    workloads: dict[str, WorkloadModel] = field(default_factory=dict)
    services: dict[str, ServiceModel] = field(default_factory=dict)

    # Waiters: list of (predicate, future) pairs. The background event loop
    # checks all predicates after each model update and resolves matching futures.
    _waiters: list[tuple[Any, asyncio.Future]] = field(
        default_factory=list, repr=False
    )

    def _notify_waiters(self) -> None:
        """Check all waiters against current state, resolve any that match."""
        remaining = []
        for predicate, future in self._waiters:
            if future.done():
                continue
            if predicate(self):
                future.set_result(None)
            else:
                remaining.append((predicate, future))
        self._waiters = remaining

    def apply_status(self, status: NamespaceStatusReport) -> None:
        """Bootstrap model from a GetNamespaceStatus response.

        Called once on Namespace construction to seed the model before
        the event stream takes over.
        """
        self.workloads.clear()
        for wl_id, wl_report in status.workloads.items():
            state_name = _workload_state_name(wl_report.state)
            pod_id = None
            worker_id = None
            # Extract pod/worker from states that carry them
            variant, value = betterproto.which_one_of(wl_report.state, "state")
            if hasattr(value, "pod_id"):
                pod_id = value.pod_id or None
            if hasattr(value, "worker_id"):
                worker_id = value.worker_id or None
            self.workloads[wl_id] = WorkloadModel(
                workload_id=wl_id,
                state=state_name,
                pod_id=pod_id,
                worker_id=worker_id,
                spliced=wl_report.spliced,
            )

        self.services.clear()
        for svc_id, svc_report in status.services.items():
            state_name = _service_state_name(svc_report.state)
            self.services[svc_id] = ServiceModel(
                service_id=svc_id,
                workload_id=svc_report.workload_id,
                state=state_name,
                ip=svc_report.ip or None,
                activation_enabled=svc_report.activation_enabled,
                spliced=svc_report.spliced,
            )

        self._notify_waiters()

    def _get_or_create_workload(self, workload_id: str) -> WorkloadModel:
        """Get existing workload model or create a new one."""
        if workload_id not in self.workloads:
            self.workloads[workload_id] = WorkloadModel(workload_id=workload_id)
        return self.workloads[workload_id]

    def apply_event(self, event: NamespaceEvent) -> None:
        """Update model from a StreamEvents event.

        Maps proto event types to model mutations, then notifies waiters.
        """
        kind, _ = betterproto.which_one_of(event, "event")

        if kind == "pod":
            self._apply_pod_event(event.pod)
        elif kind == "workload":
            self._apply_workload_event(event.workload)
        elif kind == "endpoint":
            self._apply_endpoint_event(event.endpoint)

        self._notify_waiters()

    def _apply_pod_event(self, pod: Any) -> None:
        """Apply a PodEvent to the model."""
        wl = self._get_or_create_workload(pod.workload_id)
        variant, value = betterproto.which_one_of(pod, "event")

        if variant == "created":
            wl.pod_id = pod.pod_id
        elif variant == "scheduled":
            wl.state = "launching"
            wl.pod_id = pod.pod_id
            wl.worker_id = value.worker_id
        elif variant == "running":
            wl.state = "running"
            wl.pod_id = pod.pod_id
            wl.worker_id = value.worker_id
        elif variant == "stopped":
            wl.state = "completed"
            wl.pod_id = None
            wl.worker_id = None
        elif variant == "failed":
            wl.state = "failed"
            wl.pod_id = None
            wl.worker_id = None
        elif variant == "suspending":
            wl.state = "suspending"
        elif variant == "suspended":
            wl.state = "suspended"
            wl.pod_id = None
        elif variant == "suspend_failed":
            # Revert to running — suspend didn't work
            wl.state = "running"
        elif variant == "resuming":
            wl.state = "launching"
            wl.worker_id = value.worker_id
        elif variant == "displaced":
            wl.state = "dormant"
            wl.pod_id = None
            wl.worker_id = None
        elif variant == "reaped":
            wl.state = "dormant"
            wl.pod_id = None
            wl.worker_id = None

    def _apply_workload_event(self, workload: Any) -> None:
        """Apply a WorkloadEvent to the model."""
        wl = self._get_or_create_workload(workload.workload_id)
        variant, value = betterproto.which_one_of(workload, "event")

        if variant == "spliced":
            wl.spliced = True
            wl.worker_id = value.worker_id
        elif variant == "unspliced":
            wl.spliced = False
        # demand_changed is informational, doesn't affect model state

    def _apply_endpoint_event(self, endpoint: Any) -> None:
        """Apply an EndpointEvent to the model."""
        svc_id = endpoint.service_id
        if not svc_id:
            return
        svc = self.services.get(svc_id)
        if svc is None:
            return

        variant, _ = betterproto.which_one_of(endpoint, "event")
        if variant == "activated":
            svc.state = "active"
        elif variant == "deactivated":
            svc.state = "idle"


class EventStream:
    """Async iterator over namespace events from StreamEvents RPC.

    This is the raw event stream exposed to users via `ns.events()`.
    Internally, the Namespace also consumes a parallel stream to
    maintain the object model.
    """

    def __init__(self, grpc_stream: Any):
        self._stream = grpc_stream

    def __aiter__(self) -> AsyncIterator:
        return self

    async def __anext__(self) -> NamespaceEvent:
        try:
            return await self._stream.__anext__()
        except StopAsyncIteration:
            raise
