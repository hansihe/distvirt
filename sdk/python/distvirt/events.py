"""Event stream handling and internal object model updates."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, AsyncIterator


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

    def apply_status(self, status: Any) -> None:
        """Bootstrap model from a GetNamespaceStatus response.

        Called once on Namespace construction to seed the model before
        the event stream takes over.
        """
        # TODO: convert proto NamespaceStatusReport into WorkloadModel/ServiceModel entries
        raise NotImplementedError

    def apply_event(self, event: Any) -> None:
        """Update model from a StreamEvents event.

        Maps proto event types to model mutations:
        - WorkloadPodLaunching  → state="launching", set pod_id/worker_id
        - WorkloadPodRunning    → state="running"
        - WorkloadPodStopped    → state="dormant" or "completed" (depends on RunPolicy)
        - WorkloadPodFailed     → state="failed"
        - WorkloadPodSuspending → state="suspending"
        - WorkloadPodSuspended  → state="suspended"
        - WorkloadPodResuming   → state="launching"
        - ServiceActivated      → state="active"
        - ServiceDeactivated    → state="idle"
        - etc.

        After applying, notifies any matching waiters.
        """
        # TODO: implement event → model mutation
        raise NotImplementedError


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

    async def __anext__(self) -> Any:
        # TODO: read next event from gRPC stream, convert to Python event type
        raise NotImplementedError
