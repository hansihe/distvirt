"""Namespace handle with live object model."""

from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator

from distvirt._proto.distvirt.client.v1 import DistvirtClientStub
from distvirt.events import EventStream, NamespaceModel, WorkloadModel, ServiceModel
from distvirt.workload import Workload
from distvirt.service import Service


class Namespace:
    """Live handle to a distvirt namespace.

    Maintains an internal object model kept up-to-date by a background
    event stream. State queries (status, workload state) read from the
    local model — no RPC needed.

    The event stream is opened eagerly on construction. This eliminates
    races between apply() and wait_for() — the stream captures all
    transitions from the moment the handle is created.

    Usage::

        ns = await dv.namespace("staging")
        await ns.workload("db").wait_for(distvirt.running)
        status = ns.status()
    """

    def __init__(
        self,
        namespace_id: str,
        stub: DistvirtClientStub,
        model: NamespaceModel | None = None,
    ):
        self._namespace_id = namespace_id
        self._stub = stub
        self._model = model or NamespaceModel(namespace_id=namespace_id)
        self._event_task: asyncio.Task | None = None

    @property
    def namespace_id(self) -> str:
        return self._namespace_id

    async def _start_event_loop(self) -> None:
        """Start the background event consumption loop.

        Opens a StreamEvents RPC and continuously applies events to the
        internal model. Runs as an asyncio task for the lifetime of
        the Namespace handle.
        """
        # TODO: open StreamEvents RPC, loop applying events to self._model
        # On each event: self._model.apply_event(event); self._model._notify_waiters()
        pass

    async def close(self) -> None:
        """Stop the background event loop and release resources."""
        if self._event_task is not None:
            self._event_task.cancel()
            try:
                await self._event_task
            except asyncio.CancelledError:
                pass
            self._event_task = None

    def status(self) -> NamespaceModel:
        """Return current namespace state from the live model.

        This is a synchronous read — no RPC. The model is kept
        current by the background event stream.
        """
        return self._model

    def workload(self, workload_id: str) -> Workload:
        """Get a handle to a workload in this namespace.

        Args:
            workload_id: The workload to reference.

        Returns:
            A Workload handle bound to this namespace's live model.
        """
        return Workload(
            namespace=self,
            workload_id=workload_id,
        )

    def service(self, service_id: str) -> Service:
        """Get a handle to a service in this namespace.

        Args:
            service_id: The service to reference.

        Returns:
            A Service handle bound to this namespace's live model.
        """
        return Service(
            namespace=self,
            service_id=service_id,
        )

    def events(
        self,
        *,
        workloads: list[str] | None = None,
        services: list[str] | None = None,
    ) -> AsyncIterator[Any]:
        """Stream namespace events as an async iterator.

        Opens a separate StreamEvents RPC (independent of the internal
        model stream). Useful for logging/debugging.

        Args:
            workloads: Filter to these workload IDs. None = all.
            services: Filter to these service IDs. None = all.

        Returns:
            Async iterator of event objects.
        """
        # TODO: open StreamEvents RPC with filters, return EventStream
        raise NotImplementedError

    def logs(
        self,
        *,
        workload: str | None = None,
    ) -> AsyncIterator[Any]:
        """Stream logs as an async iterator.

        Args:
            workload: Filter to this workload ID. None = all.

        Returns:
            Async iterator of log chunks.
        """
        # TODO: open StreamLogs RPC, return async iterator
        raise NotImplementedError
