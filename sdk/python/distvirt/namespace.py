"""Namespace handle with live object model backed by Rust."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable

from distvirt._core import PyClient, NamespaceWatcher
from distvirt.errors import StreamEndedError

logger = logging.getLogger(__name__)


class Namespace:
    """Live handle to a distvirt namespace.

    Maintains an internal object model (backed by the Rust distvirt-client
    model) kept up-to-date by a background event stream. State queries
    read from the local model -- no RPC needed.

    Usage::

        ns = await dv.namespace("staging")
        await ns.workload("db").wait_for(distvirt.running)
        status = ns.status()
    """

    def __init__(
        self,
        namespace_id: str,
        client_inner: PyClient,
        watcher: NamespaceWatcher,
        model: Any,  # _core.NamespaceModel
    ):
        self._namespace_id = namespace_id
        self._client_inner = client_inner
        self._watcher = watcher
        self._model = model
        self._event_task: asyncio.Task | None = None
        self._waiters: list[tuple[Callable, asyncio.Future]] = []

    @property
    def namespace_id(self) -> str:
        return self._namespace_id

    def _start_event_loop(self) -> None:
        """Start the background event consumption task."""
        self._event_task = asyncio.create_task(
            self._run_event_loop(),
            name=f"distvirt-events-{self._namespace_id}",
        )

    async def _run_event_loop(self) -> None:
        """Consume events from the Rust NamespaceWatcher."""
        try:
            while True:
                change = await self._watcher.next()
                if change is None:
                    self._fail_waiters(
                        StreamEndedError("event stream ended unexpectedly")
                    )
                    break
                # Refresh model from watcher after each event
                self._model = self._watcher.model()
                self._notify_waiters()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception(
                "Event loop for namespace %s terminated with error",
                self._namespace_id,
            )
            self._fail_waiters(e)

    def _notify_waiters(self) -> None:
        """Check all waiters against current model state, resolve matches."""
        remaining = []
        for predicate, future in self._waiters:
            if future.done():
                continue
            if predicate(self._model):
                future.set_result(None)
            else:
                remaining.append((predicate, future))
        self._waiters = remaining

    def _fail_waiters(self, exc: BaseException) -> None:
        """Fail all pending waiters with the given exception."""
        for _predicate, future in self._waiters:
            if not future.done():
                future.set_exception(exc)
        self._waiters = []

    async def close(self) -> None:
        """Stop the background event loop and release resources."""
        if self._event_task is not None:
            self._event_task.cancel()
            try:
                await self._event_task
            except asyncio.CancelledError:
                pass
            self._event_task = None
        await self._watcher.close()

    def status(self) -> Any:
        """Return the Rust-backed namespace model."""
        return self._model

    def workload(self, workload_id: str) -> "Workload":
        """Get a handle to a workload in this namespace."""
        from distvirt.workload import Workload

        return Workload(namespace=self, workload_id=workload_id)

    def service(self, service_id: str) -> "Service":
        """Get a handle to a service in this namespace."""
        from distvirt.service import Service

        return Service(namespace=self, service_id=service_id)

    def events(
        self,
        *,
        workloads: list[str] | None = None,
        services: list[str] | None = None,
    ) -> "EventStream":
        """Stream namespace events as an async iterator.

        Opens a separate event stream (independent of the internal model stream).
        """
        from distvirt.events import EventStream

        async def _open():
            inner = await self._client_inner.stream_events(
                self._namespace_id,
                workloads or [],
                services or [],
            )
            return EventStream(inner)

        # Return a coroutine-wrapping helper so callers do `async for ev in await ns.events()`
        # Actually, return an awaitable that yields the EventStream
        import asyncio
        return asyncio.ensure_future(_open())

    def logs(
        self,
        *,
        workload: str | None = None,
    ) -> Any:
        """Stream logs as an async iterator."""
        from distvirt.events import LogStream

        async def _open():
            inner = await self._client_inner.stream_logs(
                self._namespace_id,
                workload,
            )
            return LogStream(inner)

        import asyncio
        return asyncio.ensure_future(_open())

    async def connect(self) -> "Network":
        """Open a userspace WireGuard tunnel to this namespace."""
        from distvirt.network import Network
        from distvirt._core import UserspaceNetwork

        inner = await UserspaceNetwork.connect(self._client_inner, self._namespace_id)
        return Network(inner, self._client_inner)
