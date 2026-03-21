"""Namespace handle with live object model backed by Rust."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, AsyncIterator, Callable

import grpclib

from distvirt._proto.distvirt.client.v1 import (
    DistvirtClientStub,
    StreamEventsRequest,
)
from distvirt.errors import StreamEndedError, handle_grpc_error
from distvirt.events import EventStream

logger = logging.getLogger(__name__)


class Namespace:
    """Live handle to a distvirt namespace.

    Maintains an internal object model (backed by the Rust distvirt-client
    model) kept up-to-date by a background event stream. State queries
    read from the local model — no RPC needed.

    The event stream is opened eagerly on construction. This eliminates
    races between apply() and wait_for().

    Usage::

        ns = await dv.namespace("staging")
        await ns.workload("db").wait_for(distvirt.running)
        status = ns.status()
    """

    def __init__(
        self,
        namespace_id: str,
        stub: DistvirtClientStub,
        model: Any,  # _core.NamespaceModel
        event_stream: Any = None,
    ):
        self._namespace_id = namespace_id
        self._stub = stub
        self._model = model
        self._event_stream = event_stream
        self._event_task: asyncio.Task | None = None
        self._waiters: list[tuple[Callable, asyncio.Future]] = []

    @property
    def namespace_id(self) -> str:
        return self._namespace_id

    def _start_event_loop(self) -> None:
        """Start the background event consumption task."""
        assert self._event_stream is not None, "event stream not set"
        self._event_task = asyncio.create_task(
            self._run_event_loop(),
            name=f"distvirt-events-{self._namespace_id}",
        )

    async def _run_event_loop(self) -> None:
        """Consume events from the already-open StreamEvents RPC."""
        try:
            async for event in self._event_stream:
                proto_bytes = bytes(event)
                changed = self._model.apply_event_bytes(proto_bytes)
                if changed:
                    self._notify_waiters()
            # Stream ended normally — fail waiters so they don't hang
            self._fail_waiters(
                StreamEndedError("event stream ended unexpectedly")
            )
        except asyncio.CancelledError:
            raise
        except grpclib.GRPCError as e:
            err = handle_grpc_error(e)
            logger.error(
                "Event loop for namespace %s failed: %s",
                self._namespace_id,
                err,
            )
            self._fail_waiters(err)
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

    def status(self) -> Any:
        """Return the Rust-backed namespace model.

        This is a synchronous read — no RPC. The model is kept
        current by the background event stream.
        """
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
    ) -> EventStream:
        """Stream namespace events as an async iterator.

        Opens a separate StreamEvents RPC (independent of the internal
        model stream).
        """
        request = StreamEventsRequest(
            namespace_id=self._namespace_id,
            workload_ids=workloads or [],
            service_ids=services or [],
        )
        stream = self._stub.stream_events(request)
        return EventStream(stream)

    def logs(
        self,
        *,
        workload: str | None = None,
    ) -> AsyncIterator[Any]:
        """Stream logs as an async iterator."""
        # TODO: open StreamLogs RPC, return async iterator
        raise NotImplementedError
