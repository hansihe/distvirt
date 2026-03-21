"""Event and log stream wrappers, plus public model dataclasses."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, AsyncIterator

from distvirt._core import EventStream as _CoreEventStream, LogStream as _CoreLogStream


@dataclass
class WorkloadModel:
    """Snapshot of a workload's state, returned by Workload.status()."""

    workload_id: str
    state: str = "unknown"
    pod_id: str | None = None
    worker_id: str | None = None
    spliced: bool = False
    ip: str | None = None
    demanding_services: int = 0


@dataclass
class ServiceModel:
    """Snapshot of a service's state, returned by Service.status()."""

    service_id: str
    workload_id: str = ""
    state: str = "unknown"
    ip: str | None = None
    activation_enabled: bool = False
    spliced: bool = False


class EventStream:
    """Async iterator over namespace events from the Rust-backed stream.

    Each item is the raw protobuf bytes of a NamespaceEvent.
    """

    def __init__(self, inner: _CoreEventStream):
        self._inner = inner

    def __aiter__(self) -> AsyncIterator:
        return self

    async def __anext__(self) -> bytes:
        return await self._inner.__anext__()


class LogStream:
    """Async iterator over log chunks from the Rust-backed stream.

    Each item is a dict with keys: workload_id, data, timestamp_ms, container_id.
    """

    def __init__(self, inner: _CoreLogStream):
        self._inner = inner

    def __aiter__(self) -> AsyncIterator:
        return self

    async def __anext__(self) -> dict[str, Any]:
        return await self._inner.__anext__()
