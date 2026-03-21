"""Event stream wrapper and public model dataclasses."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, AsyncIterator

from distvirt._proto.distvirt.client.v1 import NamespaceEvent


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
    """Async iterator over namespace events from StreamEvents RPC.

    This is the raw event stream exposed to users via `ns.events()`.
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
