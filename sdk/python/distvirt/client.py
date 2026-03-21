"""Async gRPC client for distvirt orchestrator."""

from __future__ import annotations

from typing import Any

import grpclib.client

from distvirt._proto.distvirt.client.v1 import (
    CreateNamespaceRequest,
    DeleteNamespaceRequest,
    DistvirtClientStub,
    GetNamespaceStatusRequest,
    ListNamespacesRequest,
    NamespaceSpec,
    NamespaceStatusReport,
    StreamEventsRequest,
    UpdateNamespaceRequest,
)
from distvirt.namespace import Namespace


class Client:
    """Async client for the distvirt orchestrator.

    Use `distvirt.connect()` to create an instance. Manages the gRPC
    channel and provides top-level operations.

    Usage::

        async with distvirt.connect("orchestrator:9090") as dv:
            ns = await dv.apply("staging", file="distvirt.yaml")
            await ns.workload("api").wait_for(distvirt.running)
    """

    def __init__(self, channel: grpclib.client.Channel, token: str | None = None):
        self._channel = channel
        self._token = token
        self._stub = DistvirtClientStub(channel)

    async def __aenter__(self) -> Client:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def close(self) -> None:
        """Close the gRPC channel and all associated namespace streams."""
        self._channel.close()

    async def apply(
        self,
        namespace_id: str,
        *,
        file: str | None = None,
        spec_bytes: bytes | None = None,
        values: dict[str, str] | None = None,
    ) -> Namespace:
        """Apply a namespace spec. Creates or updates the namespace.

        Parses the spec file (via Rust core), sends CreateNamespace or
        UpdateNamespace depending on whether the namespace already exists.
        Returns a live Namespace handle with an active event stream.

        Args:
            namespace_id: Target namespace ID. Overrides metadata.name in the spec.
            file: Path to a distvirt.yaml or docker-compose.yml file.
            spec_bytes: Pre-serialized protobuf NamespaceSpec bytes (alternative to file).
            values: Variable substitutions for fragment includes (${VAR} replacement).

        Returns:
            A Namespace handle with a live event stream attached.
        """
        if file is not None and spec_bytes is not None:
            raise ValueError("specify file or spec_bytes, not both")
        if file is None and spec_bytes is None:
            raise ValueError("one of file or spec_bytes is required")

        if file is not None:
            spec_bytes = _parse_spec_file(file, values)

        spec = NamespaceSpec().parse(spec_bytes)

        try:
            await self._stub.create_namespace(
                CreateNamespaceRequest(namespace_id=namespace_id, spec=spec)
            )
        except grpclib.GRPCError as e:
            if e.status == grpclib.const.Status.ALREADY_EXISTS:
                await self._stub.update_namespace(
                    UpdateNamespaceRequest(namespace_id=namespace_id, spec=spec)
                )
            else:
                raise

        ns = await self._open_namespace(namespace_id)
        return ns

    async def delete(self, namespace_id: str) -> None:
        """Delete a namespace.

        Args:
            namespace_id: Namespace to delete.
        """
        await self._stub.delete_namespace(
            DeleteNamespaceRequest(namespace_id=namespace_id)
        )

    async def namespace(self, namespace_id: str) -> Namespace:
        """Get a live handle to an existing namespace.

        Opens an event stream and bootstraps the object model from
        current status.

        Args:
            namespace_id: Namespace to connect to.

        Returns:
            A Namespace handle with a live event stream attached.
        """
        return await self._open_namespace(namespace_id)

    async def _open_namespace(self, namespace_id: str) -> Namespace:
        """Create a Namespace handle with bootstrapped model and event stream.

        Opens the event stream *before* fetching status, so any events
        that occur between the two calls are buffered and not lost.
        Events that duplicate the status snapshot are harmless — applying
        a state the model is already in is a no-op.
        """
        # 1. Open event stream first — starts buffering immediately
        request = StreamEventsRequest(namespace_id=namespace_id)
        event_stream = self._stub.stream_events(request)

        ns = Namespace(
            namespace_id=namespace_id,
            stub=self._stub,
            event_stream=event_stream,
        )

        # 2. Bootstrap model from current status snapshot
        resp = await self._stub.get_namespace_status(
            GetNamespaceStatusRequest(namespace_id=namespace_id)
        )
        ns._model.apply_status(resp.status)

        # 3. Start consuming buffered + future events
        ns._start_event_loop()

        return ns

    async def namespaces(self) -> list[NamespaceStatusReport]:
        """List all namespaces with their current status.

        Returns:
            List of namespace status reports.
        """
        resp = await self._stub.list_namespaces(ListNamespacesRequest())
        return list(resp.namespaces)


def _parse_spec_file(path: str, values: dict[str, str] | None) -> bytes:
    """Parse a spec file into serialized protobuf bytes via the Rust core."""
    try:
        from distvirt._core import parse_spec
    except ImportError:
        raise RuntimeError(
            "distvirt._core extension not built. "
            "Install with: pip install -e sdk/python"
        ) from None
    _namespace_id, spec_bytes = parse_spec(path, values)
    return spec_bytes


async def connect(addr: str, *, token: str | None = None) -> Client:
    """Connect to a distvirt orchestrator.

    Args:
        addr: Orchestrator address ("host:port").
        token: Optional API key for authentication.

    Returns:
        An async context manager yielding a Client.

    Usage::

        async with distvirt.connect("localhost:9090") as dv:
            ...
    """
    host, _, port_str = addr.rpartition(":")
    if not host:
        host = addr
        port = 9090
    else:
        port = int(port_str)

    channel = grpclib.client.Channel(host=host, port=port)
    return Client(channel, token=token)
