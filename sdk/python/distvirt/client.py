"""Async gRPC client for distvirt orchestrator."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

import grpclib.client

from distvirt._core import PyClient, resolve_connection
from distvirt._proto.distvirt.client.v1 import (
    DistvirtClientStub,
    NamespaceSpec,
    NamespaceStatusReport,
    StreamEventsRequest,
    GetNamespaceStatusRequest,
)
from distvirt.errors import handle_grpc_error, ApiError, StreamEndedError
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

    def __init__(
        self,
        inner: PyClient,
        grpc_channel: grpclib.client.Channel,
        token: str | None = None,
    ):
        self._inner = inner
        # grpclib channel kept temporarily for streaming (Phase 2 will remove)
        self._channel = grpc_channel
        self._token = token
        self._stub = DistvirtClientStub(grpc_channel)

    async def __aenter__(self) -> Client:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def close(self) -> None:
        """Close the gRPC channel and all associated namespace streams."""
        self._inner.close()
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

        await self._inner.apply(namespace_id, spec_bytes)

        ns = await self._open_namespace(namespace_id)
        return ns

    async def delete(self, namespace_id: str) -> None:
        """Delete a namespace."""
        await self._inner.down(namespace_id)

    async def namespace(self, namespace_id: str) -> Namespace:
        """Get a live handle to an existing namespace.

        Opens an event stream and bootstraps the object model from
        current status.
        """
        return await self._open_namespace(namespace_id)

    async def _open_namespace(self, namespace_id: str) -> Namespace:
        """Create a Namespace handle with bootstrapped model and event stream.

        Opens the event stream *before* fetching status, so any events
        that occur between the two calls are buffered and not lost.

        Note: Still uses grpclib for streaming (temporary, removed in Phase 2).
        """
        from distvirt._core import NamespaceModel

        # 1. Open event stream first — starts buffering immediately (grpclib)
        request = StreamEventsRequest(namespace_id=namespace_id)
        event_stream = self._stub.stream_events(request)

        # 2. Fetch current status via Rust client and bootstrap model
        status_bytes = await self._inner.get_status(namespace_id)
        model = NamespaceModel.from_status_bytes(status_bytes)

        ns = Namespace(
            namespace_id=namespace_id,
            stub=self._stub,
            model=model,
            event_stream=event_stream,
        )

        # 3. Start consuming buffered + future events
        ns._start_event_loop()

        return ns

    async def namespaces(self) -> list[NamespaceStatusReport]:
        """List all namespaces with their current status."""
        raw = await self._inner.list_namespaces()
        result = []
        for _ns_id, proto_bytes in raw:
            report = NamespaceStatusReport().parse(proto_bytes)
            result.append(report)
        return result


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


def _parse_server_url(server: str) -> tuple[str, int]:
    """Parse a server URL into (host, port) for grpclib.

    Handles formats: "host:port", "http://host:port", "http://[::1]:9090".
    """
    if "://" not in server:
        server = f"http://{server}"

    parsed = urlparse(server)
    host = parsed.hostname or "::1"
    port = parsed.port or 9090
    return host, port


async def connect(
    addr: str | None = None,
    *,
    token: str | None = None,
    context: str | None = None,
) -> Client:
    """Connect to a distvirt orchestrator.

    Connection parameters are resolved with the same precedence as the CLI:
    explicit args > env vars (DV_SERVER, DV_TOKEN) > credentials file > defaults.

    Args:
        addr: Orchestrator address ("host:port" or URL). If None, resolved
              from env/credentials.
        token: Optional API key. If None, resolved from env/credentials.
        context: Credentials file context name override.

    Returns:
        An async context manager yielding a Client.

    Usage::

        # Explicit address
        async with distvirt.connect("localhost:9090") as dv:
            ...

        # Auto-resolve from credentials/env
        async with distvirt.connect() as dv:
            ...
    """
    # Create Rust client (handles resolve + connect internally)
    inner = await PyClient.connect(server=addr, token=token, context=context)

    # Also create grpclib channel for streaming (temporary, removed in Phase 2)
    server, resolved_token = resolve_connection(
        server=addr, token=token, context=context
    )
    host, port = _parse_server_url(server)
    channel = grpclib.client.Channel(host=host, port=port)

    return Client(inner, channel, token=resolved_token)
