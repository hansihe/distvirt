"""Async client for distvirt orchestrator."""

from __future__ import annotations

from typing import Any

from distvirt._core import PyClient
from distvirt.namespace import Namespace


class Client:
    """Async client for the distvirt orchestrator.

    Use `distvirt.connect()` to create an instance.

    Usage::

        async with distvirt.connect("orchestrator:9090") as dv:
            ns = await dv.apply("staging", file="distvirt.yaml")
            await ns.workload("api").wait_for(distvirt.running)
    """

    def __init__(self, inner: PyClient):
        self._inner = inner

    async def __aenter__(self) -> Client:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def close(self) -> None:
        """Close the client."""
        self._inner.close()

    async def apply(
        self,
        namespace_id: str,
        *,
        file: str | None = None,
        spec_bytes: bytes | None = None,
        values: dict[str, str] | None = None,
    ) -> Namespace:
        """Apply a namespace spec. Creates or updates the namespace.

        Returns a live Namespace handle with an active event stream.
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
        """Get a live handle to an existing namespace."""
        return await self._open_namespace(namespace_id)

    async def _open_namespace(self, namespace_id: str) -> Namespace:
        """Create a Namespace handle with bootstrapped model and event stream."""
        watcher = await self._inner.start_watcher(namespace_id)
        model = watcher.model()

        ns = Namespace(
            namespace_id=namespace_id,
            client_inner=self._inner,
            watcher=watcher,
            model=model,
        )

        ns._start_event_loop()
        return ns

    async def namespaces(self) -> list[dict]:
        """List all namespaces with their current status."""
        return await self._inner.list_namespaces()


def _parse_spec_file(path: str, values: dict[str, str] | None) -> bytes:
    """Parse a spec file into serialized protobuf bytes via the Rust core."""
    from distvirt._core import parse_spec
    _namespace_id, spec_bytes = parse_spec(path, values)
    return spec_bytes


async def connect(
    addr: str | None = None,
    *,
    token: str | None = None,
    context: str | None = None,
) -> Client:
    """Connect to a distvirt orchestrator.

    Connection parameters are resolved with the same precedence as the CLI:
    explicit args > env vars (DV_SERVER, DV_TOKEN) > credentials file > defaults.
    """
    inner = await PyClient.connect(server=addr, token=token, context=context)
    return Client(inner)
