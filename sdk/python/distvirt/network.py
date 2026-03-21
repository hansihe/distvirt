"""Userspace WireGuard tunnel and socket wrappers."""

from __future__ import annotations

from typing import Any

from distvirt._core import (
    UserspaceNetwork as _CoreUserspaceNetwork,
    TcpStream as _CoreTcpStream,
    UdpSocket as _CoreUdpSocket,
    PyClient,
)


class Network:
    """Userspace WireGuard tunnel to a namespace.

    Provides TCP/UDP socket access to workloads inside the namespace
    without requiring root privileges.

    Usage::

        async with await ns.connect() as net:
            tcp = await net.connect_tcp("10.0.0.2", 8080)
            await tcp.write_all(b"GET / HTTP/1.0\\r\\n\\r\\n")
            data = await tcp.read()
    """

    def __init__(self, inner: _CoreUserspaceNetwork, client_inner: PyClient):
        self._inner = inner
        self._client_inner = client_inner

    async def __aenter__(self) -> Network:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.disconnect()

    async def connect_tcp(self, host: str, port: int) -> TcpConnection:
        """Open a TCP connection to an address inside the namespace."""
        inner = await self._inner.connect_tcp(host, port)
        return TcpConnection(inner)

    async def bind_udp(self, port: int = 0) -> UdpConnection:
        """Bind a UDP socket inside the namespace."""
        inner = await self._inner.bind_udp(port)
        return UdpConnection(inner)

    async def disconnect(self) -> None:
        """Shut down the tunnel and deregister with the server."""
        await self._inner.disconnect(self._client_inner)

    @property
    def client_ip(self) -> str:
        """The client's IP address inside the namespace."""
        return self._inner.client_ip

    @property
    def subnet(self) -> str:
        """The namespace subnet (e.g. "10.0.0.0/24")."""
        return self._inner.subnet


class TcpConnection:
    """TCP connection over the userspace WireGuard tunnel."""

    def __init__(self, inner: _CoreTcpStream):
        self._inner = inner

    async def __aenter__(self) -> TcpConnection:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def read(self, n: int = 4096) -> bytes:
        """Read up to n bytes."""
        return await self._inner.read(n)

    async def write(self, data: bytes) -> int:
        """Write data, returning bytes written."""
        return await self._inner.write(data)

    async def write_all(self, data: bytes) -> None:
        """Write all data."""
        await self._inner.write_all(data)

    async def close(self) -> None:
        """Close the connection."""
        self._inner.close()

    @property
    def peer_addr(self) -> str:
        """Remote address as "ip:port"."""
        return self._inner.peer_addr


class UdpConnection:
    """UDP socket over the userspace WireGuard tunnel."""

    def __init__(self, inner: _CoreUdpSocket):
        self._inner = inner

    async def send_to(self, data: bytes, host: str, port: int) -> int:
        """Send a datagram to the given address."""
        return await self._inner.send_to(data, host, port)

    async def recv_from(self, bufsize: int = 65536) -> tuple[bytes, str]:
        """Receive a datagram. Returns (data, "ip:port")."""
        return await self._inner.recv_from(bufsize)

    @property
    def local_port(self) -> int:
        """The local port this socket is bound to."""
        return self._inner.local_port

    def close(self) -> None:
        """Close the socket."""
        self._inner.close()
