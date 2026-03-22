"""Userspace WireGuard tunnel and socket wrappers."""

from __future__ import annotations

import asyncio
from typing import Any

from distvirt._core import (
    UserspaceNetwork as _CoreUserspaceNetwork,
    TcpStream as _CoreTcpStream,
    UdpSocket as _CoreUdpSocket,
    PyClient,
)


class _TcpTransport(asyncio.Transport):
    """Bridges a Rust TcpStream to asyncio's Transport/Protocol interface."""

    def __init__(self, stream: _CoreTcpStream, protocol: asyncio.Protocol):
        super().__init__()
        self._stream = stream
        self._protocol = protocol
        self._closing = False
        self._closed_fut: asyncio.Future[None] | None = None
        self._write_buf = bytearray()
        self._write_task: asyncio.Task[None] | None = None
        self._read_task: asyncio.Task[None] | None = None
        self._high_water = 65536
        self._low_water = 16384
        self._protocol_paused = False

    # -- Lifecycle ------------------------------------------------------------

    def _start(self) -> None:
        loop = asyncio.get_running_loop()
        self._closed_fut = loop.create_future()
        self._read_task = loop.create_task(self._read_loop())

    async def _read_loop(self) -> None:
        try:
            while not self._closing:
                data = await self._stream.read(65536)
                if not data:
                    keep_open = self._protocol.eof_received()
                    if not keep_open:
                        break
                    continue
                self._protocol.data_received(data)
        except Exception as exc:
            self._force_close(exc)
            return
        if not self._closing:
            self.close()

    # -- Transport.write / flow-control ---------------------------------------

    def write(self, data: bytes | bytearray | memoryview) -> None:
        if self._closing:
            return
        self._write_buf.extend(data)
        self._maybe_start_drain()
        if not self._protocol_paused and len(self._write_buf) > self._high_water:
            self._protocol_paused = True
            self._protocol.pause_writing()

    def _maybe_start_drain(self) -> None:
        if self._write_task is None or self._write_task.done():
            self._write_task = asyncio.get_running_loop().create_task(
                self._drain_write_buf()
            )

    async def _drain_write_buf(self) -> None:
        while self._write_buf:
            chunk = bytes(self._write_buf)
            self._write_buf.clear()
            await self._stream.write_all(chunk)
            if self._protocol_paused and len(self._write_buf) <= self._low_water:
                self._protocol_paused = False
                self._protocol.resume_writing()

    def get_write_buffer_size(self) -> int:
        return len(self._write_buf)

    def get_write_buffer_limits(self) -> tuple[int, int]:
        return (self._low_water, self._high_water)

    def set_write_buffer_limits(
        self, high: int | None = None, low: int | None = None
    ) -> None:
        if high is not None:
            self._high_water = high
        if low is not None:
            self._low_water = low

    def is_reading(self) -> bool:
        return self._read_task is not None and not self._read_task.done()

    # -- Shutdown -------------------------------------------------------------

    def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        asyncio.get_running_loop().create_task(self._close_async())

    def is_closing(self) -> bool:
        return self._closing

    async def _close_async(self) -> None:
        try:
            if self._write_buf:
                await self._drain_write_buf()
            await self._stream.shutdown()
        except Exception:
            pass
        finally:
            self._stream.close()
            self._protocol.connection_lost(None)
            if self._closed_fut and not self._closed_fut.done():
                self._closed_fut.set_result(None)

    def _force_close(self, exc: Exception | None) -> None:
        if self._closing:
            return
        self._closing = True
        self._stream.close()
        self._protocol.connection_lost(exc)
        if self._closed_fut and not self._closed_fut.done():
            self._closed_fut.set_result(None)

    # -- Extra info -----------------------------------------------------------

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        if name == "peername":
            addr = self._stream.peer_addr
            host, port = addr.rsplit(":", 1)
            return (host, int(port))
        if name == "distvirt.stream":
            return self._stream
        return default


class Network:
    """Userspace WireGuard tunnel to a namespace.

    Provides TCP/UDP socket access to workloads inside the namespace
    without requiring root privileges.

    Usage::

        async with await ns.connect() as net:
            reader, writer = await net.connect_tcp("10.0.0.2", 8080)
            writer.write(b"GET / HTTP/1.0\\r\\n\\r\\n")
            await writer.drain()
            data = await reader.read(4096)
            writer.close()
            await writer.wait_closed()
    """

    def __init__(self, inner: _CoreUserspaceNetwork, client_inner: PyClient):
        self._inner = inner
        self._client_inner = client_inner

    async def __aenter__(self) -> Network:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.disconnect()

    async def connect_tcp(
        self, host: str, port: int
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """Open a TCP connection returning standard asyncio streams.

        Returns:
            A ``(reader, writer)`` pair, just like :func:`asyncio.open_connection`.
        """
        stream = await self._inner.connect_tcp(host, port)
        loop = asyncio.get_running_loop()

        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader, loop=loop)
        transport = _TcpTransport(stream, protocol)
        protocol.connection_made(transport)
        transport._start()
        writer = asyncio.StreamWriter(transport, protocol, reader, loop)

        return reader, writer

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
