"""Workload handle for state queries and operations."""

from __future__ import annotations

import asyncio
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from distvirt.namespace import Namespace
    from distvirt.events import WorkloadModel
    from distvirt.states import WorkloadStateMatcher


class Workload:
    """Handle to a workload within a namespace.

    State queries read from the namespace's live object model.
    wait_for() registers a waiter that resolves when the model
    reaches the target state.

    Usage::

        wl = ns.workload("api")
        await wl.wait_for(distvirt.running)
        status = wl.status()
    """

    def __init__(self, namespace: Namespace, workload_id: str):
        self._namespace = namespace
        self._workload_id = workload_id

    @property
    def workload_id(self) -> str:
        return self._workload_id

    def status(self) -> WorkloadModel | None:
        """Return current workload state from the live model.

        Returns None if the workload is not (yet) present in the model.
        """
        return self._namespace._model.workloads.get(self._workload_id)

    async def wait_for(
        self,
        state: WorkloadStateMatcher,
        *,
        timeout: float | None = None,
    ) -> WorkloadModel:
        """Wait until the workload reaches the given state.

        Checks the current model state first. If already matching,
        returns immediately. Otherwise, registers a waiter on the
        namespace model that resolves when the state matches.

        Args:
            state: State matcher (e.g. distvirt.running, distvirt.completed).
            timeout: Maximum seconds to wait. None = wait forever.

        Returns:
            The WorkloadModel at the time the state matched.

        Raises:
            asyncio.TimeoutError: If timeout expires before state is reached.
        """
        model = self._namespace._model

        # Check current state
        wl = model.workloads.get(self._workload_id)
        if wl is not None and state.matches(wl):
            return wl

        # Register waiter
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()

        def predicate(m: Any) -> bool:
            wl = m.workloads.get(self._workload_id)
            return wl is not None and state.matches(wl)

        model._waiters.append((predicate, future))

        try:
            await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            # Clean up the waiter
            model._waiters = [
                (p, f) for p, f in model._waiters if f is not future
            ]
            raise

        return model.workloads[self._workload_id]

    async def deactivate(self) -> tuple[bool, str]:
        """Explicitly deactivate this workload's pod.

        Returns:
            Tuple of (deactivated, reason). If deactivated is False,
            reason explains why (e.g. already dormant).
        """
        # TODO: call DeactivateWorkload RPC
        raise NotImplementedError

    async def attach(self) -> AttachSession:
        """Attach to the workload's entrypoint stdin/stdout/stderr.

        Returns an async context manager for the bidirectional stream.

        Usage::

            async with wl.attach() as session:
                await session.send(b"ls\\n")
                async for output in session:
                    print(output.data.decode(), end="")
        """
        # TODO: open AttachWorkload bidirectional stream
        raise NotImplementedError


class AttachSession:
    """Bidirectional attach session to a workload's entrypoint.

    Wraps the AttachWorkload bidirectional gRPC stream.
    """

    def __init__(self, stream: Any, tty: bool):
        self._stream = stream
        self._tty = tty

    @property
    def tty(self) -> bool:
        """Whether the entrypoint is running with a PTY."""
        return self._tty

    async def __aenter__(self) -> AttachSession:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def close(self) -> None:
        """Cancel the attach stream. Does not kill the entrypoint."""
        # TODO: cancel gRPC stream
        pass

    async def send(self, data: bytes) -> None:
        """Send stdin data to the entrypoint."""
        # TODO: send AttachStdin message
        raise NotImplementedError

    async def resize(self, cols: int, rows: int) -> None:
        """Send terminal resize event (only meaningful if tty=True)."""
        # TODO: send AttachResize message
        raise NotImplementedError

    def __aiter__(self) -> AttachSession:
        return self

    async def __anext__(self) -> Any:
        """Receive next stdout/stderr/exited message."""
        # TODO: read from gRPC stream
        raise NotImplementedError
