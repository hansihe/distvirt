"""Workload handle for state queries and operations."""

from __future__ import annotations

import asyncio
from typing import Any, TYPE_CHECKING

from distvirt.errors import TimeoutError as DistvirtTimeoutError
from distvirt.events import WorkloadModel

if TYPE_CHECKING:
    from distvirt.namespace import Namespace
    from distvirt.states import WorkloadStateMatcher


class Workload:
    """Handle to a workload within a namespace.

    State queries read from the namespace's Rust-backed model.
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
        info = self._namespace._model.workload_info(self._workload_id)
        if info is None:
            return None
        return WorkloadModel(
            workload_id=info["workload_id"],
            state=info["state"],
            pod_id=info["pod_id"],
            worker_id=info["worker_id"],
            spliced=info["spliced"],
            ip=info["ip"],
            demanding_services=info["demanding_services"],
        )

    async def wait_for(
        self,
        state: WorkloadStateMatcher,
        *,
        timeout: float | None = None,
    ) -> WorkloadModel:
        """Wait until the workload reaches the given state.

        Checks the current model state first. If already matching,
        returns immediately. Otherwise, registers a waiter on the
        namespace that resolves when the state matches.

        Args:
            state: State matcher (e.g. distvirt.running, distvirt.completed).
            timeout: Maximum seconds to wait. None = wait forever.

        Returns:
            The WorkloadModel at the time the state matched.

        Raises:
            distvirt.errors.TimeoutError: If timeout expires before state is reached.
            distvirt.errors.StreamEndedError: If the event stream ends before state is reached.
            distvirt.errors.ApiError: If a gRPC error occurs on the event stream.
        """
        model = self._namespace._model

        # Check current state
        current = model.workload_state(self._workload_id)
        if current is not None and current == state.state:
            return self.status()

        # Register waiter
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()

        def predicate(m: Any) -> bool:
            s = m.workload_state(self._workload_id)
            return s is not None and s == state.state

        self._namespace._waiters.append((predicate, future))

        try:
            await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            # Clean up the waiter
            self._namespace._waiters = [
                (p, f) for p, f in self._namespace._waiters if f is not future
            ]
            raise DistvirtTimeoutError(
                entity_type="workload",
                entity_id=self._workload_id,
                target_state=state.state,
                timeout=timeout,
            ) from None

        return self.status()

    async def deactivate(self) -> tuple[bool, str]:
        """Explicitly deactivate this workload's pod."""
        # TODO: call DeactivateWorkload RPC
        raise NotImplementedError

    async def attach(self) -> AttachSession:
        """Attach to the workload's entrypoint stdin/stdout/stderr."""
        # TODO: open AttachWorkload bidirectional stream
        raise NotImplementedError


class AttachSession:
    """Bidirectional attach session to a workload's entrypoint."""

    def __init__(self, stream: Any, tty: bool):
        self._stream = stream
        self._tty = tty

    @property
    def tty(self) -> bool:
        return self._tty

    async def __aenter__(self) -> AttachSession:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def close(self) -> None:
        pass

    async def send(self, data: bytes) -> None:
        raise NotImplementedError

    async def resize(self, cols: int, rows: int) -> None:
        raise NotImplementedError

    def __aiter__(self) -> AttachSession:
        return self

    async def __anext__(self) -> Any:
        raise NotImplementedError
