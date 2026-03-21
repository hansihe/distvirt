"""Service handle for state queries."""

from __future__ import annotations

import asyncio
from typing import Any, TYPE_CHECKING

from distvirt.errors import TimeoutError as DistvirtTimeoutError
from distvirt.events import ServiceModel

if TYPE_CHECKING:
    from distvirt.namespace import Namespace
    from distvirt.states import ServiceStateMatcher


class Service:
    """Handle to a service within a namespace.

    State queries read from the namespace's Rust-backed model.

    Usage::

        svc = ns.service("api")
        await svc.wait_for(distvirt.active)
        status = svc.status()
    """

    def __init__(self, namespace: Namespace, service_id: str):
        self._namespace = namespace
        self._service_id = service_id

    @property
    def service_id(self) -> str:
        return self._service_id

    def status(self) -> ServiceModel | None:
        """Return current service state from the live model.

        Returns None if the service is not (yet) present in the model.
        """
        info = self._namespace._model.service_info(self._service_id)
        if info is None:
            return None
        return ServiceModel(
            service_id=info["service_id"],
            workload_id=info["workload_id"],
            state=info["state"],
            ip=info["ip"],
            activation_enabled=info["activation_enabled"],
            spliced=info["spliced"],
        )

    async def wait_for(
        self,
        state: ServiceStateMatcher,
        *,
        timeout: float | None = None,
    ) -> ServiceModel:
        """Wait until the service reaches the given state.

        Args:
            state: State matcher (e.g. distvirt.active, distvirt.idle).
            timeout: Maximum seconds to wait. None = wait forever.

        Returns:
            The ServiceModel at the time the state matched.

        Raises:
            distvirt.errors.TimeoutError: If timeout expires before state is reached.
            distvirt.errors.StreamEndedError: If the event stream ends before state is reached.
            distvirt.errors.ApiError: If a gRPC error occurs on the event stream.
        """
        model = self._namespace._model

        # Check current state
        current = model.service_state(self._service_id)
        if current is not None and current == state.state:
            return self.status()

        # Register waiter
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()

        def predicate(m: Any) -> bool:
            s = m.service_state(self._service_id)
            return s is not None and s == state.state

        self._namespace._waiters.append((predicate, future))

        try:
            await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            self._namespace._waiters = [
                (p, f) for p, f in self._namespace._waiters if f is not future
            ]
            raise DistvirtTimeoutError(
                entity_type="service",
                entity_id=self._service_id,
                target_state=state.state,
                timeout=timeout,
            ) from None

        return self.status()
