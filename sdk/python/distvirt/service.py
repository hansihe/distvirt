"""Service handle for state queries."""

from __future__ import annotations

import asyncio
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from distvirt.namespace import Namespace
    from distvirt.events import ServiceModel
    from distvirt.states import ServiceStateMatcher


class Service:
    """Handle to a service within a namespace.

    State queries read from the namespace's live object model.

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
        return self._namespace._model.services.get(self._service_id)

    async def wait_for(
        self,
        state: ServiceStateMatcher,
        *,
        timeout: float | None = None,
    ) -> ServiceModel:
        """Wait until the service reaches the given state.

        Checks the current model state first. If already matching,
        returns immediately. Otherwise, registers a waiter on the
        namespace model that resolves when the state matches.

        Args:
            state: State matcher (e.g. distvirt.active, distvirt.idle).
            timeout: Maximum seconds to wait. None = wait forever.

        Returns:
            The ServiceModel at the time the state matched.

        Raises:
            asyncio.TimeoutError: If timeout expires before state is reached.
        """
        model = self._namespace._model

        # Check current state
        svc = model.services.get(self._service_id)
        if svc is not None and state.matches(svc):
            return svc

        # Register waiter
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()

        def predicate(m: Any) -> bool:
            svc = m.services.get(self._service_id)
            return svc is not None and state.matches(svc)

        model._waiters.append((predicate, future))

        try:
            await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            model._waiters = [
                (p, f) for p, f in model._waiters if f is not future
            ]
            raise

        return model.services[self._service_id]
