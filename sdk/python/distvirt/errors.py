"""Exception hierarchy for the distvirt SDK.

Base exceptions (DistvirtError, SpecError, ConnectionError, ApiError) are
defined in the Rust _core extension so that Rust code can raise them
directly. This module re-exports them and adds pure-Python subtypes.
"""

from __future__ import annotations

from distvirt._core import (
    DistvirtError,
    SpecError,
    ConnectionError,
    ApiError,
)


class StreamEndedError(ApiError):
    """The event stream ended unexpectedly."""


class TimeoutError(DistvirtError):
    """A wait_for() call timed out.

    Attributes:
        entity_type: "workload" or "service".
        entity_id: The workload/service ID being waited on.
        target_state: The state that was being waited for.
        timeout: The timeout value in seconds.
    """

    def __init__(
        self,
        entity_type: str,
        entity_id: str,
        target_state: str,
        timeout: float,
    ):
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.target_state = target_state
        self.timeout = timeout
        super().__init__(
            f"timed out waiting for {entity_type} {entity_id!r} "
            f"to reach {target_state!r} after {timeout}s"
        )


__all__ = [
    "DistvirtError",
    "SpecError",
    "ConnectionError",
    "ApiError",
    "StreamEndedError",
    "TimeoutError",
]
