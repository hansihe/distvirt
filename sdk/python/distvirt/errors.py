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

import grpclib.const


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


# -------------------------------------------------------------------------
# gRPC error mapping (mirrors distvirt-client handle_grpc_error)
# -------------------------------------------------------------------------

_STATUS_MESSAGES = {
    grpclib.const.Status.NOT_FOUND: "not found",
    grpclib.const.Status.ALREADY_EXISTS: "already exists",
    grpclib.const.Status.INVALID_ARGUMENT: "invalid argument",
    grpclib.const.Status.PERMISSION_DENIED: "permission denied",
    grpclib.const.Status.UNAUTHENTICATED: "unauthenticated",
    grpclib.const.Status.UNAVAILABLE: "server unavailable",
}


def handle_grpc_error(exc: grpclib.GRPCError) -> ApiError:
    """Convert a grpclib.GRPCError into a typed ApiError.

    Use in except blocks::

        try:
            await stub.some_rpc(request)
        except grpclib.GRPCError as e:
            raise handle_grpc_error(e) from e
    """
    prefix = _STATUS_MESSAGES.get(exc.status)
    detail = exc.message or ""
    if prefix:
        message = f"{prefix}: {detail}" if detail else prefix
    else:
        message = f"gRPC error ({exc.status.name}): {detail}"
    return ApiError(message)


__all__ = [
    "DistvirtError",
    "SpecError",
    "ConnectionError",
    "ApiError",
    "StreamEndedError",
    "TimeoutError",
    "handle_grpc_error",
]
