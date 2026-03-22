"""distvirt SDK — async Python client for distvirt orchestrator."""

from distvirt.client import Client, connect
from distvirt.errors import (
    DistvirtError,
    SpecError,
    ConnectionError,
    ApiError,
    StreamEndedError,
    TimeoutError,
)
from distvirt.namespace import Namespace
from distvirt.workload import Workload
from distvirt.service import Service
from distvirt.network import Network, UdpConnection
from distvirt.states import (
    dormant,
    launching,
    running,
    completed,
    failed,
    suspended,
    idle,
    active,
)

__all__ = [
    # Client
    "Client",
    "connect",
    # Handles
    "Namespace",
    "Workload",
    "Service",
    # Network
    "Network",
    "UdpConnection",
    # Errors
    "DistvirtError",
    "SpecError",
    "ConnectionError",
    "ApiError",
    "StreamEndedError",
    "TimeoutError",
    # State matchers
    "dormant",
    "launching",
    "running",
    "completed",
    "failed",
    "suspended",
    "idle",
    "active",
]
