"""Type stubs for the Rust PyO3 extension module."""

# ---------------------------------------------------------------------------
# Exception hierarchy
# ---------------------------------------------------------------------------

class DistvirtError(Exception):
    """Base exception for all distvirt SDK errors."""
    ...

class SpecError(DistvirtError):
    """Spec parsing, validation, or resolution error."""
    ...

class ConnectionError(DistvirtError):
    """Connection resolution or transport error."""
    ...

class ApiError(DistvirtError):
    """gRPC API error or protocol-level failure."""
    ...

# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------

def parse_spec(
    path: str,
    values: dict[str, str] | None = None,
) -> tuple[str | None, bytes]:
    """Parse a distvirt spec file into (namespace_id, protobuf bytes).

    Args:
        path: Path to a distvirt.yaml or docker-compose.yml file.
        values: Variable substitutions for ${VAR} in fragment includes.

    Returns:
        Tuple of (namespace_id from metadata.name or None, serialized NamespaceSpec proto bytes).

    Raises:
        SpecError: If the spec file has parse or validation errors.
    """
    ...

def resolve_connection(
    server: str | None = None,
    token: str | None = None,
    context: str | None = None,
) -> tuple[str, str | None]:
    """Resolve connection parameters using CLI precedence.

    Raises:
        ConnectionError: If credentials file cannot be read or parsed.
    """
    ...

# ---------------------------------------------------------------------------
# PyClient
# ---------------------------------------------------------------------------

class PyClient:
    """Async gRPC client backed by the Rust tonic client."""

    @staticmethod
    async def connect(
        server: str | None = None,
        token: str | None = None,
        context: str | None = None,
    ) -> PyClient:
        """Connect to a distvirt orchestrator.

        Raises:
            ConnectionError: If connection resolution or transport fails.
        """
        ...

    async def apply(self, namespace_id: str, spec_bytes: bytes) -> str:
        """Apply a namespace spec. Returns "created" or "patched".

        Raises:
            ApiError: On gRPC or decode errors.
        """
        ...

    async def sync_ns(self, namespace_id: str, spec_bytes: bytes) -> str:
        """Sync a namespace spec. Returns "created" or "synced".

        Raises:
            ApiError: On gRPC or decode errors.
        """
        ...

    async def down(self, namespace_id: str) -> None:
        """Delete a namespace.

        Raises:
            ApiError: On gRPC errors.
        """
        ...

    async def clone_namespace(self, source: str, target: str) -> None:
        """Clone a namespace from source to target.

        Raises:
            ApiError: On gRPC errors.
        """
        ...

    async def deactivate(self, namespace_id: str, workload_id: str) -> tuple[bool, str]:
        """Deactivate a workload. Returns (deactivated, reason).

        Raises:
            ApiError: On gRPC errors.
        """
        ...

    async def get_status(self, namespace_id: str) -> bytes:
        """Get namespace status as serialized protobuf bytes.

        Raises:
            ApiError: On gRPC errors.
        """
        ...

    async def list_namespaces(self) -> list[tuple[str, bytes]]:
        """List all namespaces. Returns list of (namespace_id, status_proto_bytes).

        Raises:
            ApiError: On gRPC errors.
        """
        ...

    def close(self) -> None:
        """Close the client, dropping the inner gRPC connection."""
        ...

# ---------------------------------------------------------------------------
# NamespaceModel
# ---------------------------------------------------------------------------

class NamespaceModel:
    """Live namespace state model backed by Rust."""

    @staticmethod
    def from_status_bytes(proto_bytes: bytes) -> NamespaceModel:
        """Create from serialized NamespaceStatusReport.

        Raises:
            ApiError: If protobuf bytes cannot be decoded.
        """
        ...

    def apply_event_bytes(self, proto_bytes: bytes) -> bool:
        """Apply a serialized NamespaceEvent. Returns True if model changed.

        Raises:
            ApiError: If protobuf bytes cannot be decoded.
        """
        ...

    @property
    def namespace_id(self) -> str: ...
    @property
    def namespace_state(self) -> str: ...
    def workload_ids(self) -> list[str]: ...
    def service_ids(self) -> list[str]: ...
    def workload_state(self, workload_id: str) -> str | None: ...
    def workload_info(self, workload_id: str) -> dict[str, object] | None: ...
    def service_state(self, service_id: str) -> str | None: ...
    def service_info(self, service_id: str) -> dict[str, object] | None: ...
