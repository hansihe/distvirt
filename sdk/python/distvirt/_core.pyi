"""Type stubs for the Rust PyO3 extension module."""

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
        ValueError: If the spec file has validation errors.
        FileNotFoundError: If the spec file or included fragments don't exist.
    """
    ...
