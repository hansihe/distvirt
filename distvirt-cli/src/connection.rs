use distvirt_client::connection::{ConnectionOverrides, ConnectionParams};

/// Resolve connection parameters, mapping CLI flags to client overrides.
pub fn resolve(
    cli_server: Option<&str>,
    cli_token: Option<&str>,
    cli_context: Option<&str>,
) -> anyhow::Result<ConnectionParams> {
    distvirt_client::connection::resolve(ConnectionOverrides {
        server: cli_server.map(|s| s.to_string()),
        token: cli_token.map(|t| t.to_string()),
        context: cli_context.map(|c| c.to_string()),
    })
}
