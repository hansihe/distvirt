use std::env;

use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

use crate::config;

pub struct ConnectionParams {
    pub server: String,
    pub token: Option<String>,
}

/// Resolve connection parameters with precedence:
/// 1. CLI flags (--server, --token)
/// 2. Env vars (DV_SERVER, DV_TOKEN)
/// 3. Active context from credentials file
pub fn resolve(
    cli_server: Option<&str>,
    cli_token: Option<&str>,
    cli_context: Option<&str>,
) -> anyhow::Result<ConnectionParams> {
    let creds = config::load()?;

    // Determine which context to use
    let context_name = cli_context.unwrap_or(&creds.current_context);
    let context = creds.contexts.get(context_name);

    // Server: CLI flag > env var > context > default
    let server = if let Some(s) = cli_server {
        s.to_string()
    } else if let Ok(s) = env::var("DV_SERVER") {
        s
    } else if let Some(ctx) = context {
        ctx.server.clone()
    } else {
        "http://[::1]:9090".to_string()
    };

    // Token: CLI flag > env var > context
    let token = if let Some(t) = cli_token {
        Some(t.to_string())
    } else if let Ok(t) = env::var("DV_TOKEN") {
        Some(t)
    } else {
        context.map(|ctx| ctx.token.clone())
    };

    Ok(ConnectionParams { server, token })
}

#[derive(Clone)]
pub struct AuthInterceptor {
    token: Option<String>,
}

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        if let Some(ref token) = self.token {
            let value = format!("Bearer {}", token)
                .parse()
                .map_err(|_| tonic::Status::internal("invalid auth token"))?;
            request
                .metadata_mut()
                .insert("authorization", value);
        }
        Ok(request)
    }
}

pub type AuthChannel = InterceptedService<Channel, AuthInterceptor>;

pub async fn connect(params: &ConnectionParams) -> anyhow::Result<AuthChannel> {
    // Ensure the server address has a scheme
    let endpoint = if params.server.starts_with("http://") || params.server.starts_with("https://") {
        params.server.clone()
    } else {
        format!("http://{}", params.server)
    };

    let channel = Channel::from_shared(endpoint)?
        .connect()
        .await?;

    let interceptor = AuthInterceptor {
        token: params.token.clone(),
    };

    Ok(InterceptedService::new(channel, interceptor))
}
