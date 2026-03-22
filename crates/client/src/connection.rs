use std::env;
use std::time::Duration;

use snafu::ResultExt;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

use distvirt_client_protocol::DistvirtClientClient;

use crate::errors::*;

pub struct ConnectionParams {
    pub server: String,
    pub token: Option<String>,
}

/// Optional overrides for connection resolution.
/// Fields that are `None` fall through to the next source in the precedence chain.
#[derive(Default)]
pub struct ConnectionOverrides {
    pub server: Option<String>,
    pub token: Option<String>,
    pub context: Option<String>,
}

/// Resolve connection parameters with precedence:
/// 1. Explicit overrides (e.g. CLI flags)
/// 2. Env vars (DV_SERVER, DV_TOKEN)
/// 3. Active context from credentials file
/// 4. Default (http://[::1]:9090)
pub fn resolve(overrides: ConnectionOverrides) -> Result<ConnectionParams, ConnectionError> {
    let creds = crate::config::load()?;

    let context_name = overrides
        .context
        .as_deref()
        .unwrap_or(&creds.current_context);
    let context = creds.contexts.get(context_name);

    let server = if let Some(s) = overrides.server {
        s
    } else if let Ok(s) = env::var("DV_SERVER") {
        s
    } else if let Some(ctx) = context {
        ctx.server.clone()
    } else {
        "http://[::1]:9090".to_string()
    };

    let token = if let Some(t) = overrides.token {
        Some(t)
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
    ) -> std::result::Result<tonic::Request<()>, tonic::Status> {
        if let Some(ref token) = self.token {
            let value = format!("Bearer {}", token)
                .parse()
                .map_err(|_| tonic::Status::internal("invalid auth token"))?;
            request.metadata_mut().insert("authorization", value);
        }
        Ok(request)
    }
}

pub type AuthChannel = InterceptedService<Channel, AuthInterceptor>;

pub type Client = DistvirtClientClient<AuthChannel>;

pub async fn connect(params: &ConnectionParams) -> Result<Client, ConnectionError> {
    let endpoint = if params.server.starts_with("http://") || params.server.starts_with("https://")
    {
        params.server.clone()
    } else {
        format!("http://{}", params.server)
    };

    log::debug!("connecting to {}", endpoint);
    let channel = Channel::from_shared(endpoint)
        .map_err(|e| ConnectionError::InvalidEndpoint {
            message: format!("invalid endpoint: {e}"),
        })?
        .connect_timeout(Duration::from_secs(5))
        .connect()
        .await
        .context(TransportSnafu)?;

    let interceptor = AuthInterceptor {
        token: params.token.clone(),
    };

    Ok(DistvirtClientClient::new(InterceptedService::new(
        channel,
        interceptor,
    )))
}

pub fn handle_grpc_error(status: tonic::Status) -> ApiError {
    let message = match status.code() {
        tonic::Code::NotFound => format!("not found: {}", status.message()),
        tonic::Code::AlreadyExists => format!("already exists: {}", status.message()),
        tonic::Code::InvalidArgument => {
            format!("invalid argument: {}", status.message())
        }
        tonic::Code::PermissionDenied => {
            format!("permission denied: {}", status.message())
        }
        tonic::Code::Unauthenticated => {
            format!("unauthenticated: {}", status.message())
        }
        tonic::Code::Unavailable => {
            format!("server unavailable: {}", status.message())
        }
        _ => format!("gRPC error ({}): {}", status.code(), status.message()),
    };
    ApiError::Status { message }
}
