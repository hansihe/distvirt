use distvirt_client_protocol::DistvirtClientClient;

use crate::connection::{self, AuthChannel, ConnectionParams};

pub type Client = DistvirtClientClient<AuthChannel>;

pub async fn connect(params: &ConnectionParams) -> anyhow::Result<Client> {
    let channel = connection::connect(params).await?;
    Ok(DistvirtClientClient::new(channel))
}

pub fn handle_grpc_error(status: tonic::Status) -> anyhow::Error {
    match status.code() {
        tonic::Code::NotFound => anyhow::anyhow!("not found: {}", status.message()),
        tonic::Code::AlreadyExists => anyhow::anyhow!("already exists: {}", status.message()),
        tonic::Code::InvalidArgument => {
            anyhow::anyhow!("invalid argument: {}", status.message())
        }
        tonic::Code::PermissionDenied => {
            anyhow::anyhow!("permission denied: {}", status.message())
        }
        tonic::Code::Unauthenticated => {
            anyhow::anyhow!(
                "unauthenticated: {}. Run `dv login` to configure credentials.",
                status.message()
            )
        }
        tonic::Code::Unavailable => {
            anyhow::anyhow!("server unavailable: {}", status.message())
        }
        _ => anyhow::anyhow!("gRPC error ({}): {}", status.code(), status.message()),
    }
}
