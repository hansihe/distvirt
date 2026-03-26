use std::path::PathBuf;
use std::time::Duration;

/// Wrapper around the CH API socket path, providing a clean interface
/// for making API requests to a Cloud Hypervisor instance.
pub(super) struct ApiClient {
    socket_path: PathBuf,
}

impl ApiClient {
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        ApiClient {
            socket_path: socket_path.into(),
        }
    }

    pub async fn request(
        &self,
        method: &str,
        path: &str,
        body: Option<&serde_json::Value>,
    ) -> anyhow::Result<()> {
        crate::vmm::api_request(method, &self.socket_path, path, body).await
    }

    #[allow(dead_code)]
    pub async fn request_with_timeout(
        &self,
        method: &str,
        path: &str,
        body: Option<&serde_json::Value>,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        crate::vmm::api_request_with_timeout(method, &self.socket_path, path, body, timeout).await
    }
}
