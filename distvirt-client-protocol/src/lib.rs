pub mod proto {
    tonic::include_proto!("distvirt.client.v1");
}

pub use proto::distvirt_client_client::DistvirtClientClient;
pub use proto::distvirt_client_server::{DistvirtClient, DistvirtClientServer};
pub use proto::*;
