use std::net::SocketAddr;
use async_trait::async_trait;
use crate::config::ConfigOptions;

#[async_trait]
pub trait Server: Send + Sync {
    /// Starts the server.
    async fn start(&self, listening: SocketAddr, config: ConfigOptions);
}
