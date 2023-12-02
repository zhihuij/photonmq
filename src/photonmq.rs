mod server;
mod message;
mod http_server;
mod topic_mgr;
mod config;
mod error;
mod storage;

use std::error::Error;
use std::net::SocketAddr;

use http_server::HttpServer;
use crate::config::ConfigOptions;
use crate::server::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config_options = ConfigOptions::load_layered_options().unwrap();
    println!("{:?}", config_options);

    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    let http_server = HttpServer;
    http_server.start(addr, config_options).await;

    Ok(())
}