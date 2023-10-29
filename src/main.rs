mod mmap_file;
mod server;
mod message;
mod http_server;
mod commit_log;
mod index;
mod topic_mgr;

use std::collections::HashMap;
use std::error::Error;
use std::net::SocketAddr;
use config::Config;

use http_server::HttpServer;
use crate::server::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::builder()
        .add_source(config::File::with_name("./config.toml"))
        .build()
        .unwrap();

    println!(
        "{:?}",
        config.clone()
            .try_deserialize::<HashMap<String, String>>()
            .unwrap()
    );

    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    let http_server = HttpServer;
    http_server.start(addr, &config).await;

    Ok(())
}