use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use data_server::server::DataServer;
use futures::try_join;
use presign_handler::signer::PresignHandler;
use service_server::server::{InternalServerImpl, ProxyServer};
use std::io::Write;
use storage_backend::s3_backend::S3Backend;

mod api;
mod data_middleware;
mod data_server;
mod presign_handler;
mod service_server;
mod storage_backend;

#[tokio::main]
async fn main() {
    env_logger::Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
                "{}:{} {} [{}] - {}",
                record.file().unwrap_or("unknown"),
                record.line().unwrap_or(0),
                chrono::Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.level(),
                record.args()
            )
        })
        .filter_level(log::LevelFilter::Info)
        .init();

    let s3_client = match S3Backend::new().await {
        Ok(value) => value,
        Err(err) => {
            log::error!("{}", err);
            return;
        }
    };
    let s3_client_arc = Arc::new(s3_client);

    let signer = match PresignHandler::new() {
        Ok(value) => value,
        Err(err) => {
            log::error!("{}", err);
            return;
        }
    };
    let signer_arc = Arc::new(signer);

    let data_socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

    let data_server = DataServer::new(s3_client_arc.clone(), signer_arc.clone(), data_socket)
        .await
        .unwrap();

    let internal_proxy_server = InternalServerImpl::new(s3_client_arc.clone(), signer_arc)
        .await
        .unwrap();
    let internal_proxy_socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8081);

    let internal_proxy_server =
        ProxyServer::new(Arc::new(internal_proxy_server), internal_proxy_socket)
            .await
            .unwrap();

    let _end = match try_join!(data_server.serve(), internal_proxy_server.serve()) {
        Ok(value) => value,
        Err(err) => {
            log::error!("{}", err);
            return;
        }
    };
}
