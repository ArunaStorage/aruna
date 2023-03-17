use std::sync::Arc;

use backends::{s3_backend::S3Backend, storage_backend::StorageBackend};
use futures::try_join;
use service_server::server::{InternalServerImpl, ProxyServer};
use std::io::Write;

use crate::data_server::{data_handler::DataHandler, s3service::ServiceSettings, server::S3Server};

mod backends;
mod data_server;
mod helpers;
mod service_server;

#[tokio::main]
async fn main() {
    dotenv::from_filename(".env").ok();

    let hostname = dotenv::var("PROXY_HOSTNAME").unwrap();
    let endpoint_id = dotenv::var("ENDPOINT_ID").unwrap();

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
        .filter_level(log::LevelFilter::Debug)
        .init();

    let s3_client = match S3Backend::new().await {
        Ok(value) => value,
        Err(err) => {
            log::error!("{}", err);
            return;
        }
    };
    let storage_backend: Arc<Box<dyn StorageBackend>> = Arc::new(Box::new(s3_client));

    let data_socket = format!("{hostname}:8080");
    let aruna_server = dotenv::var("BACKEND_HOST").unwrap();

    let data_handler = Arc::new(
        DataHandler::new(
            storage_backend.clone(),
            aruna_server.to_string(),
            ServiceSettings {
                endpoint_id: uuid::Uuid::parse_str(&endpoint_id).unwrap(),
                ..Default::default()
            },
        )
        .await
        .unwrap(),
    );

    let data_server = S3Server::new(
        &data_socket,
        aruna_server,
        storage_backend.clone(),
        data_handler,
    )
    .await
    .unwrap();

    let internal_proxy_server = InternalServerImpl::new(storage_backend.clone())
        .await
        .unwrap();
    let internal_proxy_socket = format!("{hostname}:8081").parse().unwrap();

    let internal_proxy_server =
        ProxyServer::new(Arc::new(internal_proxy_server), internal_proxy_socket)
            .await
            .unwrap();

    log::info!("Starting proxy and dataserver");
    let _end = match try_join!(data_server.run(), internal_proxy_server.serve()) {
        Ok(value) => value,
        Err(err) => {
            log::error!("{}", err);
            return;
        }
    };
}
