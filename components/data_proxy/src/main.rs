use std::{str::FromStr, sync::Arc};

use data_backends::{s3_backend::S3Backend, storage_backend::StorageBackend};
use futures::try_join;
use service_server::server::{InternalServerImpl, ProxyServer};
use std::io::Write;

use crate::{
    bundler::{
        bundler::Bundler, bundler_web_server::run_axum,
        internal_bundler_service::InternalBundlerServiceImpl,
    },
    data_server::{
        data_handler::DataHandler, s3server::S3Server, utils::settings::ServiceSettings,
    },
};

mod bundler;
mod data_backends;
mod data_server;
mod helpers;
mod service_server;

#[tokio::main]
async fn main() {
    dotenvy::from_filename(".env").ok();

    let hostname = dotenvy::var("PROXY_HOSTNAME").unwrap();
    // External S3 server
    let proxy_data_host = dotenvy::var("PROXY_DATA_HOST").unwrap();
    // ULID of the endpoint
    let endpoint_id = dotenvy::var("ENDPOINT_ID").unwrap();
    // Aruna Backend
    let backend_host = dotenvy::var("BACKEND_HOST").unwrap();
    // Internal backchannel Aruna -> Dproxy
    let internal_backend_host = dotenvy::var("BACKEND_HOST_INTERNAL").unwrap();
    // Optional Bundler URL
    let external_bundler_url = dotenvy::var("BUNDLER_URL").ok();

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

    let data_handler = Arc::new(
        DataHandler::new(
            storage_backend.clone(),
            backend_host.to_string(),
            ServiceSettings {
                endpoint_id: rusty_ulid::Ulid::from_str(&endpoint_id).unwrap(),
                ..Default::default()
            },
        )
        .await
        .unwrap(),
    );

    let data_server = S3Server::new(
        &proxy_data_host,
        hostname,
        backend_host.to_string(),
        storage_backend.clone(),
        data_handler.clone(),
        endpoint_id.to_string(),
    )
    .await
    .unwrap();

    let internal_proxy_server =
        InternalServerImpl::new(storage_backend.clone(), data_handler.clone())
            .await
            .unwrap();
    let internal_proxy_socket = internal_backend_host.parse().unwrap();

    match external_bundler_url {
        Some(bundler_url) => {
            let bundl = Bundler::new(backend_host, endpoint_id).await.unwrap();

            let internal_bundler = InternalBundlerServiceImpl::new(bundl.clone());

            let internal_proxy_server = ProxyServer::new(
                Some(Arc::new(internal_bundler)),
                Arc::new(internal_proxy_server),
                internal_proxy_socket,
            )
            .await
            .unwrap();

            let axum_handle = run_axum(bundler_url, bundl.clone(), storage_backend.clone());

            log::info!("Starting proxy, dataserver and axum server");
            let _end = match try_join!(
                data_server.run(),
                internal_proxy_server.serve(),
                axum_handle
            ) {
                Ok(value) => value,
                Err(err) => {
                    log::error!("{}", err);
                    return;
                }
            };
        }
        None => {
            let internal_proxy_server =
                ProxyServer::new(None, Arc::new(internal_proxy_server), internal_proxy_socket)
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
    }
}
