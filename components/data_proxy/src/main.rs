use anyhow::anyhow;
use anyhow::Result;
use aruna_rust_api::api::dataproxy::services::v2::dataproxy_service_server::DataproxyServiceServer;
use aruna_rust_api::api::dataproxy::services::v2::dataproxy_user_service_server::DataproxyUserServiceServer;
use caching::cache::Cache;
use data_backends::{s3_backend::S3Backend, storage_backend::StorageBackend};
use futures_util::TryFutureExt;
use grpc_api::{proxy_service::DataproxyServiceImpl, user_service::DataproxyUserServiceImpl};
use simple_logger::SimpleLogger;
use std::{net::SocketAddr, str::FromStr, sync::Arc};
use tokio::try_join;
use tonic::transport::Server;

mod bundler;
mod caching;
mod data_backends;
mod database;
mod s3_frontend;
// mod helpers;
mod grpc_api;
mod structs;
#[macro_use]
mod macros;
mod helpers;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::from_filename(".env").ok();

    let remote_synced = dotenvy::var("DATA_PROXY_REMOTE_SYNCED")?.parse::<bool>()?;
    let aruna_host_url = if let true = remote_synced {
        Some(dotenvy::var("ARUNA_HOST_URL")?)
    } else {
        None
    };
    let with_persistence = dotenvy::var("DATA_PROXY_PERSISTENCE")?.parse::<bool>()?;

    let hostname = dotenvy::var("DATA_PROXY_DATA_SERVER")?;
    // ULID of the endpoint
    let endpoint_id = dotenvy::var("DATA_PROXY_ENDPOINT_ID")?;

    //
    let data_proxy_grpc_addr = dotenvy::var("DATA_PROXY_GRPC_SERVER")?.parse::<SocketAddr>()?;

    // Init logger
    SimpleLogger::new()
        .with_level(log::LevelFilter::Debug)
        .env()
        .init()?;

    let encoding_key = dotenvy::var("DATA_PROXY_ENCODING_KEY")?;
    let encoding_key_serial = dotenvy::var("DATA_PROXY_PUBKEY_SERIAL")?.parse::<i32>()?;

    let storage_backend: Arc<Box<dyn StorageBackend>> =
        Arc::new(Box::new(S3Backend::new(endpoint_id.to_string()).await?));

    let cache = Cache::new(
        aruna_host_url,
        with_persistence,
        diesel_ulid::DieselUlid::from_str(&endpoint_id)?,
        encoding_key,
        encoding_key_serial,
    )
    .await?;

    let cache_clone = cache.clone();

    let grpc_server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(DataproxyServiceServer::new(DataproxyServiceImpl::new(
                cache_clone.clone(),
            )))
            .add_service(DataproxyUserServiceServer::new(
                DataproxyUserServiceImpl::new(cache_clone.clone()),
            ))
            .serve(data_proxy_grpc_addr)
            .await
    })
    .map_err(|e| anyhow!("an error occured {e}"));

    let s3_server = s3_frontend::s3server::S3Server::new(
        &hostname,
        hostname.to_string(),
        storage_backend,
        cache,
    )
    .await?;

    match try_join!(s3_server.run(), grpc_server_handle) {
        Ok(_) => Ok(()),
        Err(err) => {
            log::error!("{}", err);
            Err(err)
        }
    }
}
