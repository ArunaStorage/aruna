use anyhow::anyhow;
use anyhow::Result;
use aruna_rust_api::api::dataproxy::services::v2::bundler_service_server::BundlerServiceServer;
use aruna_rust_api::api::dataproxy::services::v2::dataproxy_service_server::DataproxyServiceServer;
use aruna_rust_api::api::dataproxy::services::v2::dataproxy_user_service_server::DataproxyUserServiceServer;
use caching::cache::Cache;
use data_backends::{s3_backend::S3Backend, storage_backend::StorageBackend};
use grpc_api::bundler::BundlerServiceImpl;
use grpc_api::{proxy_service::DataproxyServiceImpl, user_service::DataproxyUserServiceImpl};
use std::{net::SocketAddr, str::FromStr, sync::Arc};
use tokio::try_join;
use tonic::transport::Server;
use tracing::debug;
use tracing::info_span;
use tracing::trace;
use tracing::Instrument;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;
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
use futures_util::TryFutureExt;

#[tracing::instrument(level = "trace", skip())]
#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::from_filename(".env").ok();

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or("none".into())
        .add_directive("aos_data_proxy=trace".parse()?);

    let subscriber = tracing_subscriber::fmt()
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        // Use a more compact, abbreviated log format
        .compact()
        // Set LOG_LEVEL to
        .with_env_filter(filter)
        // Display source code file paths
        .with_file(true)
        // Display source code line numbers
        .with_line_number(true)
        .with_target(false)
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    let span = info_span!("INIT ENV");
    let guard = span.enter();

    let remote_synced =
        trace_err!(trace_err!(dotenvy::var("DATA_PROXY_REMOTE_SYNCED"))?.parse::<bool>())?;
    debug!(target = "DATA_PROXY_REMOTE_SYNCED", value = remote_synced);
    let aruna_host_url = if let true = remote_synced {
        Some(trace_err!(dotenvy::var("ARUNA_HOST_URL"))?)
    } else {
        None
    };
    debug!(target = "ARUNA_HOST_URL", value = aruna_host_url);
    let with_persistence =
        trace_err!(trace_err!(dotenvy::var("DATA_PROXY_PERSISTENCE"))?.parse::<bool>())?;
    debug!(target = "DATA_PROXY_PERSISTENCE", value = with_persistence);
    let hostname = trace_err!(dotenvy::var("DATA_PROXY_DATA_SERVER"))?;
    debug!(target = "DATA_PROXY_DATA_SERVER", value = hostname);
    // ULID of the endpoint
    let endpoint_id = trace_err!(dotenvy::var("DATA_PROXY_ENDPOINT_ID"))?;
    debug!(target = "DATA_PROXY_ENDPOINT_ID", value = endpoint_id);
    let data_proxy_grpc_addr =
        trace_err!(trace_err!(dotenvy::var("DATA_PROXY_GRPC_SERVER"))?.parse::<SocketAddr>())?;
    debug!(
        target = "DATA_PROXY_GRPC_SERVER",
        value = ?data_proxy_grpc_addr
    );

    let encoding_key = trace_err!(dotenvy::var("DATA_PROXY_ENCODING_KEY"))?;
    let encoding_key_serial =
        trace_err!(trace_err!(dotenvy::var("DATA_PROXY_PUBKEY_SERIAL"))?.parse::<i32>())?;

    drop(guard);

    trace!("init storage backend");
    let storage_backend: Arc<Box<dyn StorageBackend>> =
        Arc::new(Box::new(S3Backend::new(endpoint_id.to_string()).await?));

    trace!("init cache");
    let cache = Cache::new(
        aruna_host_url,
        with_persistence,
        diesel_ulid::DieselUlid::from_str(&endpoint_id)?,
        encoding_key,
        encoding_key_serial,
    )
    .await?;

    let cache_clone = cache.clone();

    trace!("init s3 server");
    let s3_server = s3_frontend::s3server::S3Server::new(
        &hostname,
        hostname.to_string(),
        storage_backend,
        cache,
    )
    .await?;

    trace!("init grpc server");
    let grpc_server_handle = trace_err!(tokio::spawn(
        async move {
            Server::builder()
                .add_service(DataproxyServiceServer::new(DataproxyServiceImpl::new(
                    cache_clone.clone(),
                )))
                .add_service(DataproxyUserServiceServer::new(
                    DataproxyUserServiceImpl::new(cache_clone.clone()),
                ))
                .add_service(BundlerServiceServer::new(BundlerServiceImpl::new(
                    cache_clone.clone(),
                    hostname.clone(),
                    false,
                )))
                .serve(data_proxy_grpc_addr)
                .await
        }
        .instrument(info_span!("grpc_server_run"))
    ))
    .map_err(|e| anyhow!("an error occured {e}"));

    match try_join!(s3_server.run(), grpc_server_handle) {
        Ok(_) => Ok(()),
        Err(err) => {
            log::error!("{}", err);
            Err(err)
        }
    }
}
