use anyhow::Result;
use caching::cache::Cache;
use data_backends::{s3_backend::S3Backend, storage_backend::StorageBackend};
use std::{io::Write, str::FromStr, sync::Arc};
use tokio::try_join;

// mod bundler;
mod caching;
mod data_backends;
mod database;
mod s3_frontend;
// mod helpers;
mod grpc_api;
mod structs;
#[macro_use]
mod macros;

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

    let storage_backend: Arc<Box<dyn StorageBackend>> =
        Arc::new(Box::new(S3Backend::new(endpoint_id.to_string()).await?));

    let cache = Cache::new(
        aruna_host_url,
        with_persistence,
        diesel_ulid::DieselUlid::from_str(&endpoint_id)?,
    )
    .await?;

    let s3_server =
        s3_frontend::s3server::S3Server::new("0.0.0.0:9000", hostname, storage_backend, cache)
            .await?;

    match try_join!(s3_server.run()) {
        Ok(_) => Ok(()),
        Err(err) => {
            log::error!("{}", err);
            Err(err)
        }
    }
}
//      {
//         Ok(value) => value,
//         Err(err) => {
//             log::error!("{}", err);
//             return;
//         }
//     };
//     let storage_backend: Arc<Box<dyn StorageBackend>> = Arc::new(Box::new(s3_client));

//     let data_handler = Arc::new(
//         DataHandler::new(
//             storage_backend.clone(),
//             backend_host.to_string(),
//             ServiceSettings {
//                 endpoint_id: rusty_ulid::Ulid::from_str(&endpoint_id).unwrap(),
//                 ..Default::default()
//             },
//         )
//         .await
//         .unwrap(),
//     );

//     let data_server = S3Server::new(
//         &proxy_data_host,
//         hostname,
//         backend_host.to_string(),
//         storage_backend.clone(),
//         data_handler.clone(),
//         endpoint_id.to_string(),
//     )
//     .await
//     .unwrap();

//     let internal_proxy_server =
//         InternalServerImpl::new(storage_backend.clone(), data_handler.clone())
//             .await
//             .unwrap();
//     let internal_proxy_socket = internal_backend_host.parse().unwrap();

//     match external_bundler_url {
//         Some(bundler_url) => {
//             let bundl = Bundler::new(backend_host, endpoint_id).await.unwrap();

//             let internal_bundler = InternalBundlerServiceImpl::new(bundl.clone());

//             let internal_proxy_server = ProxyServer::new(
//                 Some(Arc::new(internal_bundler)),
//                 Arc::new(internal_proxy_server),
//                 internal_proxy_socket,
//             )
//             .await
//             .unwrap();

//             let axum_handle = run_axum(bundler_url, bundl.clone(), storage_backend.clone());

//             log::info!("Starting proxy, dataserver and axum server");
//             let _end = match try_join!(
//                 data_server.run(),
//                 internal_proxy_server.serve(),
//                 axum_handle
//             ) {
//                 Ok(value) => value,
//                 Err(err) => {
//                     log::error!("{}", err);
//                     return;
//                 }
//             };
//         }
//         None => {
//             let internal_proxy_server =
//                 ProxyServer::new(None, Arc::new(internal_proxy_server), internal_proxy_socket)
//                     .await
//                     .unwrap();

//             log::info!("Starting proxy and dataserver");
//             let _end = match try_join!(data_server.run(), internal_proxy_server.serve()) {
//                 Ok(value) => value,
//                 Err(err) => {
//                     log::error!("{}", err);
//                     return;
//                 }
//             };
//         }
//     }
// }
