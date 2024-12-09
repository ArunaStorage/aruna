use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use s3s::{host::MultiDomain, service::S3ServiceBuilder};
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::info;

use crate::client::ServerClient;
use crate::s3::auth::AuthProvider;
use crate::s3::route::CustomRoute;
use crate::{error::ProxyError, lmdbstore::LmdbStore};
use crate::{logerr, CONFIG};

use super::{access::AccessChecker, service::ArunaS3Service};

pub async fn run_server(storage: Arc<LmdbStore>, client: ServerClient) -> Result<(), ProxyError> {
    let aruna_s3_service = ArunaS3Service::new(storage.clone(), client.clone());

    let service = {
        let mut builder = S3ServiceBuilder::new(aruna_s3_service);

        builder.set_auth(AuthProvider::new(storage.clone()));
        builder.set_access(AccessChecker::new(storage, client));
        builder.set_host(
            MultiDomain::new(&[CONFIG.frontend.hostname.clone()]).inspect_err(logerr!())?,
        );
        builder.set_route(CustomRoute {});
        builder.build()
    };

    let listener = TcpListener::bind(("0.0.0.0", 1337))
        .await
        .inspect_err(logerr!())?;

    let local_addr = listener.local_addr()?;

    let connection = ConnBuilder::new(TokioExecutor::new());
    let shared = service.into_shared();

    let server = async move {
        loop {
            let (socket, _) = match listener.accept().await {
                Ok(ok) => ok,
                Err(err) => {
                    tracing::error!("error accepting connection: {err}");
                    continue;
                }
            };
            let service = shared.clone();
            let conn = connection.clone();
            tokio::spawn(async move {
                let _ = conn.serve_connection(TokioIo::new(socket), service).await;
            });
        }
    };

    info!("server is running at http://{local_addr}");

    server.await; // This will never return
    Ok(())
}
