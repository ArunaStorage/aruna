use anyhow::Result;
use hyper::Server;
use s3s::service::S3Service;
use std::{net::TcpListener, sync::Arc};
use tracing::info;

use crate::data_backends::storage_backend::StorageBackend;

use super::{auth::AuthProvider, data_handler::DataHandler, s3service::S3ServiceServer};

pub struct S3Server {
    s3service: S3Service,
    address: String,
}

impl S3Server {
    pub async fn new(
        address: impl Into<String> + Copy,
        hostname: impl Into<String>,
        aruna_server: impl Into<String>,
        backend: Arc<Box<dyn StorageBackend>>,
        data_handler: Arc<DataHandler>,
        endpoint_id: impl Into<String>,
    ) -> Result<Self> {
        let server_url = aruna_server.into();

        let mut service = S3Service::new(Box::new(
            S3ServiceServer::new(backend, data_handler, endpoint_id.into()).await?,
        ));

        service.set_base_domain(hostname);
        service.set_auth(Box::new(AuthProvider::new(server_url).await?));

        Ok(S3Server {
            s3service: service,
            address: address.into(),
        })
    }

    pub async fn run(self) -> Result<()> {
        // Run server
        let listener = TcpListener::bind(&self.address)?;
        let server =
            Server::from_tcp(listener)?.serve(self.s3service.into_shared().into_make_service());

        info!("server is running at http(s)://{}/", self.address);
        Ok(tokio::spawn(server).await??)
    }
}
