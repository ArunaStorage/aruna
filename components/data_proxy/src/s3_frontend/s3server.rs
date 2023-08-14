use super::auth::AuthProvider;
use super::s3service::ArunaS3Service;
use crate::caching::cache;
use crate::data_backends::storage_backend::StorageBackend;
use anyhow::Result;
use hyper::Server;
use s3s::service::S3Service;
use s3s::service::S3ServiceBuilder;
use std::{net::TcpListener, sync::Arc};
use tracing::info;

pub struct S3Server {
    s3service: S3Service,
    address: String,
}

impl S3Server {
    pub async fn new(
        address: impl Into<String> + Copy,
        hostname: impl Into<String>,
        backend: Arc<Box<dyn StorageBackend>>,
        endpoint_id: impl Into<String>,
        cache: Arc<cache::Cache>,
    ) -> Result<Self> {
        let s3service = ArunaS3Service::new(backend, endpoint_id.into()).await?;

        let service = {
            let mut b = S3ServiceBuilder::new(s3service);
            b.set_base_domain(hostname);
            b.set_auth(AuthProvider::new(cache).await);
            b.build()
        };

        Ok(Self {
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
