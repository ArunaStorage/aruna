use anyhow::Result;
use hyper::Server;
use s3s::service::S3Service;
use std::net::TcpListener;
use tokio::signal::unix::{signal, SignalKind};
use tracing::info;

use super::{auth::AuthProvider, s3service::S3ServiceServer};

pub struct S3Server {
    s3service: S3Service,
    address: String,
}

impl S3Server {
    pub async fn new(
        address: impl Into<String> + Copy,
        aruna_server: impl Into<String>,
    ) -> Result<Self> {
        let mut service = S3Service::new(Box::new(S3ServiceServer::new().await));

        service.set_base_domain(address);
        service.set_auth(Box::new(AuthProvider::new(aruna_server).await));

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
        let task = tokio::spawn(server);

        let mut stream = signal(SignalKind::terminate())?;

        loop {
            stream.recv().await;
            task.abort();
            break;
        }

        Ok(())
    }
}
