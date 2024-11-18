use std::sync::Arc;

use s3s::{host::MultiDomain, service::{S3Service, S3ServiceBuilder}};
use tokio::net::TcpListener;

use crate::{config::Config, error::ProxyError, lmdbstore::LmdbStore};

use super::{access::AccessChecker, service::ArunaS3Service};

pub struct Server {
    service: S3Service,
    config: Arc<Config>,
}

impl Server {
    pub fn new(storage: Arc<LmdbStore>, config: Arc<Config>) -> Result<Self, ProxyError> {

        let aruna_s3_service = ArunaS3Service::new(storage, config.clone());

        let service = {
            let mut builder = S3ServiceBuilder::new(aruna_s3_service);

            builder.set_access(AccessChecker {});
            builder.set_host(MultiDomain::new(&[config.frontend.hostname.clone()])?);
            builder.build()
        };

        Ok(Server {
            service,
            config,
        })
    }


    async fn run(&self) -> Result<(), ProxyError> {


        let listener = TcpListener::bind((self.config.frontend.host.as_str(), self.config.port)).await?;



        self.service.run().await?;
        Ok(())
    }
}
