use super::auth::AuthProvider;
use super::s3service::ArunaS3Service;
use crate::caching::cache;
use crate::data_backends::storage_backend::StorageBackend;
use anyhow::Result;
use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use hyper::service::Service;
use hyper::Server;
use s3s::service::S3Service;
use s3s::service::S3ServiceBuilder;
use s3s::service::SharedS3Service;
use s3s::Body;
use s3s::S3Error;
use std::convert::Infallible;
use std::future::ready;
use std::future::Ready;
use std::task::{Context, Poll};
use std::{net::TcpListener, sync::Arc};
use tracing::info;

pub struct S3Server {
    s3service: S3Service,
    address: String,
}

#[derive(Clone)]
pub struct WrappingService(SharedS3Service);

impl S3Server {
    pub async fn new(
        address: impl Into<String> + Copy,
        hostname: impl Into<String>,
        backend: Arc<Box<dyn StorageBackend>>,
        cache: Arc<cache::Cache>,
    ) -> Result<Self> {
        let s3service = ArunaS3Service::new(backend, cache.clone()).await?;

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
        let server = Server::from_tcp(listener)?
            .serve(WrappingService(self.s3service.into_shared()).into_make_service());

        info!("server is running at http(s)://{}/", self.address);
        Ok(tokio::spawn(server).await??)
    }
}

impl Service<hyper::Request<hyper::Body>> for WrappingService {
    type Response = hyper::Response<Body>;

    type Error = S3Error;

    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: hyper::Request<hyper::Body>) -> Self::Future {
        let mut service = self.0.clone();
        let resp = service.call(req);
        let res = resp.map(|r| {
            r.map(|mut r| {
                if r.headers().contains_key("Transfer-Encoding") {
                    r.headers_mut().remove("Content-Length");
                }
                r.map(Body::from)
            })
        });
        res.boxed()
    }
}

impl AsRef<S3Service> for WrappingService {
    fn as_ref(&self) -> &S3Service {
        &self.0.as_ref()
    }
}

impl WrappingService {
    #[must_use]
    pub fn into_make_service(self) -> MakeService<Self> {
        MakeService(self)
    }
}

#[derive(Clone)]
pub struct MakeService<S>(S);

impl<T, S: Clone> Service<T> for MakeService<S> {
    type Response = S;

    type Error = Infallible;

    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: T) -> Self::Future {
        ready(Ok(self.0.clone()))
    }
}
