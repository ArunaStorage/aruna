use super::auth::AuthProvider;
use super::s3service::ArunaS3Service;
use crate::caching::cache;
use crate::data_backends::storage_backend::StorageBackend;
use anyhow::Result;
use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use http::Method;
use http::StatusCode;
use hyper::service::Service;
use hyper::Server;
use s3s::s3_error;
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
use tracing::error;
use tracing::info;
use tracing::info_span;
use tracing::Instrument;

pub struct S3Server {
    s3service: S3Service,
    address: String,
}

#[derive(Clone)]
pub struct WrappingService(SharedS3Service);

impl S3Server {
    #[tracing::instrument(level = "trace", skip(address, hostname, backend, cache))]
    pub async fn new(
        address: impl Into<String> + Copy,
        hostname: impl Into<String>,
        backend: Arc<Box<dyn StorageBackend>>,
        cache: Arc<cache::Cache>,
    ) -> Result<Self> {
        let s3service = ArunaS3Service::new(backend, cache.clone())
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                tonic::Status::unauthenticated(e.to_string())
            })?;

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
    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn run(self) -> Result<()> {
        // Run server
        let listener = TcpListener::bind(&self.address).map_err(|e| {
            error!(error = ?e, msg = e.to_string());
            tonic::Status::unauthenticated(e.to_string())
        })?;
        let server = Server::from_tcp(listener)
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                tonic::Status::unauthenticated(e.to_string())
            })?
            .serve(WrappingService(self.s3service.into_shared()).into_make_service());
        info!("server is running at http(s)://{}/", self.address);
        Ok(tokio::spawn(server)
            .instrument(info_span!("s3_server_run"))
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                tonic::Status::unauthenticated(e.to_string())
            })?
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                tonic::Status::unauthenticated(e.to_string())
            })?)
    }
}

impl Service<hyper::Request<hyper::Body>> for WrappingService {
    type Response = hyper::Response<Body>;

    type Error = S3Error;

    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    #[tracing::instrument(level = "trace", skip(self))]
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    #[tracing::instrument(level = "trace", skip(self, req))]
    fn call(&mut self, req: hyper::Request<hyper::Body>) -> Self::Future {
        // Catch OPTIONS requests
        if req.method() == Method::OPTIONS {
            let resp = Box::pin(async {
                hyper::Response::builder()
                    .header("Access-Control-Allow-Origin", "*")
                    .header("Access-Control-Allow-Methods", "*")
                    .header("Access-Control-Allow-Headers", "*")
                    .body(s3s::Body::empty())
                    .map_err(|_| s3_error!(InvalidRequest, "Invalid OPTIONS request"))
            });

            return resp;
        }

        let mut service = self.0.clone();
        let resp = service.call(req);
        let res = resp.map(|r| {
            r.map(|mut r| {
                if r.headers().contains_key("Transfer-Encoding") {
                    r.headers_mut().remove("Content-Length");
                }

                // Workaround to return 206 (Partial Content) for range responses
                if r.headers().contains_key("Content-Range")
                    && r.headers().contains_key("Accept-Ranges")
                    && r.status().as_u16() == 200
                {
                    let status = r.status_mut();
                    *status = StatusCode::from_u16(206).unwrap();
                }

                r.map(Body::from)
            })
        });
        res.boxed()
    }
}

impl AsRef<S3Service> for WrappingService {
    #[tracing::instrument(level = "trace", skip(self))]
    fn as_ref(&self) -> &S3Service {
        self.0.as_ref()
    }
}

impl WrappingService {
    #[tracing::instrument(level = "trace", skip(self))]
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

    #[tracing::instrument(level = "trace", skip(self))]
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn call(&mut self, _: T) -> Self::Future {
        ready(Ok(self.0.clone()))
    }
}
