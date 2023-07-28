//! Server for the grpc service that expose the internal API components to create signed URLs
use crate::{
    bundler::internal_bundler_service::InternalBundlerServiceImpl,
    data_backends::storage_backend::StorageBackend, data_server::data_handler::DataHandler,
};
use anyhow::{bail, Result};
use async_trait::async_trait;
use std::{env, net::SocketAddr, sync::Arc};
use tonic::Response;

/// Implements the API for the internal proxy that handles presigned URL generation to access and upload stored objects
#[derive(Debug, Clone)]
pub struct InternalServerImpl {
    pub data_client: Arc<Box<dyn StorageBackend>>,
    pub data_handler: Arc<DataHandler>,
}

/// The gRPC Server to run the internal proxy api.
#[derive(Debug, Clone)]
pub struct ProxyServer {
    pub bundler: Option<Arc<InternalBundlerServiceImpl>>,
    pub internal_api: Arc<InternalServerImpl>,
    pub addr: SocketAddr,
}

/// The actual implementation of the internal API
impl ProxyServer {
    pub async fn new(
        bundler: Option<Arc<InternalBundlerServiceImpl>>,
        internal_api: Arc<InternalServerImpl>,
        addr: SocketAddr,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(ProxyServer {
            addr,
            internal_api,
            bundler,
        })
    }

    pub async fn serve(&self) -> Result<()> {
        let internal_proxy_service =
            InternalProxyServiceServer::from_arc(self.internal_api.clone());

        let bundler = self
            .bundler
            .clone()
            .map(|b| InternalBundlerServiceServer::from_arc(b));

        tonic::transport::Server::builder()
            .add_service(internal_proxy_service)
            .add_optional_service(bundler)
            .serve(self.addr)
            .await?;

        Ok(())
    }
}

impl InternalServerImpl {
    pub async fn new(
        data_client: Arc<Box<dyn StorageBackend>>,
        data_handler: Arc<DataHandler>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let _proxy_data_host = match env::var("PROXY_DATA_HOST") {
            Ok(value) => value,
            Err(err) => {
                log::info!("{}", err);
                log::error!("Missing env var: PROXY_DATA_HOST env var required");
                return Err(Box::new(err));
            }
        };

        Ok(InternalServerImpl {
            data_client,
            data_handler,
        })
    }
}

#[async_trait]
impl InternalProxyService for InternalServerImpl {
    async fn init_multipart_upload(
        &self,
        request: tonic::Request<InitMultipartUploadRequest>,
    ) -> Result<tonic::Response<InitMultipartUploadResponse>, tonic::Status> {
        let inner_request = request.into_inner();

        let upload_id = self
            .data_client
            .init_multipart_upload(Location {
                bucket: format!(
                    "{}-temp",
                    &self.data_handler.settings.endpoint_id.to_string()
                ),
                path: format!(
                    "{}/{}",
                    inner_request.collection_id, inner_request.object_id
                ),
                ..Default::default()
            })
            .await
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;

        return Ok(Response::new(InitMultipartUploadResponse { upload_id }));
    }

    async fn finish_multipart_upload(
        &self,
        request: tonic::Request<FinishMultipartUploadRequest>,
    ) -> Result<tonic::Response<FinishMultipartUploadResponse>, tonic::Status> {
        let inner_request = request.into_inner();

        self.data_handler
            .clone()
            .finish_multipart(
                inner_request.part_etags,
                inner_request.object_id,
                inner_request.collection_id,
                inner_request.upload_id,
                inner_request.path,
            )
            .await
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
        let response = FinishMultipartUploadResponse {};
        return Ok(Response::new(response));
    }

    async fn delete_object(
        &self,
        _request: tonic::Request<DeleteObjectRequest>,
    ) -> Result<tonic::Response<DeleteObjectResponse>, tonic::Status> {
        todo!()
    }
}

fn _location_from_path(path: String) -> Result<(String, String)> {
    let splits = path
        .split('/')
        .map(|e| e.to_string())
        .collect::<Vec<String>>();

    if splits.len() != 2 {
        bail!("Invalid path parts (expected collection/object_id)")
    } else {
        Ok((splits[0].to_string(), splits[1].to_string()))
    }
}
