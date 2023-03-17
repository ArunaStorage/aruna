//! Server for the grpc service that expose the internal API components to create signed URLs

use anyhow::Result;
use std::{env, net::SocketAddr, sync::Arc};

use aruna_rust_api::api::internal::v1::{
    internal_proxy_service_server::{InternalProxyService, InternalProxyServiceServer},
    DeleteObjectRequest, DeleteObjectResponse, FinishMultipartUploadRequest,
    FinishMultipartUploadResponse, InitMultipartUploadRequest, InitMultipartUploadResponse,
    Location, LocationType,
};

use crate::{backends::storage_backend::StorageBackend, data_server::data_handler::DataHandler};
use async_trait::async_trait;
use tonic::{Code, Response, Status};

/// Implements the API for the internal proxy that handles presigned URL generation to access and upload stored objects
#[derive(Debug, Clone)]
pub struct InternalServerImpl {
    pub data_client: Arc<Box<dyn StorageBackend>>,
}

/// The gRPC Server to run the internal proxy api.
#[derive(Debug, Clone)]
pub struct ProxyServer {
    pub internal_api: Arc<InternalServerImpl>,
    pub data_handler: Arc<DataHandler>,
    pub addr: SocketAddr,
}

/// The actual implementation of the internal API
impl ProxyServer {
    pub async fn new(
        internal_api: Arc<InternalServerImpl>,
        data_handler: Arc<DataHandler>,
        addr: SocketAddr,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(ProxyServer {
            addr,
            data_handler,
            internal_api,
        })
    }

    pub async fn serve(&self) -> Result<()> {
        let internal_proxy_service =
            InternalProxyServiceServer::from_arc(self.internal_api.clone());

        tonic::transport::Server::builder()
            .add_service(internal_proxy_service)
            .serve(self.addr)
            .await?;

        Ok(())
    }
}

impl InternalServerImpl {
    pub async fn new(
        data_client: Arc<Box<dyn StorageBackend>>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let _proxy_data_host = match env::var("PROXY_DATA_HOST") {
            Ok(value) => value,
            Err(err) => {
                log::info!("{}", err);
                log::error!("Missing env var: PROXY_DATA_HOST env var required");
                return Err(Box::new(err));
            }
        };

        Ok(InternalServerImpl { data_client })
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
            .init_multipart_upload(location_from_path(inner_request.path)?)
            .await
            .unwrap();

        return Ok(Response::new(InitMultipartUploadResponse { upload_id }));
    }

    async fn finish_multipart_upload(
        &self,
        request: tonic::Request<FinishMultipartUploadRequest>,
    ) -> Result<tonic::Response<FinishMultipartUploadResponse>, tonic::Status> {
        let inner_request = request.into_inner();

        match self
            .data_client
            .finish_multipart_upload(
                location_from_path(inner_request.path)?,
                inner_request.part_etags,
                inner_request.upload_id,
            )
            .await
        {
            Ok(_) => {}
            Err(err) => {
                return Err(Status::new(
                    Code::Internal,
                    format!("could not create new bucket: {}", err),
                ));
            }
        }

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

fn location_from_path(path: String) -> Result<Location, tonic::Status> {
    let splits = path.split('.').collect::<Vec<&str>>();

    if splits.len() == 3 {
        Ok(Location {
            r#type: LocationType::Unspecified as i32,
            bucket: format!("{}.{}", splits[0], splits[1]),
            path: format!("{}", splits[2]),
            endpoint_id: "".to_string(),
            is_compressed: false,
            is_encrypted: false,
            encryption_key: "".to_string(),
        })
    } else if splits.len() == 5 {
        Ok(Location {
            r#type: LocationType::Unspecified as i32,
            bucket: format!("{}.{}.{}.{}", splits[0], splits[1], splits[2], splits[3]),
            path: format!("{}", splits[4]),
            endpoint_id: "".to_string(),
            is_compressed: false,
            is_encrypted: false,
            encryption_key: "".to_string(),
        })
    } else {
        Err(tonic::Status::invalid_argument("Unable to parse location"))
    }
}
