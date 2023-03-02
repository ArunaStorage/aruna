//! Server for the grpc service that expose the internal API components to create signed URLs

use anyhow::Result;
use std::{env, net::SocketAddr, sync::Arc};

use aruna_rust_api::api::internal::v1::{
    internal_proxy_service_server::{InternalProxyService, InternalProxyServiceServer},
    CreateBucketRequest, CreateBucketResponse, CreatePresignedDownloadRequest,
    CreatePresignedDownloadResponse, CreatePresignedUploadUrlRequest,
    CreatePresignedUploadUrlResponse, DeleteObjectRequest, DeleteObjectResponse,
    FinishPresignedUploadRequest, FinishPresignedUploadResponse, InitPresignedUploadRequest,
    InitPresignedUploadResponse, Location, LocationType, MoveObjectRequest, MoveObjectResponse,
};

use crate::backends::storage_backend::StorageBackend;
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
    pub addr: SocketAddr,
}

/// The actual implementation of the internal API
impl ProxyServer {
    pub async fn new(
        internal_api: Arc<InternalServerImpl>,
        addr: SocketAddr,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(ProxyServer { addr, internal_api })
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
    async fn init_presigned_upload(
        &self,
        request: tonic::Request<InitPresignedUploadRequest>,
    ) -> Result<tonic::Response<InitPresignedUploadResponse>, tonic::Status> {
        let inner_request = request.into_inner();

        let upload_id = uuid::Uuid::new_v4();

        if !inner_request.multipart {
            return Ok(tonic::Response::new(InitPresignedUploadResponse {
                upload_id: upload_id.to_string(),
            }));
        }

        let upload_id = self
            .data_client
            .init_multipart_upload(inner_request.location.unwrap())
            .await
            .unwrap();

        return Ok(Response::new(InitPresignedUploadResponse { upload_id }));
    }

    async fn create_presigned_upload_url(
        &self,
        _request: tonic::Request<CreatePresignedUploadUrlRequest>,
    ) -> Result<tonic::Response<CreatePresignedUploadUrlResponse>, tonic::Status> {
        todo!()
    }

    async fn finish_presigned_upload(
        &self,
        request: tonic::Request<FinishPresignedUploadRequest>,
    ) -> Result<tonic::Response<FinishPresignedUploadResponse>, tonic::Status> {
        let inner_request = request.into_inner();

        let location = Location {
            bucket: inner_request.bucket,
            path: inner_request.key,
            r#type: LocationType::Unspecified as i32,
        };

        match self
            .data_client
            .finish_multipart_upload(location, inner_request.part_etags, inner_request.upload_id)
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

        let response = FinishPresignedUploadResponse { ok: true };
        return Ok(Response::new(response));
    }

    async fn create_presigned_download(
        &self,
        _request: tonic::Request<CreatePresignedDownloadRequest>,
    ) -> Result<tonic::Response<CreatePresignedDownloadResponse>, tonic::Status> {
        todo!()
    }

    async fn create_bucket(
        &self,
        request: tonic::Request<CreateBucketRequest>,
    ) -> Result<tonic::Response<CreateBucketResponse>, tonic::Status> {
        let inner_request = request.into_inner();
        match self
            .data_client
            .create_bucket(inner_request.bucket_name)
            .await
        {
            Ok(_) => {}
            Err(_) => {
                return Err(Status::new(Code::Internal, "could not create bucket"));
            }
        }

        return Ok(Response::new(CreateBucketResponse {}));
    }

    async fn delete_object(
        &self,
        _request: tonic::Request<DeleteObjectRequest>,
    ) -> Result<tonic::Response<DeleteObjectResponse>, tonic::Status> {
        todo!()
    }
    async fn move_object(
        &self,
        _request: tonic::Request<MoveObjectRequest>,
    ) -> Result<tonic::Response<MoveObjectResponse>, tonic::Status> {
        todo!()
    }
}
