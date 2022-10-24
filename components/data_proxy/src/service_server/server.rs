//! Server for the grpc service that expose the internal API components to create signed URLs

use std::{env, net::SocketAddr, sync::Arc, time::Duration};

use aruna_rust_api::api::internal::v1::{
    internal_proxy_service_server::{InternalProxyService, InternalProxyServiceServer},
    CreateBucketRequest, CreateBucketResponse, CreatePresignedDownloadRequest,
    CreatePresignedDownloadResponse, CreatePresignedUploadUrlRequest,
    CreatePresignedUploadUrlResponse, FinishPresignedUploadRequest, FinishPresignedUploadResponse,
    InitPresignedUploadRequest, InitPresignedUploadResponse, Location, LocationType,
};

use crate::{backends::storage_backend::StorageBackend, presign_handler::signer::PresignHandler};
use async_trait::async_trait;
use tonic::{Code, Response, Status};

/// Implements the API for the internal proxy that handles presigned URL generation to access and upload stored objects
#[derive(Debug, Clone)]
pub struct InternalServerImpl {
    pub data_client: Arc<Box<dyn StorageBackend>>,
    pub signer: Arc<PresignHandler>,
    pub data_proxy_hostname: String,
}

/// The gRPC Server to run the internal proxy api.
#[derive(Debug, Clone)]
pub struct ProxyServer {
    pub internal_api: Arc<InternalServerImpl>,
    pub addr: SocketAddr,
    pub data_proxy_hostname: String,
}

/// The actual implementation of the internal API
impl ProxyServer {
    pub async fn new(
        internal_api: Arc<InternalServerImpl>,
        addr: SocketAddr,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(ProxyServer {
            addr,
            data_proxy_hostname: internal_api.data_proxy_hostname.clone(),
            internal_api,
        })
    }

    pub async fn serve(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
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
        presign_handler: Arc<PresignHandler>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let proxy_data_host = match env::var("PROXY_DATA_HOST") {
            Ok(value) => value,
            Err(err) => {
                log::info!("{}", err);
                log::error!("Missing env var: PROXY_DATA_HOST env var required");
                return Err(Box::new(err));
            }
        };

        Ok(InternalServerImpl {
            data_client,
            data_proxy_hostname: proxy_data_host,
            signer: presign_handler,
        })
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
        request: tonic::Request<CreatePresignedUploadUrlRequest>,
    ) -> Result<tonic::Response<CreatePresignedUploadUrlResponse>, tonic::Status> {
        let inner_request = request.into_inner();
        let mut url = url::Url::parse(self.data_proxy_hostname.as_str()).unwrap();

        let location = inner_request.location.unwrap();

        let bucket = location.bucket;
        let key = location.path;
        let resource = if inner_request.multipart {
            let part = inner_request.part_number;
            format!("/objects/upload/multi/{part}/{bucket}/{key}")
        } else {
            format!("/objects/upload/single/{bucket}/{key}")
        };
        let duration = Duration::new(15 * 60, 0);
        url.set_path(resource.as_str());
        let signed_url = self
            .signer
            .sign_url(duration, Some(inner_request.upload_id), None, url)
            .unwrap();

        let response = CreatePresignedUploadUrlResponse {
            url: signed_url.to_string(),
        };

        return Ok(Response::new(response));
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
        request: tonic::Request<CreatePresignedDownloadRequest>,
    ) -> Result<tonic::Response<CreatePresignedDownloadResponse>, tonic::Status> {
        let inner_request = request.into_inner();

        let location = inner_request.location.unwrap();
        let path = format!("/objects/download/{}/{}", location.bucket, location.path);
        let duration = Duration::new(15 * 60, 0);

        let mut url = url::Url::parse(self.data_proxy_hostname.as_str()).unwrap();
        url.set_path(path.as_str());

        let signed_url = self
            .signer
            .sign_url(duration, None, Some(inner_request.filename), url)
            .unwrap();

        let url = signed_url.as_str();

        return Ok(Response::new(CreatePresignedDownloadResponse {
            url: url.to_string(),
        }));
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
}
