//! Server for the grpc service that expose the internal API components to create signed URLs

use std::{env, net::SocketAddr, sync::Arc, time::Duration};

use async_trait::async_trait;
use tonic::Response;

use crate::{
    api::aruna::api::internal::proxy::v1::{
        internal_proxy_service_server::{InternalProxyService, InternalProxyServiceServer},
        CreatePresignedDownloadRequest, CreatePresignedDownloadResponse,
        CreatePresignedUploadUrlRequest, CreatePresignedUploadUrlResponse,
        FinishPresignedUploadRequest, FinishPresignedUploadResponse, InitPresignedUploadRequest,
        InitPresignedUploadResponse,
    },
    presign_handler::signer::PresignHandler,
    storage_backend::s3_backend::S3Backend,
};

/// Implements the API for the internal proxy that handles presigned URL generation to access and upload stored objects
#[derive(Debug, Clone)]
pub struct InternalServerImpl {
    pub data_client: Arc<S3Backend>,
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

impl ProxyServer {
    pub async fn new(
        internal_api: Arc<InternalServerImpl>,
        addr: SocketAddr,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        return Ok(ProxyServer {
            addr: addr,
            data_proxy_hostname: internal_api.data_proxy_hostname.clone(),
            internal_api: internal_api,
        });
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
        data_client: Arc<S3Backend>,
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
            data_client: data_client,
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

        if !inner_request.multipart {
            return Ok(tonic::Response::new(InitPresignedUploadResponse {
                upload_id: "".to_string(),
            }));
        }

        let upload_id = self
            .data_client
            .init_multipart_upload(inner_request.location.unwrap())
            .await
            .unwrap();

        return Ok(Response::new(InitPresignedUploadResponse {
            upload_id: upload_id,
        }));
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
        let resource = format!("/objects/upload/single/{bucket}/{key}");
        let duration = Duration::new(15 * 60, 0);
        url.set_path(resource.as_str());
        let signed_url = self.signer.get_sign_url(resource, duration).unwrap();

        let response = CreatePresignedUploadUrlResponse {
            url: signed_url.to_string(),
        };

        return Ok(Response::new(response));
    }
    async fn finish_presigned_upload(
        &self,
        request: tonic::Request<FinishPresignedUploadRequest>,
    ) -> Result<tonic::Response<FinishPresignedUploadResponse>, tonic::Status> {
        let _inner_request = request.into_inner();

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

        let signed_url = self.signer.get_sign_url(path, duration).unwrap();

        let url = signed_url.as_str();

        return Ok(Response::new(CreatePresignedDownloadResponse {
            url: url.to_string(),
        }));
    }
}
