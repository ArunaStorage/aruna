use crate::auth::permission_handler::PermissionHandler;
use crate::caching::cache::Cache;
use crate::middlelayer::db_handler::DatabaseHandler;
use aruna_rust_api::api::storage::services::v2::{
    service_account_service_server::ServiceAccountService, CreateDataproxyTokenSvcAccountRequest,
    CreateDataproxyTokenSvcAccountResponse, CreateServiceAccountRequest,
    CreateServiceAccountResponse, CreateServiceAccountTokenRequest,
    CreateServiceAccountTokenResponse, DeleteServiceAccountRequest, DeleteServiceAccountResponse,
    DeleteServiceAccountTokenRequest, DeleteServiceAccountTokenResponse,
    DeleteServiceAccountTokensRequest, DeleteServiceAccountTokensResponse,
    GetS3CredentialsSvcAccountRequest, GetS3CredentialsSvcAccountResponse,
    GetServiceAccountTokenRequest, GetServiceAccountTokenResponse, GetServiceAccountTokensRequest,
    GetServiceAccountTokensResponse, SetServiceAccountPermissionRequest,
    SetServiceAccountPermissionResponse,
};
use std::sync::Arc;
use tonic::{Request, Response, Result};

crate::impl_grpc_server!(ServiceAccountServiceImpl);

#[tonic::async_trait]
impl ServiceAccountService for ServiceAccountServiceImpl {
    async fn create_service_account(
        &self,
        _request: Request<CreateServiceAccountRequest>,
    ) -> Result<Response<CreateServiceAccountResponse>> {
        todo!()
    }
    async fn create_service_account_token(
        &self,
        _request: Request<CreateServiceAccountTokenRequest>,
    ) -> Result<Response<CreateServiceAccountTokenResponse>> {
        todo!()
    }
    async fn set_service_account_permission(
        &self,
        _request: Request<SetServiceAccountPermissionRequest>,
    ) -> Result<Response<SetServiceAccountPermissionResponse>> {
        todo!()
    }
    async fn get_service_account_token(
        &self,
        _request: Request<GetServiceAccountTokenRequest>,
    ) -> Result<Response<GetServiceAccountTokenResponse>> {
        todo!()
    }
    async fn get_service_account_tokens(
        &self,
        _request: Request<GetServiceAccountTokensRequest>,
    ) -> Result<Response<GetServiceAccountTokensResponse>> {
        todo!()
    }
    async fn delete_service_account_token(
        &self,
        _request: Request<DeleteServiceAccountTokenRequest>,
    ) -> Result<Response<DeleteServiceAccountTokenResponse>> {
        todo!()
    }
    async fn delete_service_account_tokens(
        &self,
        _request: Request<DeleteServiceAccountTokensRequest>,
    ) -> Result<Response<DeleteServiceAccountTokensResponse>> {
        todo!()
    }
    async fn delete_service_account(
        &self,
        _request: Request<DeleteServiceAccountRequest>,
    ) -> Result<Response<DeleteServiceAccountResponse>> {
        todo!()
    }
    async fn get_s3_credentials_svc_account(
        &self,
        _request: Request<GetS3CredentialsSvcAccountRequest>,
    ) -> Result<Response<GetS3CredentialsSvcAccountResponse>> {
        todo!()
    }
    async fn create_dataproxy_token_svc_account(
        &self,
        _request: Request<CreateDataproxyTokenSvcAccountRequest>,
    ) -> Result<Response<CreateDataproxyTokenSvcAccountResponse>> {
        todo!()
    }
}
