use crate::auth::structs::Context;
use crate::caching::cache::Cache;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::service_account_request_types::{
    CreateServiceAccount, CreateServiceAccountToken,
};
use crate::{auth::permission_handler::PermissionHandler, utils::conversions::get_token_from_md};
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
        request: Request<CreateServiceAccountRequest>,
    ) -> Result<Response<CreateServiceAccountResponse>> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = CreateServiceAccount(request.into_inner());
        let (id, perm) = tonic_invalid!(request.get_permissions(), "Invalid request");
        let ctx = Context::res_ctx(id, perm.into_inner(), false);
        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );
        let service_account = tonic_internal!(
            self.database_handler.create_service_account(request).await,
            "Internal create service account error"
        );
        let response = CreateServiceAccountResponse {
            service_account: Some(tonic_internal!(
                service_account.try_into(),
                "User conversion error"
            )),
        };
        return_with_log!(response);
    }
    async fn create_service_account_token(
        &self,
        request: Request<CreateServiceAccountTokenRequest>,
    ) -> Result<Response<CreateServiceAccountTokenResponse>> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = CreateServiceAccountToken(request.into_inner());
        let (id, perm) = tonic_invalid!(request.get_permissions(), "Invalid permissions provided");
        let ctx = Context::res_ctx(id, perm.into_inner(), false);
        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );
        let (token, token_secret) = tonic_internal!(
            self.database_handler
                .create_service_account_token(self.authorizer.clone(), request)
                .await,
            "Internal create service account error"
        );
        let response = CreateServiceAccountTokenResponse {
            token,
            token_secret,
        };
        return_with_log!(response);
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
