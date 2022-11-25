use aruna_rust_api::api::storage::services::v1::{
    service_account_service_server::ServiceAccountService, CreateServiceAccountRequest,
    CreateServiceAccountResponse, CreateServiceAccountTokenRequest,
    CreateServiceAccountTokenResponse, DeleteServiceAccountRequest, DeleteServiceAccountResponse,
    DeleteServiceAccountTokenRequest, DeleteServiceAccountTokenResponse,
    DeleteServiceAccountTokensRequest, DeleteServiceAccountTokensResponse,
    EditServiceAccountPermissionRequest, EditServiceAccountPermissionResponse,
    GetServiceAccountTokenRequest, GetServiceAccountTokenResponse, GetServiceAccountTokensRequest,
    GetServiceAccountTokensResponse, GetServiceAccountsByProjectRequest,
    GetServiceAccountsByProjectResponse,
};

use super::authz::Authz;
use crate::database::connection::Database;
use std::sync::Arc;

// This macro automatically creates the Impl struct with all associated fields
crate::impl_grpc_server!(ServiceAccountServiceImpl);

#[tonic::async_trait]
impl ServiceAccountService for ServiceAccountServiceImpl {
    /// CreateServiceAccount
    ///
    /// Creates a service account for a given project
    /// If the service account has permissions for the global Admin project
    /// it will be a global service account that can interact with any resource
    async fn create_service_account(
        &self,
        _request: tonic::Request<CreateServiceAccountRequest>,
    ) -> Result<tonic::Response<CreateServiceAccountResponse>, tonic::Status> {
        todo!()
    }
    /// CreateServiceAccountToken
    ///
    /// Creates a token for a service account
    /// Each service account can only have one permission -> The token will have the same permission as the
    /// service account
    async fn create_service_account_token(
        &self,
        _request: tonic::Request<CreateServiceAccountTokenRequest>,
    ) -> Result<tonic::Response<CreateServiceAccountTokenResponse>, tonic::Status> {
        todo!()
    }
    /// EditServiceAccountPermission
    ///
    /// Overwrites the project specific permissions for a service account
    async fn edit_service_account_permission(
        &self,
        _request: tonic::Request<EditServiceAccountPermissionRequest>,
    ) -> Result<tonic::Response<EditServiceAccountPermissionResponse>, tonic::Status> {
        todo!()
    }
    /// GetServiceAccountToken
    ///
    /// This requests the overall information about a specifc service account token (by id)
    /// it will not contain the token itself.
    async fn get_service_account_token(
        &self,
        _request: tonic::Request<GetServiceAccountTokenRequest>,
    ) -> Result<tonic::Response<GetServiceAccountTokenResponse>, tonic::Status> {
        todo!()
    }
    /// GetServiceAccountTokens
    ///
    /// This requests the overall information about all service account tokens
    /// it will not contain the token itself.
    async fn get_service_account_tokens(
        &self,
        _request: tonic::Request<GetServiceAccountTokensRequest>,
    ) -> Result<tonic::Response<GetServiceAccountTokensResponse>, tonic::Status> {
        todo!()
    }
    /// GetServiceAccountsByProject
    ///
    /// Will request all service_accounts for a given project
    /// each service account is bound to a specific project
    async fn get_service_accounts_by_project(
        &self,
        _request: tonic::Request<GetServiceAccountsByProjectRequest>,
    ) -> Result<tonic::Response<GetServiceAccountsByProjectResponse>, tonic::Status> {
        todo!()
    }
    /// DeleteServiceAccountToken
    ///
    /// Deletes one service account token by ID
    async fn delete_service_account_token(
        &self,
        _request: tonic::Request<DeleteServiceAccountTokenRequest>,
    ) -> Result<tonic::Response<DeleteServiceAccountTokenResponse>, tonic::Status> {
        todo!()
    }
    /// DeleteServiceAccountTokens
    ///
    /// Deletes all service account tokens
    async fn delete_service_account_tokens(
        &self,
        _request: tonic::Request<DeleteServiceAccountTokensRequest>,
    ) -> Result<tonic::Response<DeleteServiceAccountTokensResponse>, tonic::Status> {
        todo!()
    }
    /// DeleteServiceAccount
    ///
    /// Deletes a service account (by id)
    async fn delete_service_account(
        &self,
        _request: tonic::Request<DeleteServiceAccountRequest>,
    ) -> Result<tonic::Response<DeleteServiceAccountResponse>, tonic::Status> {
        todo!()
    }
}
