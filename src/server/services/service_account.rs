use aruna_rust_api::api::storage::services::v1::{
    service_account_service_server::ServiceAccountService, CreateServiceAccountRequest,
    CreateServiceAccountResponse, CreateServiceAccountTokenRequest,
    CreateServiceAccountTokenResponse, DeleteServiceAccountRequest, DeleteServiceAccountResponse,
    DeleteServiceAccountTokenRequest, DeleteServiceAccountTokenResponse,
    DeleteServiceAccountTokensRequest, DeleteServiceAccountTokensResponse,
    GetServiceAccountTokenRequest, GetServiceAccountTokenResponse, GetServiceAccountTokensRequest,
    GetServiceAccountTokensResponse, GetServiceAccountsByProjectRequest,
    GetServiceAccountsByProjectResponse, SetServiceAccountPermissionRequest,
    SetServiceAccountPermissionResponse,
};
use tokio::task;
use tonic::Response;

use super::authz::Authz;
use crate::{
    database::{connection::Database, models::enums::UserRights},
    error::ArunaError,
    server::services::utils::{format_grpc_request, format_grpc_response},
};
use std::{str::FromStr, sync::Arc};

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
        request: tonic::Request<CreateServiceAccountRequest>,
    ) -> Result<tonic::Response<CreateServiceAccountResponse>, tonic::Status> {
        log::info!("Received CreateServiceAccountRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Parse the project Uuid
        let parsed_project_id = diesel_ulid::DieselUlid::from_str(&request.get_ref().project_id)
            .map_err(ArunaError::from)?;

        // Authorize user
        let _admin_user = self
            .authz
            .project_authorize(
                request.metadata(),
                parsed_project_id,
                UserRights::ADMIN,
                false,
            )
            .await?;

        let database_clone = self.database.clone();
        let response = Response::new(
            task::spawn_blocking(move || {
                database_clone.create_service_account(request.into_inner())
            })
            .await
            .map_err(ArunaError::from)??,
        );

        log::info!("Sending CreateServiceAccountResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        return Ok(response);
    }

    /// CreateServiceAccountToken
    ///
    /// Creates a token for a service account
    /// Each service account can only have one permission -> The token will have the same permission as the
    /// service account
    async fn create_service_account_token(
        &self,
        request: tonic::Request<CreateServiceAccountTokenRequest>,
    ) -> Result<tonic::Response<CreateServiceAccountTokenResponse>, tonic::Status> {
        log::info!("Received CreateServiceAccountTokenRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Parse the project Uuid
        let parsed_project_id = diesel_ulid::DieselUlid::from_str(&request.get_ref().project_id)
            .map_err(ArunaError::from)?;

        // Authorize user
        let admin_user = self
            .authz
            .project_authorize(
                request.metadata(),
                parsed_project_id,
                UserRights::ADMIN,
                false,
            )
            .await?;

        let decoding_serial = self.authz.get_decoding_serial().await;
        let database_clone = self.database.clone();
        let response = Response::new(
            task::spawn_blocking(move || {
                database_clone.create_service_account_token(
                    request.into_inner(),
                    admin_user,
                    decoding_serial,
                )
            })
            .await
            .map_err(ArunaError::from)??,
        );

        log::info!("Sending CreateServiceAccountTokenResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        return Ok(response);
    }

    /// EditServiceAccountPermission
    ///
    /// Overwrites the project specific permissions for a service account
    async fn set_service_account_permission(
        &self,
        _request: tonic::Request<SetServiceAccountPermissionRequest>,
    ) -> Result<tonic::Response<SetServiceAccountPermissionResponse>, tonic::Status> {
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
