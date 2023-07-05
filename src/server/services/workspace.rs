use crate::database::crud::utils::naivedatetime_to_prost_time;
use crate::database::models::enums::UserRights;
use crate::error::{ArunaError, TypeConversionError};
use crate::server::services::authz::Authz;
use crate::server::services::utils::format_grpc_response;
use crate::{database::connection::Database, server::services::utils::format_grpc_request};
use aruna_rust_api::api::storage::services::v1::{
    workspace_service_server::WorkspaceService, ClaimWorkspaceRequest, ClaimWorkspaceResponse,
    CreateWorkspaceRequest, CreateWorkspaceResponse, DeleteWorkspaceRequest,
    DeleteWorkspaceResponse, MoveWorkspaceDataRequest, MoveWorkspaceDataResponse,
};
use chrono::Months;
use std::str::FromStr;
use std::sync::Arc;
use tokio::task;
use tonic::metadata::MetadataValue;

// This macro automatically creates the Impl struct with all associated fields
crate::impl_grpc_server!(WorkspaceServiceImpl);

#[tonic::async_trait]
impl WorkspaceService for WorkspaceServiceImpl {
    /// CreateWorkspace
    ///
    /// Status: ALPHA
    ///
    /// A new request to create a personal anonymous workspace
    async fn create_workspace(
        &self,
        request: tonic::Request<CreateWorkspaceRequest>,
    ) -> std::result::Result<tonic::Response<CreateWorkspaceResponse>, tonic::Status> {
        log::info!("Received CreateWorkspaceRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // TODO: RATE LIMITING

        // Authorize collection - READ
        let database_clone = self.database.clone();
        let decoding_serial = self.authz.get_decoding_serial().await;

        let mut resp = task::spawn_blocking(move || {
            database_clone.create_workspace(request.into_inner(), decoding_serial)
        })
        .await
        .map_err(ArunaError::from)??;

        resp.token = self
            .authz
            .sign_new_token(
                &resp.access_key,
                naivedatetime_to_prost_time(
                    chrono::Utc::now()
                        .naive_utc()
                        .checked_add_months(Months::new(1))
                        .ok_or_else(|| {
                            ArunaError::InvalidRequest("Unable to parse time".to_string())
                        })?,
                )
                .ok(),
            )
            .await?;

        let response = tonic::Response::new(resp);
        log::info!("Sending CreateWorkspaceResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }
    /// DeleteWorkspace
    ///
    /// Status: ALPHA
    ///
    /// Delete a workspace
    async fn delete_workspace(
        &self,
        request: tonic::Request<DeleteWorkspaceRequest>,
    ) -> std::result::Result<tonic::Response<DeleteWorkspaceResponse>, tonic::Status> {
        log::info!("Received DeleteWorkspaceRequest.");
        log::debug!("{}", format_grpc_request(&request));

        let user_id = self
            .authz
            .collection_authorize(
                request.metadata(),
                diesel_ulid::DieselUlid::from_str(&request.get_ref().workspace_id)
                    .map_err(|_| ArunaError::TypeConversionError(TypeConversionError::UUID))?,
                UserRights::WRITE,
            )
            .await?;

        // Authorize collection - READ
        let database_clone = self.database.clone();

        let resp = task::spawn_blocking(move || {
            database_clone.delete_workspace(request.into_inner(), user_id)
        })
        .await
        .map_err(ArunaError::from)??;

        let response = tonic::Response::new(resp);
        log::info!("Sending DeleteWorkspaceResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }
    /// ClaimWorkspace
    ///
    /// Status: ALPHA
    ///
    /// Claims an anonymous workspace, and transfers ownership to a regular user account.
    async fn claim_workspace(
        &self,
        request: tonic::Request<ClaimWorkspaceRequest>,
    ) -> std::result::Result<tonic::Response<ClaimWorkspaceResponse>, tonic::Status> {
        log::info!("Received ClaimWorkspaceRequest.");
        log::debug!("{}", format_grpc_request(&request));

        let metadata = request.metadata_mut();
        let old_value = metadata
            .insert(
                "Authorization",
                MetadataValue::try_from(request.get_ref().token)
                    .map_err(|e| tonic::Status::invalid_argument("Unable to utilize token"))?,
            )
            .ok_or_else(|| tonic::Status::invalid_argument("Unable to utilize token"))?;

        let token_id = self
            .authz
            .validate_and_query_token(
                old_value
                    .to_str()
                    .map_err(|e| tonic::Status::invalid_argument("Unable to utilize token"))?,
            )
            .await?;

        let user_id = self
            .authz
            .collection_authorize(
                request.metadata(),
                diesel_ulid::DieselUlid::from_str(&request.get_ref().workspace_id)
                    .map_err(|_| ArunaError::TypeConversionError(TypeConversionError::UUID))?,
                UserRights::WRITE,
            )
            .await?;

        // Authorize collection - READ
        let database_clone = self.database.clone();

        let resp = task::spawn_blocking(move || {
            database_clone.claim_workspace(request.into_inner(), token_id, user_id)
        })
        .await
        .map_err(ArunaError::from)??;

        let response = tonic::Response::new(resp);
        log::info!("Sending ClaimWorkspaceResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }
    /// MoveWorkspaceData
    ///
    /// Status: ALPHA
    ///
    /// Claims an anonymous workspace
    async fn move_workspace_data(
        &self,
        _request: tonic::Request<MoveWorkspaceDataRequest>,
    ) -> std::result::Result<tonic::Response<MoveWorkspaceDataResponse>, tonic::Status> {
        todo!()
    }
}
