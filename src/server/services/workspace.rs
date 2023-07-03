use crate::database::crud::utils::naivedatetime_to_prost_time;
use crate::error::ArunaError;
use crate::server::services::authz::Authz;
use crate::server::services::utils::format_grpc_response;
use crate::{database::connection::Database, server::services::utils::format_grpc_request};
use aruna_rust_api::api::storage::services::v1::{
    workspace_service_server::WorkspaceService, ClaimWorkspaceRequest, ClaimWorkspaceResponse,
    CreateWorkspaceRequest, CreateWorkspaceResponse, DeleteWorkspaceRequest,
    DeleteWorkspaceResponse, MoveWorkspaceDataRequest, MoveWorkspaceDataResponse,
};
use chrono::Months;
use std::sync::Arc;
use tokio::task;

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
        _request: tonic::Request<DeleteWorkspaceRequest>,
    ) -> std::result::Result<tonic::Response<DeleteWorkspaceResponse>, tonic::Status> {
        todo!()
    }
    /// DeleteWorkspace
    ///
    /// Status: ALPHA
    ///
    /// Claims an anonymous workspace, and transfers the owner to a regular user account.
    async fn claim_workspace(
        &self,
        _request: tonic::Request<ClaimWorkspaceRequest>,
    ) -> std::result::Result<tonic::Response<ClaimWorkspaceResponse>, tonic::Status> {
        todo!()
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
