use crate::auth::structs::Context;
use crate::caching::cache::Cache;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::workspace_request_types::{CreateTemplate, CreateWorkspace};
use crate::{auth::permission_handler::PermissionHandler, utils::conversions::get_token_from_md};
use aruna_rust_api::api::storage::services::v2::{
    workspace_service_server::WorkspaceService, ClaimWorkspaceRequest, ClaimWorkspaceResponse,
    CreateWorkspaceRequest, CreateWorkspaceResponse, CreateWorkspaceTemplateRequest,
    CreateWorkspaceTemplateResponse, DeleteWorkspaceRequest, DeleteWorkspaceResponse,
    MoveWorkspaceDataRequest, MoveWorkspaceDataResponse,
};

use std::sync::Arc;
use tonic::{Request, Response, Result, Status};

crate::impl_grpc_server!(WorkspaceServiceImpl, default_endpoint: String);

#[tonic::async_trait]
impl WorkspaceService for WorkspaceServiceImpl {
    async fn create_workspace_template(
        &self,
        request: Request<CreateWorkspaceTemplateRequest>,
    ) -> Result<Response<CreateWorkspaceTemplateResponse>> {
        log_received!(&request);

        // Consume gRPC request into its parts
        let (request_metadata, _, inner_request) = request.into_parts();
        let request = CreateTemplate(inner_request);

        // Extract token from request and check permissions
        let token = tonic_auth!(
            get_token_from_md(&request_metadata),
            "Token authentication error"
        );

        // Deny service_accounts
        let mut ctx = Context::self_ctx();
        ctx.allow_service_account = false;

        let user_id = tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        // Create template
        let template_name = tonic_invalid!(
            self.database_handler
                .create_workspace_template(request, user_id)
                .await,
            "Invalid request"
        );

        // TODO: Change name into id
        let response = CreateWorkspaceTemplateResponse { template_name };
        return_with_log!(response);
    }

    // TODO:
    // - get_templates_by()
    // - get_template()
    // - delete_template()

    async fn create_workspace(
        &self,
        request: Request<CreateWorkspaceRequest>,
    ) -> Result<Response<CreateWorkspaceResponse>> {
        log_received!(&request);

        let request = CreateWorkspace(request.into_inner());

        let endpoint = self.default_endpoint.clone();
        // Create template
        let template_name = tonic_invalid!(
            self.database_handler
                .create_workspace(self.authorizer.clone(), request, endpoint)
                .await,
            "Invalid request"
        );

        return Err(Status::unimplemented(
            "Creating workspaces is not implemented!",
        ));
    }

    async fn delete_workspace(
        &self,
        _request: Request<DeleteWorkspaceRequest>,
    ) -> Result<Response<DeleteWorkspaceResponse>> {
        return Err(Status::unimplemented(
            "Deleting workspaces is not implemented!",
        ));
    }

    async fn claim_workspace(
        &self,
        _request: Request<ClaimWorkspaceRequest>,
    ) -> Result<Response<ClaimWorkspaceResponse>> {
        // TODO:
        // - Remove all hooks
        // - Make user account project admin
        return Err(Status::unimplemented(
            "Claiming workspaces is not implemented!",
        ));
    }

    async fn move_workspace_data(
        &self,
        _request: Request<MoveWorkspaceDataRequest>,
    ) -> Result<Response<MoveWorkspaceDataResponse>> {
        return Err(Status::unimplemented(
            "Moving workspaces is not implemented!",
        ));
    }
}
