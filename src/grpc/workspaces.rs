use crate::auth::structs::Context;
use crate::caching::cache::Cache;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::workspace_request_types::{CreateTemplate, CreateWorkspace};
use crate::{auth::permission_handler::PermissionHandler, utils::conversions::get_token_from_md};
use aruna_rust_api::api::storage::services::v2::{
    workspace_service_server::WorkspaceService, ClaimWorkspaceRequest, ClaimWorkspaceResponse,
    CreateWorkspaceRequest, CreateWorkspaceResponse, CreateWorkspaceTemplateRequest,
    CreateWorkspaceTemplateResponse, DeleteWorkspaceRequest, DeleteWorkspaceResponse,
};
use aruna_rust_api::api::storage::services::v2::{
    DeleteWorkspaceTemplateRequest, DeleteWorkspaceTemplateResponse, GetWorkspaceTemplateRequest,
    GetWorkspaceTemplateResponse, ListOwnedWorkspaceTemplatesRequest,
    ListOwnedWorkspaceTemplatesResponse,
};

use std::str::FromStr;
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
        let template_id = tonic_invalid!(
            self.database_handler
                .create_workspace_template(request, user_id)
                .await,
            "Invalid request"
        );

        // TODO: Change name into id
        let response = CreateWorkspaceTemplateResponse { template_id };
        return_with_log!(response);
    }

    async fn create_workspace(
        &self,
        request: Request<CreateWorkspaceRequest>,
    ) -> Result<Response<CreateWorkspaceResponse>> {
        log_received!(&request);

        let request = CreateWorkspace(request.into_inner());

        let endpoint = self.default_endpoint.clone();
        // Create template
        let (workspace_id, access_key, secret_key, token) = tonic_invalid!(
            self.database_handler
                .create_workspace(self.authorizer.clone(), request, endpoint)
                .await,
            "Invalid request"
        );

        let response = CreateWorkspaceResponse {
            workspace_id: workspace_id.to_string(),
            access_key,
            secret_key,
            token,
        };
        return_with_log!(response);
    }

    async fn delete_workspace(
        &self,
        request: Request<DeleteWorkspaceRequest>,
    ) -> Result<Response<DeleteWorkspaceResponse>> {
        log_received!(&request);
        let (metadata, _, request) = request.into_parts();

        let token = tonic_auth!(get_token_from_md(&metadata), "Token authentication error");
        let id = tonic_invalid!(
            diesel_ulid::DieselUlid::from_str(&request.workspace_id),
            "Invalid workspace id"
        );

        // Deny service_accounts
        let ctx = Context::res_ctx(
            id,
            //TODO: allow service accounts to delete their own workspaces?
            crate::database::enums::DbPermissionLevel::ADMIN,
            false,
        );

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        tonic_internal!(
            self.database_handler.delete_workspace(id).await,
            "Internal error for DeleteWorkspaceRequest"
        );

        return_with_log!(DeleteWorkspaceResponse {});
    }

    async fn claim_workspace(
        &self,
        request: Request<ClaimWorkspaceRequest>,
    ) -> Result<Response<ClaimWorkspaceResponse>> {
        log_received!(&request);
        let (metadata, _, request) = request.into_parts();

        let token = tonic_auth!(get_token_from_md(&metadata), "Token authentication error");
        let id = tonic_invalid!(
            diesel_ulid::DieselUlid::from_str(&request.workspace_id),
            "Invalid workspace id"
        );

        // Check if user is valid
        let user_ctx = Context::user_ctx(id, crate::database::enums::DbPermissionLevel::NONE);
        let user_id = tonic_auth!(
            self.authorizer
                .check_permissions(&token, vec![user_ctx])
                .await,
            "Unauthorized"
        );
        // Probably does not work, because new user is not assoiciated with workspace project
        // TODO: Use request_token
        let res_ctx = Context::res_ctx(
            id,
            crate::database::enums::DbPermissionLevel::APPEND, // Must be append, because
            // workspaces are APPEND only for
            // service accounts
            false,
        );
        tonic_auth!(
            self.authorizer
                .check_permissions(&request.token, vec![res_ctx])
                .await,
            "Unauthorized"
        );

        return Err(Status::unimplemented(
            "Claiming workspaces is not implemented!",
        ));
    }
    async fn get_workspace_template(
        &self,
        request: Request<GetWorkspaceTemplateRequest>,
    ) -> Result<Response<GetWorkspaceTemplateResponse>> {
        Err(tonic::Status::unimplemented(
            "GetWorkspaceTemplate is currently unimplemented",
        ))
    }
    async fn list_owned_workspace_templates(
        &self,
        request: Request<ListOwnedWorkspaceTemplatesRequest>,
    ) -> Result<Response<ListOwnedWorkspaceTemplatesResponse>> {
        Err(tonic::Status::unimplemented(
            "ListOwnedWorkspaceTemplates is currently unimplemented",
        ))
    }
    async fn delete_workspace_template(
        &self,
        request: Request<DeleteWorkspaceTemplateRequest>,
    ) -> Result<Response<DeleteWorkspaceTemplateResponse>> {
        Err(tonic::Status::unimplemented(
            "DeleteWorkspaceTemplate is currently unimplemented",
        ))
    }
}
