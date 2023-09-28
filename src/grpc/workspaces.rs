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
use tonic::{Request, Response, Result};
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

        let ctx = Context::res_ctx(id, crate::database::enums::DbPermissionLevel::APPEND, true);
        let service_account_id = tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        tonic_internal!(
            // Deletes workspace and user
            self.database_handler
                .delete_workspace(id, service_account_id)
                .await,
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
        let user_ctx = Context::self_ctx();
        let user_id = tonic_auth!(
            self.authorizer
                .check_permissions(&token, vec![user_ctx])
                .await,
            "Unauthorized"
        );
        // Check if token is valid and has permissions for workspace
        let res_ctx = Context::res_ctx(
            id,
            crate::database::enums::DbPermissionLevel::APPEND,
            // Must be append, because workspaces
            // are APPEND only for service accounts
            true,
        );
        tonic_auth!(
            self.authorizer
                .check_permissions(&request.token, vec![res_ctx])
                .await,
            "Unauthorized"
        );

        tonic_internal!(
            self.database_handler
                .claim_workspace(request, user_id)
                .await,
            "Internal database error"
        );

        return_with_log!(ClaimWorkspaceResponse {});
    }
    async fn get_workspace_template(
        &self,
        request: Request<GetWorkspaceTemplateRequest>,
    ) -> Result<Response<GetWorkspaceTemplateResponse>> {
        log_received!(&request);
        let (metadata, _, request) = request.into_parts();

        // Authorization
        let token = tonic_auth!(get_token_from_md(&metadata), "Token authentication error");
        let ctx = Context::self_ctx();
        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let template_id = tonic_invalid!(
            diesel_ulid::DieselUlid::from_str(&request.template_id),
            "Invalid workspace id"
        );

        let workspace = tonic_invalid!(
            self.database_handler.get_ws_template(&template_id).await,
            "No template found"
        );
        let response = GetWorkspaceTemplateResponse {
            workspaces: Some(workspace.into()),
        };

        return_with_log!(response);
    }
    async fn list_owned_workspace_templates(
        &self,
        request: Request<ListOwnedWorkspaceTemplatesRequest>,
    ) -> Result<Response<ListOwnedWorkspaceTemplatesResponse>> {
        log_received!(&request);
        let metadata = request.metadata();

        // Authorization
        let token = tonic_auth!(get_token_from_md(&metadata), "Token authentication error");
        let ctx = Context::self_ctx();
        let user_id = tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );
        let workspaces = tonic_invalid!(
            self.database_handler.get_owned_ws(&user_id).await,
            "No workspaces found"
        );
        let response = ListOwnedWorkspaceTemplatesResponse {
            workspaces: workspaces.into_iter().map(|ws| ws.into()).collect(),
        };
        return_with_log!(response);
    }
    async fn delete_workspace_template(
        &self,
        request: Request<DeleteWorkspaceTemplateRequest>,
    ) -> Result<Response<DeleteWorkspaceTemplateResponse>> {
        log_received!(&request);
        let (metadata, _, request) = request.into_parts();
        let token = tonic_auth!(get_token_from_md(&metadata), "Token authentication error");
        let ctx = Context::self_ctx();
        let user_id = tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );
        tonic_invalid!(
            self.database_handler
                .delete_workspace_template(request.template_id, &user_id)
                .await,
            "No workspaces found"
        );
        return_with_log!(DeleteWorkspaceTemplateResponse {});
    }
}
