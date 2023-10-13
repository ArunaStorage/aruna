use crate::auth::permission_handler::PermissionHandler;
use crate::auth::structs::Context;
use crate::caching::cache::Cache;
use crate::database::enums::DbPermissionLevel;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::hooks_request_types::CreateHook;
use crate::middlelayer::hooks_request_types::ListBy;
use crate::utils::conversions::get_token_from_md;
use aruna_rust_api::api::hooks::services::v2::hooks_service_server::HooksService;
use aruna_rust_api::api::hooks::services::v2::AddProjectsToHookRequest;
use aruna_rust_api::api::hooks::services::v2::AddProjectsToHookResponse;
use aruna_rust_api::api::hooks::services::v2::{
    CreateHookRequest, CreateHookResponse, DeleteHookRequest, DeleteHookResponse,
    HookCallbackRequest, HookCallbackResponse, ListOwnedHooksRequest, ListOwnedHooksResponse,
    ListProjectHooksRequest, ListProjectHooksResponse,
};
use diesel_ulid::DieselUlid;
use std::str::FromStr;
use std::sync::Arc;
use tonic::{Request, Response, Result};

crate::impl_grpc_server!(HookServiceImpl);

#[tonic::async_trait]
impl HooksService for HookServiceImpl {
    async fn create_hook(
        &self,
        request: Request<CreateHookRequest>,
    ) -> Result<Response<CreateHookResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let request = CreateHook(request.into_inner());
        let project_ids = tonic_invalid!(request.get_project_ids(), "invalid parent");

        let ctx = project_ids
            .iter()
            .map(|id| Context::res_ctx(*id, DbPermissionLevel::APPEND, true))
            .collect();

        let user_id = tonic_auth!(
            self.authorizer.check_permissions(&token, ctx).await,
            "Unauthorized"
        );

        let hook = tonic_internal!(
            self.database_handler.create_hook(request, &user_id).await,
            "Error while creating hook"
        );

        let response = CreateHookResponse {
            hook_id: hook.id.to_string(),
        };

        return_with_log!(response);
    }

    async fn list_project_hooks(
        &self,
        request: Request<ListProjectHooksRequest>,
    ) -> Result<Response<ListProjectHooksResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let request = ListBy::PROJECT(request.into_inner());
        let project_id = tonic_invalid!(request.get_id(), "invalid parent");
        let ctx = Context::res_ctx(project_id, DbPermissionLevel::ADMIN, true);
        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let hooks = tonic_internal!(
            self.database_handler.list_hook(request).await,
            "Internal error while listing hooks"
        );

        let response = ListProjectHooksResponse {
            infos: hooks.into_iter().map(|h| h.into()).collect(),
        };

        return_with_log!(response);
    }

    async fn list_owned_hooks(
        &self,
        request: Request<ListOwnedHooksRequest>,
    ) -> Result<Response<ListOwnedHooksResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let request = request.into_inner();
        let request = if request.user_id.is_empty() {
            let ctx = Context::self_ctx();
            let owner = tonic_auth!(
                self.authorizer.check_permissions(&token, vec![ctx]).await,
                "Unauthorized"
            );
            ListBy::OWNER(owner)
        } else {
            let id = tonic_invalid!(DieselUlid::from_str(&request.user_id), "Invalid user_id");
            let ctx = Context::user_ctx(id, DbPermissionLevel::ADMIN);
            tonic_auth!(
                self.authorizer.check_permissions(&token, vec![ctx]).await,
                "Unauthorized"
            );
            ListBy::OWNER(id)
        };
        let hooks = tonic_internal!(
            self.database_handler.list_hook(request).await,
            "Internal error while listing hooks"
        );

        let response = ListOwnedHooksResponse {
            infos: hooks.into_iter().map(|h| h.into()).collect(),
        };

        return_with_log!(response);
    }
    async fn delete_hook(
        &self,
        request: Request<DeleteHookRequest>,
    ) -> Result<Response<DeleteHookResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let request = request.into_inner();
        let hook_id = tonic_invalid!(DieselUlid::from_str(&request.hook_id), "invalid hook id");
        let project_ids = tonic_invalid!(
            self.database_handler.get_project_by_hook(&hook_id).await,
            "Hook or parent not found"
        );

        let ctx = project_ids
            .iter()
            .map(|id| Context::res_ctx(*id, DbPermissionLevel::ADMIN, true))
            .collect();

        tonic_auth!(
            self.authorizer.check_permissions(&token, ctx).await,
            "Unauthorized"
        );

        tonic_internal!(
            self.database_handler.delete_hook(hook_id).await,
            "Error while deleting hook"
        );
        return_with_log!(DeleteHookResponse {});
    }
    async fn hook_callback(
        &self,
        request: Request<HookCallbackRequest>,
    ) -> Result<Response<HookCallbackResponse>> {
        log_received!(&request);
        let request = crate::middlelayer::hooks_request_types::Callback(request.into_inner());

        tonic_auth!(
            request.verify_secret(self.authorizer.clone(), self.cache.clone()),
            "Unauthorized"
        );
        dbg!("Verified secret");

        tonic_internal!(
            self.database_handler.hook_callback(request).await,
            "HookCallback failed"
        );
        return_with_log!(HookCallbackResponse {});
    }
    async fn add_projects_to_hook(
        &self,
        request: Request<AddProjectsToHookRequest>,
    ) -> Result<Response<AddProjectsToHookResponse>> {
        log_received!(&request);
        let (metadata, _, request) = request.into_parts();

        let token = tonic_auth!(get_token_from_md(&metadata), "Token authentication error");
        let project_ids = request
            .project_ids
            .iter()
            .map(|id| {
                DieselUlid::from_str(id)
                    .map_err(|_| tonic::Status::invalid_argument("Invalid id provided"))
            })
            .collect::<Result<Vec<DieselUlid>>>()?;
        let ctx = project_ids
            .iter()
            .map(|id| Context::res_ctx(*id, DbPermissionLevel::ADMIN, true))
            .collect();
        let user = tonic_auth!(
            self.authorizer.check_permissions(&token, ctx).await,
            "Unauthorized"
        );

        tonic_internal!(
            self.database_handler
                .append_project_to_hook(request, &user)
                .await,
            "HookCallback failed"
        );
        return_with_log!(AddProjectsToHookResponse {});
    }
}
