use crate::auth::permission_handler::PermissionHandler;
use crate::auth::structs::Context;
use crate::caching::cache::Cache;
use crate::database::enums::DbPermissionLevel;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::hooks_request_types::CreateHook;
use crate::utils::conversions::get_token_from_md;
use aruna_rust_api::api::hooks::services::v2::hooks_service_server::HooksService;
use aruna_rust_api::api::hooks::services::v2::{
    CreateHookRequest, CreateHookResponse, DeleteHookRequest, DeleteHookResponse,
    HookCallbackRequest, HookCallbackResponse, ListHooksRequest, ListHooksResponse,
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
        let project_id = tonic_invalid!(request.get_project_id(), "invalid parent");

        let ctx = Context::res_ctx(project_id, DbPermissionLevel::APPEND, true);
        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let hook = tonic_internal!(
            self.database_handler.create_hook(request).await,
            "Error while creating hook"
        );

        let response = CreateHookResponse {
            hook_id: hook.id.to_string(),
        };

        return_with_log!(response);
    }
    async fn list_hooks(
        &self,
        request: Request<ListHooksRequest>,
    ) -> Result<Response<ListHooksResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let request = request.into_inner();
        let project_id =
            tonic_invalid!(DieselUlid::from_str(&request.project_id), "invalid parent");
        let ctx = Context::res_ctx(project_id, DbPermissionLevel::READ, true);
        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let hooks = tonic_internal!(
            self.database_handler.list_hook(request).await,
            "Internal error while listing hooks"
        );

        let response = ListHooksResponse {
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
        let project_id = tonic_invalid!(
            self.database_handler.get_project_by_hook(&hook_id).await,
            "Hook or parent not found"
        );

        let ctx = Context::res_ctx(project_id, DbPermissionLevel::WRITE, true);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
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
        tonic_internal!(
            self.database_handler.hook_callback(request).await,
            "HookCallback failed"
        );
        return_with_log!(HookCallbackResponse {});
    }
}
