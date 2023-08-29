use crate::auth::permission_handler::PermissionHandler;
use crate::auth::structs::Context;
use crate::caching::cache::Cache;
use crate::database::dsls::object_dsl::ObjectWithRelations;
use crate::database::enums::DbPermissionLevel;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::presigned_url_handler::{PresignedDownload, PresignedUpload};
use crate::utils::conversions::get_token_from_md;
use crate::utils::grpc_utils::{self, get_id_and_ctx, query, IntoGenericInner};
use aruna_rust_api::api::hooks::services::v2::hooks_service_server::HooksService;
use aruna_rust_api::api::hooks::services::v2::{
    CreateHookRequest, CreateHookResponse, DeleteHookRequest, DeleteHookResponse,
    HookCallbackRequest, HookCallbackResponse, ListHooksRequest, ListHooksResponse,
};
use diesel_ulid::DieselUlid;
use std::str::FromStr;
use std::sync::Arc;
use tonic::{Request, Response, Result, Status};

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

        let request = request.into_inner();
        let project_id =
            tonic_invalid!(DieselUlid::from_str(&request.project_id), "invalid parent");

        let ctx = Context::res_ctx(project_id, DbPermissionLevel::APPEND, true);
        let user_id = tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        todo!()
    }
    async fn list_hooks(
        &self,
        request: Request<ListHooksRequest>,
    ) -> Result<Response<ListHooksResponse>> {
        todo!()
    }
    async fn delete_hook(
        &self,
        request: Request<DeleteHookRequest>,
    ) -> Result<Response<DeleteHookResponse>> {
        todo!()
    }
    async fn hook_callback(
        &self,
        request: Request<HookCallbackRequest>,
    ) -> Result<Response<HookCallbackResponse>> {
        todo!()
    }
}
