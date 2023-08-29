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
