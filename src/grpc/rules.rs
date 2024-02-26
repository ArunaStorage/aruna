use crate::auth::permission_handler::PermissionHandler;
use crate::caching::cache::Cache;
use crate::middlelayer::db_handler::DatabaseHandler;
use aruna_rust_api::api::storage::services::v2::{
    rules_service_server::RulesService, CreateRuleResponse,
};
use aruna_rust_api::api::storage::services::v2::{
    CreateRuleBindingRequest, CreateRuleBindingResponse, CreateRuleRequest,
    DeleteRuleBindingRequest, DeleteRuleBindingResponse, DeleteRuleRequest, DeleteRuleResponse,
    GetRuleRequest, GetRuleResponse, ListRuleRequest, ListRuleResponse, UpdateRuleRequest,
    UpdateRuleResponse,
};
use std::sync::Arc;
use tonic::{Request, Response, Result};

crate::impl_grpc_server!(RuleServiceImpl);

#[tonic::async_trait]
impl RulesService for RuleServiceImpl {
    async fn create_rule(
        &self,
        request: Request<CreateRuleRequest>,
    ) -> Result<Response<CreateRuleResponse>> {
        Err(tonic::Status::unimplemented(
            "Creating rules currently not implemented",
        ))
    }

    async fn get_rule(
        &self,
        request: Request<GetRuleRequest>,
    ) -> Result<Response<GetRuleResponse>> {
        Err(tonic::Status::unimplemented(
            "Getting rules currently not implemented",
        ))
    }
    async fn delete_rule(
        &self,
        request: Request<DeleteRuleRequest>,
    ) -> Result<Response<DeleteRuleResponse>> {
        Err(tonic::Status::unimplemented(
            "Deleting rules currently not implemented",
        ))
    }
    async fn list_rule(
        &self,
        request: Request<ListRuleRequest>,
    ) -> Result<Response<ListRuleResponse>> {
        Err(tonic::Status::unimplemented(
            "Listing rules currently not implemented",
        ))
    }

    async fn update_rule(
        &self,
        request: Request<UpdateRuleRequest>,
    ) -> Result<Response<UpdateRuleResponse>> {
        Err(tonic::Status::unimplemented(
            "Updating rules currently not implemented",
        ))
    }

    async fn create_rule_binding(
        &self,
        request: Request<CreateRuleBindingRequest>,
    ) -> Result<Response<CreateRuleBindingResponse>> {
        Err(tonic::Status::unimplemented(
            "Creating rule bindings currently not implemented",
        ))
    }

    async fn delete_rule_binding(
        &self,
        request: Request<DeleteRuleBindingRequest>,
    ) -> Result<Response<DeleteRuleBindingResponse>> {
        Err(tonic::Status::unimplemented(
            "Deleting rule bindings currently not implemented",
        ))
    }
}
