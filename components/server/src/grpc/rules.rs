use crate::auth::permission_handler::PermissionHandler;
use crate::auth::structs::Context;
use crate::caching::cache::Cache;
use crate::database::enums::DbPermissionLevel;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::rule_request_types::{
    CreateRule, CreateRuleBinding, DeleteRule, DeleteRuleBinding, UpdateRule,
};
use crate::utils::grpc_utils::get_token_from_md;
use aruna_rust_api::api::storage::services::v2::{
    rules_service_server::RulesService, CreateRuleResponse, Rule,
};
use aruna_rust_api::api::storage::services::v2::{
    CreateRuleBindingRequest, CreateRuleBindingResponse, CreateRuleRequest,
    DeleteRuleBindingRequest, DeleteRuleBindingResponse, DeleteRuleRequest, DeleteRuleResponse,
    GetRuleRequest, GetRuleResponse, ListRuleRequest, ListRuleResponse, UpdateRuleRequest,
    UpdateRuleResponse,
};
use std::str::FromStr;
use std::sync::Arc;
use tonic::{Request, Response, Result};

crate::impl_grpc_server!(RuleServiceImpl);

#[tonic::async_trait]
impl RulesService for RuleServiceImpl {
    async fn create_rule(
        &self,
        request: Request<CreateRuleRequest>,
    ) -> Result<Response<CreateRuleResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let request = CreateRule(request.into_inner());
        let user_id = tonic_auth!(
            self.authorizer
                .check_permissions(
                    &token,
                    vec![Context {
                        variant: crate::auth::structs::ContextVariant::Registered,
                        allow_service_account: false,
                        is_self: false,
                    }]
                )
                .await,
            "Unauthorized"
        );

        let response = CreateRuleResponse {
            id: tonic_invalid!(
                self.database_handler.create_rule(request, user_id).await,
                "Invalid request"
            )
            .to_string(),
        };

        return_with_log!(response);
    }

    async fn get_rule(
        &self,
        request: Request<GetRuleRequest>,
    ) -> Result<Response<GetRuleResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let id = tonic_invalid!(
            diesel_ulid::DieselUlid::from_str(&request.into_inner().id),
            "Invalid rule id"
        );

        let rule: Rule = if let Some(rule) = self.cache.get_rule(&id) {
            if rule.rule.is_public {
                rule.as_ref().into()
            } else {
                let user_id = tonic_auth!(
                    self.authorizer
                        .check_permissions(
                            &token,
                            vec![Context {
                                variant: crate::auth::structs::ContextVariant::Registered,
                                allow_service_account: false,
                                is_self: false,
                            }]
                        )
                        .await,
                    "Unauthorized"
                );
                if user_id == rule.rule.owner_id {
                    rule.as_ref().into()
                } else {
                    return Err(tonic::Status::unauthenticated("Unauthorized"));
                }
            }
        } else {
            return Err(tonic::Status::not_found("Rule not found"));
        };

        let response = GetRuleResponse { rule: Some(rule) };
        return_with_log!(response);
    }
    async fn list_rule(
        &self,
        _request: Request<ListRuleRequest>,
    ) -> Result<Response<ListRuleResponse>> {
        let rules = self.cache.list_rules();
        let response = ListRuleResponse { rules };
        return_with_log!(response);
    }
    async fn update_rule(
        &self,
        request: Request<UpdateRuleRequest>,
    ) -> Result<Response<UpdateRuleResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let request = UpdateRule(request.into_inner());
        let id = tonic_invalid!(request.get_id(), "Invalid ID");
        let rule = self
            .cache
            .get_rule(&id)
            .ok_or_else(|| tonic::Status::not_found("Rule not found"))?;
        let user_id = tonic_auth!(
            self.authorizer
                .check_permissions(
                    &token,
                    vec![Context {
                        variant: crate::auth::structs::ContextVariant::Registered,
                        allow_service_account: false,
                        is_self: false,
                    }]
                )
                .await,
            "Unauthorized"
        );
        if rule.rule.owner_id != user_id {
            return Err(tonic::Status::unauthenticated("Unauthorized"));
        }
        let rule = &tonic_invalid!(
            self.database_handler.update_rule(request, rule).await,
            "Invalid request"
        );
        let response = UpdateRuleResponse {
            rule: Some(rule.into()),
        };

        return_with_log!(response);
    }

    async fn delete_rule(
        &self,
        request: Request<DeleteRuleRequest>,
    ) -> Result<Response<DeleteRuleResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let request = DeleteRule(request.into_inner());
        let id = tonic_invalid!(request.get_id(), "Invalid ID");
        let rule = self
            .cache
            .get_rule(&id)
            .ok_or_else(|| tonic::Status::not_found("Rule not found"))?;
        let user_id = tonic_auth!(
            self.authorizer
                .check_permissions(
                    &token,
                    vec![Context {
                        variant: crate::auth::structs::ContextVariant::Registered,
                        allow_service_account: false,
                        is_self: false,
                    }]
                )
                .await,
            "Unauthorized"
        );
        if rule.rule.owner_id != user_id {
            return Err(tonic::Status::unauthenticated("Unauthorized"));
        }
        tonic_invalid!(
            self.database_handler.delete_rule(&rule).await,
            "Invalid request"
        );
        return_with_log!(DeleteRuleResponse {});
    }

    async fn create_rule_binding(
        &self,
        request: Request<CreateRuleBindingRequest>,
    ) -> Result<Response<CreateRuleBindingResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let request = CreateRuleBinding(request.into_inner());
        let resource_id = tonic_invalid!(request.get_resource_id(), "Invalid resource id");
        let rule_id = tonic_invalid!(request.get_rule_id(), "Invalid rule id");

        let user_id = tonic_auth!(
            self.authorizer
                .check_permissions(
                    &token,
                    vec![Context::res_ctx(
                        resource_id,
                        DbPermissionLevel::ADMIN,
                        false
                    )]
                )
                .await,
            "Unauthorized"
        );
        let rule = self
            .cache
            .get_rule(&rule_id)
            .ok_or_else(|| tonic::Status::not_found("Rule not found"))?;
        if !rule.rule.is_public && rule.rule.owner_id != user_id {
            // Private rules can only be added by rule owners
            return Err(tonic::Status::unauthenticated("Unauthorized"));
        }
        tonic_invalid!(
            self.database_handler
                .create_rule_binding(request.clone())
                .await,
            "Invalid request"
        );
        let response = CreateRuleBindingResponse {
            rule_id: rule_id.to_string(),
            object_id: resource_id.to_string(),
            cascading: request.0.cascading,
        };
        return_with_log!(response);
    }

    async fn delete_rule_binding(
        &self,
        request: Request<DeleteRuleBindingRequest>,
    ) -> Result<Response<DeleteRuleBindingResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let request = DeleteRuleBinding(request.into_inner());
        let (resource_id, rule_id) = tonic_invalid!(request.get_ids(), "Invalid ids provided");
        tonic_auth!(
            self.authorizer
                .check_permissions(
                    &token,
                    vec![Context::res_ctx(
                        resource_id,
                        DbPermissionLevel::ADMIN,
                        false
                    )]
                )
                .await,
            "Unauthorized"
        );
        self.cache
            .get_rule(&rule_id)
            .ok_or_else(|| tonic::Status::not_found("Rule not found"))?;

        tonic_invalid!(
            self.database_handler.delete_rule_binding(request).await,
            "Invalid request"
        );
        return_with_log!(DeleteRuleBindingResponse {});
    }
}
