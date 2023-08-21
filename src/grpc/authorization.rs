use crate::auth::permission_handler::PermissionHandler;
use crate::auth::structs::Context;
use crate::caching::cache::Cache;

use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::delete_request_types::DeleteRequest;
use crate::middlelayer::snapshot_request_types::SnapshotRequest;
use crate::middlelayer::update_request_types::{
    DataClassUpdate, DescriptionUpdate, KeyValueUpdate, NameUpdate,
};
use crate::utils::conversions::get_token_from_md;
use crate::utils::grpc_utils::{get_id_and_ctx, query, IntoGenericInner};
use anyhow::anyhow;
use aruna_rust_api::api::storage::models::v2::{generic_resource, Collection};
use aruna_rust_api::api::storage::services::v2::authorization_service_server::AuthorizationService;
use aruna_rust_api::api::storage::services::v2::collection_service_server::CollectionService;
use aruna_rust_api::api::storage::services::v2::{
    CreateAuthorizationRequest, CreateAuthorizationResponse, DeleteAuthorizationRequest,
    DeleteAuthorizationResponse, GetAuthorizationsRequest, GetAuthorizationsResponse,
    UpdateAuthorizationsRequest, UpdateAuthorizationsResponse,
};
use diesel_ulid::DieselUlid;
use std::str::FromStr;
use std::sync::Arc;

crate::impl_grpc_server!(AuthorizationServiceImpl);

#[tonic::async_trait]
impl AuthorizationService for AuthorizationServiceImpl {
    /// CreateAuthorization
    ///
    /// Status: BETA
    ///
    /// This creates a user-specific attribute that handles permission for a
    /// specific resource
    async fn create_authorization(
        &self,
        request: tonic::Request<CreateAuthorizationRequest>,
    ) -> std::result::Result<tonic::Response<CreateAuthorizationResponse>, tonic::Status> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let ctx = Context::res_ctx(
            tonic_invalid!(
                DieselUlid::from_str(&request.get_ref().resource_id),
                "Invalid ulid"
            ),
            crate::database::enums::DbPermissionLevel::ADMIN,
            false,
        );
        let user_id = tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        return_with_log!(response);
    }
    /// GetAuthorization
    ///
    /// Status: BETA
    ///
    /// This gets resource specific user authorizations
    async fn get_authorizations(
        &self,
        request: tonic::Request<GetAuthorizationsRequest>,
    ) -> std::result::Result<tonic::Response<GetAuthorizationsResponse>, tonic::Status> {
        todo!()
    }
    /// DeleteAuthorization
    ///
    /// Status: BETA
    ///
    /// This creates a user-specific attribute that handles permission for a
    /// specific resource
    async fn delete_authorization(
        &self,
        request: tonic::Request<DeleteAuthorizationRequest>,
    ) -> std::result::Result<tonic::Response<DeleteAuthorizationResponse>, tonic::Status> {
        todo!()
    }
    /// UpdateAuthorization
    ///
    /// Status: BETA
    ///
    /// This creates a user-specific attribute that handles permission for a
    /// specific resource
    async fn update_authorizations(
        &self,
        request: tonic::Request<UpdateAuthorizationsRequest>,
    ) -> std::result::Result<tonic::Response<UpdateAuthorizationsResponse>, tonic::Status> {
        todo!()
    }
}
