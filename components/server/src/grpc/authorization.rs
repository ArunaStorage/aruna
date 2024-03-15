use crate::auth::permission_handler::PermissionHandler;
use crate::auth::structs::Context;
use crate::caching::cache::Cache;
use crate::database::enums::DbPermissionLevel;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::utils::grpc_utils::get_token_from_md;
use aruna_rust_api::api::storage::services::v2::authorization_service_server::AuthorizationService;
use aruna_rust_api::api::storage::services::v2::{
    CreateAuthorizationRequest, CreateAuthorizationResponse, DeleteAuthorizationRequest,
    DeleteAuthorizationResponse, GetAuthorizationsRequest, GetAuthorizationsResponse,
    ResourceAuthorization, UpdateAuthorizationRequest, UpdateAuthorizationResponse, UserPermission,
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
    ) -> Result<tonic::Response<CreateAuthorizationResponse>, tonic::Status> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let user_id = DieselUlid::from_str(&request.get_ref().user_id)
            .map_err(|_| tonic::Status::invalid_argument("Invalid ulid"))?;
        let resource_id = DieselUlid::from_str(&request.get_ref().resource_id)
            .map_err(|_| tonic::Status::invalid_argument("Invalid ulid"))?;

        let ctx = Context::res_ctx(resource_id, DbPermissionLevel::ADMIN, false);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let obj = self
            .cache
            .get_object(&resource_id)
            .ok_or_else(|| tonic::Status::not_found("Resource not found"))?;

        let user = tonic_internal!(
            self.database_handler
                .add_permission_to_user(
                    user_id,
                    resource_id,
                    &obj.object.name,
                    obj.as_object_mapping(tonic_invalid!(
                        DbPermissionLevel::try_from(request.get_ref().permission_level),
                        "Invalid permission level"
                    )),
                    true
                )
                .await,
            "Internal error"
        );

        // Create response
        let resp = CreateAuthorizationResponse {
            resource_id: resource_id.to_string(),
            user_id: user_id.to_string(),
            user_name: user.display_name.to_string(),
            permission_level: request.into_inner().permission_level,
        };

        // Return gRPC response
        return_with_log!(resp);
    }

    /// GetAuthorization
    ///
    /// Status: BETA
    ///
    /// This gets resource specific user authorizations
    async fn get_authorizations(
        &self,
        request: tonic::Request<GetAuthorizationsRequest>,
    ) -> Result<tonic::Response<GetAuthorizationsResponse>, tonic::Status> {
        // Log some stuff
        log_received!(&request);

        // Consume gRPC request into its parts
        let (metadata, _, inner_request) = request.into_parts();

        // Validate request parameter
        let resource_id = tonic_invalid!(
            DieselUlid::from_str(&inner_request.resource_id),
            "Invalid resource id format"
        );

        // Check permissions to fetch authorizations
        let token = tonic_auth!(get_token_from_md(&metadata), "Token authentication error");

        let ctx = Context::res_ctx(resource_id, DbPermissionLevel::ADMIN, false);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        //ToDo: Check all users for permissions on specific resource ... ?
        let perms = tonic_internal!(
            self.cache
                .get_resource_permissions(resource_id, inner_request.recursive),
            "Permission fetch failed"
        );

        let authorizations = perms
            .into_iter()
            .map(|(resource_id, user_permissions)| ResourceAuthorization {
                resource_id: resource_id.to_string(),
                user_permission: user_permissions,
            })
            .collect::<Vec<_>>();

        // Return found authorizations
        let response = GetAuthorizationsResponse { authorizations };
        return_with_log!(response);
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
    ) -> Result<tonic::Response<DeleteAuthorizationResponse>, tonic::Status> {
        // Log some stuff
        log_received!(&request);

        // Consume gRPC request into its parts
        let (metadata, _, inner_request) = request.into_parts();

        // Validate request parameter
        let resource_id = tonic_invalid!(
            DieselUlid::from_str(&inner_request.resource_id),
            "Invalid resource id format"
        );

        let user_id = tonic_invalid!(
            DieselUlid::from_str(&inner_request.user_id),
            "Invalid user id format"
        );

        // Check permissions to fetch authorizations
        let token = tonic_auth!(get_token_from_md(&metadata), "Token authentication error");

        let ctx = Context::res_ctx(resource_id, DbPermissionLevel::ADMIN, false);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        // Remove resource permission from user
        tonic_internal!(
            self.database_handler
                .remove_permission_from_user(user_id, resource_id)
                .await,
            "Permission removal failed"
        );

        // Return found authorizations
        let response = DeleteAuthorizationResponse {};
        return_with_log!(response);
    }

    /// UpdateAuthorization
    ///
    /// Status: BETA
    ///
    /// This creates a user-specific attribute that handles permission for a
    /// specific resource
    async fn update_authorization(
        &self,
        request: tonic::Request<UpdateAuthorizationRequest>,
    ) -> Result<tonic::Response<UpdateAuthorizationResponse>, tonic::Status> {
        // Log some stuff
        log_received!(&request);

        // Consume gRPC request into its parts
        let (metadata, _, inner_request) = request.into_parts();

        // Validate request parameter
        let resource_id = tonic_invalid!(
            DieselUlid::from_str(&inner_request.resource_id),
            "Invalid resource id format"
        );

        let user_id = tonic_invalid!(
            DieselUlid::from_str(&inner_request.user_id),
            "Invalid user id format"
        );

        let permission_level: DbPermissionLevel = tonic_invalid!(
            inner_request.permission_level.try_into(),
            "Invalid permission level"
        );

        // Check permissions to fetch authorizations
        let token = tonic_auth!(get_token_from_md(&metadata), "Token authentication error");

        let ctx = Context::res_ctx(resource_id, DbPermissionLevel::ADMIN, false);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        // Fetch object from cache to determine type
        let permission = self
            .cache
            .get_object(&resource_id)
            .ok_or_else(|| tonic::Status::not_found("Object does not exist"))?
            .as_object_mapping::<DbPermissionLevel>(permission_level);

        // Update resource permission of user
        let user = tonic_internal!(
            self.database_handler
                .update_permission_from_user(user_id, resource_id, permission)
                .await,
            "Permission update failed"
        );

        // Return found authorizations
        let response = UpdateAuthorizationResponse {
            user_permission: Some(UserPermission {
                user_id: user_id.to_string(),
                user_name: user.display_name,
                permission_level: inner_request.permission_level,
            }),
        };
        return_with_log!(response);
    }
}
