use crate::auth::permission_handler::PermissionHandler;
use crate::auth::structs::Context;
use crate::caching::cache::Cache;
use crate::database::enums::DbPermissionLevel;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::user_request_types::{
    ActivateUser, DeactivateUser, RegisterUser, UpdateUserEmail, UpdateUserName,
};
use crate::utils::conversions::get_token_from_md;
use aruna_rust_api::api::storage::services::v2::user_service_server::UserService;
use aruna_rust_api::api::storage::services::v2::{
    ActivateUserRequest, ActivateUserResponse, CreateApiTokenRequest, CreateApiTokenResponse,
    DeactivateUserRequest, DeactivateUserResponse, DeleteApiTokenRequest, DeleteApiTokenResponse,
    DeleteApiTokensRequest, DeleteApiTokensResponse, GetAllUsersRequest, GetAllUsersResponse,
    GetApiTokenRequest, GetApiTokenResponse, GetApiTokensRequest, GetApiTokensResponse,
    GetDataproxyTokenUserRequest, GetDataproxyTokenUserResponse, GetNotActivatedUsersRequest,
    GetNotActivatedUsersResponse, GetS3CredentialsUserRequest, GetS3CredentialsUserResponse,
    GetUserRedactedRequest, GetUserRedactedResponse, GetUserRequest, GetUserResponse,
    RegisterUserRequest, RegisterUserResponse, UpdateUserDisplayNameRequest,
    UpdateUserDisplayNameResponse, UpdateUserEmailRequest, UpdateUserEmailResponse,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};
crate::impl_grpc_server!(UserServiceImpl);

#[tonic::async_trait]
impl UserService for UserServiceImpl {
    async fn register_user(
        &self,
        request: Request<RegisterUserRequest>,
    ) -> Result<Response<RegisterUserResponse>, Status> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = RegisterUser(request.into_inner());
        let ctx = Context::self_ctx(); //TODO: Implementation of empty ctx for user registration
        let _external_id = tonic_auth!(
            //TODO: Get external_id from token
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );
        let (user_id, new_user) = tonic_internal!(
            // TODO: Add Some(external_id) from token
            self.database_handler.register_user(request, None).await,
            "Internal register user error"
        );
        self.cache.add_user(user_id, new_user);

        let response = RegisterUserResponse {
            user_id: user_id.to_string(),
        };

        return_with_log!(response);
    }

    async fn deactivate_user(
        &self,
        request: Request<DeactivateUserRequest>,
    ) -> Result<Response<DeactivateUserResponse>, Status> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = DeactivateUser(request.into_inner());
        let ctx = Context::admin();
        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );
        let (user_id, user) = tonic_internal!(
            self.database_handler.deactivate_user(request).await,
            "Internal deactivate user error"
        );
        self.cache.update_user(&user_id, user);

        return_with_log!(DeactivateUserResponse {});
    }

    async fn activate_user(
        &self,
        request: Request<ActivateUserRequest>,
    ) -> Result<Response<ActivateUserResponse>, Status> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = ActivateUser(request.into_inner());
        let ctx = Context::admin();
        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );
        let (user_id, user) = tonic_internal!(
            self.database_handler.activate_user(request).await,
            "Internal deactivate user error"
        );
        self.cache.update_user(&user_id, user);

        return_with_log!(ActivateUserResponse {});
    }

    async fn create_api_token(
        &self,
        request: Request<CreateApiTokenRequest>,
    ) -> Result<Response<CreateApiTokenResponse>, Status> {
        todo!()
    }

    async fn get_api_token(
        &self,
        request: Request<GetApiTokenRequest>,
    ) -> Result<Response<GetApiTokenResponse>, Status> {
        todo!()
    }

    async fn get_api_tokens(
        &self,
        request: Request<GetApiTokensRequest>,
    ) -> Result<Response<GetApiTokensResponse>, Status> {
        todo!()
    }

    async fn delete_api_token(
        &self,
        request: Request<DeleteApiTokenRequest>,
    ) -> Result<Response<DeleteApiTokenResponse>, Status> {
        todo!()
    }

    async fn delete_api_tokens(
        &self,
        request: Request<DeleteApiTokensRequest>,
    ) -> Result<Response<DeleteApiTokensResponse>, Status> {
        todo!()
    }

    async fn get_user(
        &self,
        request: Request<GetUserRequest>,
    ) -> Result<Response<GetUserResponse>, Status> {
        todo!()
    }

    async fn get_user_redacted(
        &self,
        request: Request<GetUserRedactedRequest>,
    ) -> Result<Response<GetUserRedactedResponse>, Status> {
        todo!()
    }

    async fn update_user_display_name(
        &self,
        request: Request<UpdateUserDisplayNameRequest>,
    ) -> Result<Response<UpdateUserDisplayNameResponse>, Status> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = UpdateUserName(request.into_inner());
        let ctx = Context::self_ctx();
        let user_id = tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        )
        .ok_or_else(|| tonic::Status::invalid_argument("No user id found"))?;
        let user = tonic_internal!(
            self.database_handler
                .update_display_name(request, user_id)
                .await,
            "Internal deactivate user error"
        );

        self.cache.update_user(&user_id, user);

        return_with_log!(UpdateUserDisplayNameResponse {
            user: Some(user.into())
        });
    }

    async fn update_user_email(
        &self,
        request: Request<UpdateUserEmailRequest>,
    ) -> Result<Response<UpdateUserEmailResponse>, Status> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = UpdateUserEmail(request.into_inner());
        let user_id = tonic_invalid!(request.get_user(), "Invalid user id");
        let ctx = Context::user_ctx(user_id, DbPermissionLevel::WRITE);
        let user_id = tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        )
        .ok_or_else(|| tonic::Status::invalid_argument("No user id found"))?;

        let user = tonic_internal!(
            self.database_handler.update_email(request, user_id).await,
            "Internal deactivate user error"
        );
        self.cache.update_user(&user_id, user);

        return_with_log!(UpdateUserEmailResponse {
            user: Some(user.into())
        });
    }

    async fn get_not_activated_users(
        &self,
        request: Request<GetNotActivatedUsersRequest>,
    ) -> Result<Response<GetNotActivatedUsersResponse>, Status> {
        todo!()
    }

    async fn get_all_users(
        &self,
        request: Request<GetAllUsersRequest>,
    ) -> Result<Response<GetAllUsersResponse>, Status> {
        todo!()
    }

    async fn get_s3_credentials_user(
        &self,
        request: Request<GetS3CredentialsUserRequest>,
    ) -> Result<Response<GetS3CredentialsUserResponse>, Status> {
        todo!()
    }

    async fn get_dataproxy_token_user(
        &self,
        request: Request<GetDataproxyTokenUserRequest>,
    ) -> Result<Response<GetDataproxyTokenUserResponse>, Status> {
        todo!()
    }
}
