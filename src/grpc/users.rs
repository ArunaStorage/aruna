use crate::auth::permission_handler::PermissionHandler;
use crate::auth::structs::Context;
use crate::auth::token_handler::TokenHandler;
use crate::caching::cache::Cache;
use crate::database::enums::DbPermissionLevel;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::token_request_types::{CreateToken, DeleteToken, GetToken};
use crate::middlelayer::user_request_types::{
    ActivateUser, DeactivateUser, GetUser, RegisterUser, UpdateUserEmail, UpdateUserName,
};
use crate::utils::conversions::{get_token_from_md, into_api_token};
use anyhow::anyhow;
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
crate::impl_grpc_server!(UserServiceImpl, token_handler: Arc<TokenHandler>);

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
        log_received!(&request);

        // Consume gRPC request into its parts
        let (metadata, _, inner_request) = request.into_parts();

        // Check empty context if is registered user
        let token = tonic_auth!(get_token_from_md(&metadata), "Token authentication error");
        let (user_id, _) = tonic_auth!(
            self.authorizer
                .check_permissions(&token, vec![Context::default()])
                .await,
            "Unauthorized"
        );

        // Create token
        let middlelayer_request = CreateToken(inner_request);
        let (token_ulid, token) = tonic_internal!(
            self.database_handler.create_token(
                &user_id,
                self.token_handler.get_current_pubkey_serial() as i32,
                middlelayer_request,
            ),
            "Token creation failed"
        );

        // Sign token
        let token_secret = tonic_internal!(
            self.token_handler.sign_user_token(
                &user_id,
                token_ulid,
                middlelayer_request.0.expires_at,
            ),
            "Token creation failed"
        );

        // Create and return response
        return_with_log!(CreateApiTokenResponse {
            token: token,
            token_secret: token_secret,
        });
    }

    async fn get_api_token(
        &self,
        request: Request<GetApiTokenRequest>,
    ) -> Result<Response<GetApiTokenResponse>, Status> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = GetToken(request.into_inner());
        let ctx = Context::self_ctx();
        let user_id = tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        )
        .ok_or_else(|| Status::internal("User id not returned"))?;
        let user = tonic_invalid!(
            self.cache
                .get_user(&user_id)
                .ok_or_else(|| anyhow!("Not found")),
            "User not found"
        );
        let token_id = tonic_invalid!(request.get_token_id(), "Invalid token_id");
        let token = match user.attributes.0.tokens.get(&token_id) {
            Some(token) => Some(into_api_token(token_id, token.clone())),
            None => return Err(Status::not_found("Token not found")),
        };
        let response = GetApiTokenResponse { token };

        return_with_log!(response);
    }

    async fn get_api_tokens(
        &self,
        _request: Request<GetApiTokensRequest>,
    ) -> Result<Response<GetApiTokensResponse>, Status> {
        todo!()
    }

    async fn delete_api_token(
        &self,
        request: Request<DeleteApiTokenRequest>,
    ) -> Result<Response<DeleteApiTokenResponse>, Status> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = DeleteToken(request.into_inner());
        let ctx = Context::self_ctx();
        let user_id = tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        )
        .ok_or_else(|| Status::internal("User id not returned"))?;
        let user = tonic_internal!(
            self.database_handler.delete_token(user_id, request).await,
            "Internal database request error"
        );
        self.cache.update_user(&user_id, user);
        return_with_log!(DeleteApiTokenResponse {});
    }

    async fn delete_api_tokens(
        &self,
        _request: Request<DeleteApiTokensRequest>,
    ) -> Result<Response<DeleteApiTokensResponse>, Status> {
        todo!()
    }

    async fn get_user(
        &self,
        request: Request<GetUserRequest>,
    ) -> Result<Response<GetUserResponse>, Status> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = GetUser::GetUser(request.into_inner());
        let user_id = match tonic_invalid!(request.get_user(), "Invalid user id") {
            (Some(id), ctx) => {
                tonic_auth!(
                    self.authorizer.check_permissions(&token, vec![ctx]).await,
                    "Unauthorized"
                );
                id
            }
            (None, ctx) => tonic_auth!(
                self.authorizer.check_permissions(&token, vec![ctx]).await,
                "Unauthorized"
            )
            .ok_or_else(|| Status::internal("GetUser error"))?,
        };
        let user = self.cache.get_user(&user_id);
        let response = GetUserResponse {
            user: user.map(|user| user.into()),
        };
        return_with_log!(response);
    }

    async fn get_user_redacted(
        &self,
        request: Request<GetUserRedactedRequest>,
    ) -> Result<Response<GetUserRedactedResponse>, Status> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = GetUser::GetUserRedacted(request.into_inner());
        let user_id = match tonic_invalid!(request.get_user(), "Invalid user id") {
            (Some(id), ctx) => {
                tonic_auth!(
                    self.authorizer.check_permissions(&token, vec![ctx]).await,
                    "Unauthorized"
                );
                id
            }
            (None, ctx) => tonic_auth!(
                self.authorizer.check_permissions(&token, vec![ctx]).await,
                "Unauthorized"
            )
            .ok_or_else(|| Status::internal("GetUser error"))?,
        };
        let user = self.cache.get_user(&user_id);
        let response = GetUserRedactedResponse {
            user: user.map(|user| user.into_redacted()),
        };
        return_with_log!(response);
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
        .ok_or_else(|| Status::invalid_argument("No user id found"))?;
        let user = tonic_internal!(
            self.database_handler
                .update_display_name(request, user_id)
                .await,
            "Internal deactivate user error"
        );

        self.cache.update_user(&user_id, user.clone());

        let response = UpdateUserDisplayNameResponse {
            user: Some(user.into()),
        };
        return_with_log!(response);
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
        .ok_or_else(|| Status::invalid_argument("No user id found"))?;

        let user = tonic_internal!(
            self.database_handler.update_email(request, user_id).await,
            "Internal deactivate user error"
        );
        self.cache.update_user(&user_id, user.clone());

        let response = UpdateUserEmailResponse {
            user: Some(user.into()),
        };
        return_with_log!(response);
    }

    async fn get_not_activated_users(
        &self,
        request: Request<GetNotActivatedUsersRequest>,
    ) -> Result<Response<GetNotActivatedUsersResponse>, Status> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let ctx = Context::admin();
        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let users = self.cache.get_all_deactivated().await;

        let response = GetNotActivatedUsersResponse { users };
        return_with_log!(response);
    }

    async fn get_all_users(
        &self,
        request: Request<GetAllUsersRequest>,
    ) -> Result<Response<GetAllUsersResponse>, Status> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let ctx = Context::admin();
        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );
        let user = self.cache.get_all().await;

        let response = GetAllUsersResponse { user };
        return_with_log!(response);
    }

    async fn get_s3_credentials_user(
        &self,
        _request: Request<GetS3CredentialsUserRequest>,
    ) -> Result<Response<GetS3CredentialsUserResponse>, Status> {
        todo!()
    }

    async fn get_dataproxy_token_user(
        &self,
        _request: Request<GetDataproxyTokenUserRequest>,
    ) -> Result<Response<GetDataproxyTokenUserResponse>, Status> {
        todo!()
    }
}
