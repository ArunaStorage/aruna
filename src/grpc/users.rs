use crate::auth::permission_handler::PermissionHandler;
use crate::auth::structs::Context;
use crate::auth::token_handler::{Action, Intent, TokenHandler};
use crate::caching::cache::Cache;
use crate::database::enums::{DataProxyFeature, DbPermissionLevel};
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::endpoints_request_types::GetEP;
use crate::middlelayer::token_request_types::{CreateToken, DeleteToken, GetToken};
use crate::middlelayer::user_request_types::{
    ActivateUser, DeactivateUser, GetUser, RegisterUser, UpdateUserEmail, UpdateUserName,
};
use crate::utils::conversions::{convert_token_to_proto, get_token_from_md, into_api_token};
use anyhow::anyhow;

use aruna_rust_api::api::dataproxy::services::v2::dataproxy_user_service_client::DataproxyUserServiceClient;
use aruna_rust_api::api::dataproxy::services::v2::GetCredentialsRequest;
use aruna_rust_api::api::storage::services::v2::get_endpoint_request::Endpoint as ApiEndpointEnum;
use aruna_rust_api::api::storage::services::v2::user_service_server::UserService;
use aruna_rust_api::api::storage::services::v2::{
    ActivateUserRequest, ActivateUserResponse, CreateApiTokenRequest, CreateApiTokenResponse,
    DeactivateUserRequest, DeactivateUserResponse, DeleteApiTokenRequest, DeleteApiTokenResponse,
    DeleteApiTokensRequest, DeleteApiTokensResponse, GetAllUsersRequest, GetAllUsersResponse,
    GetApiTokenRequest, GetApiTokenResponse, GetApiTokensRequest, GetApiTokensResponse,
    GetDataproxyTokenUserRequest, GetDataproxyTokenUserResponse, GetEndpointRequest,
    GetNotActivatedUsersRequest, GetNotActivatedUsersResponse, GetS3CredentialsUserRequest,
    GetS3CredentialsUserResponse, GetUserRedactedRequest, GetUserRedactedResponse, GetUserRequest,
    GetUserResponse, RegisterUserRequest, RegisterUserResponse, UpdateUserDisplayNameRequest,
    UpdateUserDisplayNameResponse, UpdateUserEmailRequest, UpdateUserEmailResponse,
};
use diesel_ulid::DieselUlid;
use std::str::FromStr;
use std::sync::Arc;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue};
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
            "Internal activate user error"
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
        let request_token = tonic_auth!(get_token_from_md(&metadata), "Token authentication error");
        let user_id = tonic_auth!(
            self.authorizer
                .check_permissions(&request_token, vec![Context::default()])
                .await,
            "Unauthorized"
        );

        // Create token in database
        let middlelayer_request = CreateToken(inner_request);
        let (token_ulid, token) = tonic_internal!(
            self.database_handler
                .create_token(
                    &user_id,
                    self.token_handler.get_current_pubkey_serial() as i32,
                    middlelayer_request.clone(),
                )
                .await,
            "Token creation failed"
        );

        // Sign token
        let token_secret = tonic_internal!(
            self.token_handler.sign_user_token(
                &user_id,
                &token_ulid,
                middlelayer_request.0.expires_at,
            ),
            "Token creation failed"
        );

        // Create and return response
        let response = CreateApiTokenResponse {
            token: Some(convert_token_to_proto(&token_ulid, token)),
            token_secret: token_secret,
        };
        return_with_log!(response);
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
        );

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
        request: Request<GetApiTokensRequest>,
    ) -> Result<Response<GetApiTokensResponse>, Status> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
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
        let token = Vec::from_iter(
            user.attributes
                .0
                .tokens
                .into_iter()
                .map(|t| into_api_token(t.0, t.1)),
        );
        let response = GetApiTokensResponse { token };

        return_with_log!(response);
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
        );

        let user = tonic_internal!(
            self.database_handler.delete_token(user_id, request).await,
            "Internal database request error"
        );
        self.cache.update_user(&user_id, user);
        return_with_log!(DeleteApiTokenResponse {});
    }

    async fn delete_api_tokens(
        &self,
        request: Request<DeleteApiTokensRequest>,
    ) -> Result<Response<DeleteApiTokensResponse>, Status> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let ctx = Context::self_ctx();
        let user_id = tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        )
        .ok_or_else(|| Status::internal("User id not returned"))?;
        let user = tonic_internal!(
            self.database_handler.delete_all_tokens(user_id).await,
            "Internal database request error"
        );
        self.cache.update_user(&user_id, user);
        return_with_log!(DeleteApiTokensResponse {});
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
            ),
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
            ),
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
        );

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
        );

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

    ///ToDo: Rust Doc
    async fn get_s3_credentials_user(
        &self,
        request: Request<GetS3CredentialsUserRequest>,
        request: Request<GetS3CredentialsUserRequest>,
    ) -> Result<Response<GetS3CredentialsUserResponse>, Status> {
        log_received!(&request);

        // Consume gRPC request into its parts
        let (metadata, _, inner_request) = request.into_parts();

        // Check empty context if is registered user
        let token = tonic_auth!(get_token_from_md(&metadata), "Token authentication error");
        let (user_id, maybe_token) = tonic_auth!(
            self.authorizer
                .check_permissions_verbose(&token, vec![Context::default()])
                .await,
            "Unauthorized"
        );

        // Validate endpoint id format
        let endpoint_ulid = tonic_invalid!(
            DieselUlid::from_str(&inner_request.endpoint_id),
            "Invalid endpoint id format"
        );

        // Fetch endpoint from cache/database
        let endpoint = tonic_invalid!(
            self.database_handler
                .get_endpoint(GetEP(GetEndpointRequest {
                    endpoint: Some(ApiEndpointEnum::EndpointId(endpoint_ulid.to_string())),
                }))
                .await,
            "Could not find specified endpoint"
        );

        // Create short-lived token with intent
        let slt = tonic_internal!(
            self.authorizer.token_handler.sign_dataproxy_slt(
                &user_id,
                if let Some(token_id) = maybe_token {
                    Some(token_id.to_string())
                } else {
                    None
                }, // Token_Id of user token; None if OIDC
                Some(Intent {
                    target: endpoint_ulid,
                    action: Action::CreateSecrets
                }),
            ),
            "Token signing failed"
        );

        // Request S3 credentials from Dataproxy
        let mut endpoint_host_url: String = "".to_string();
        for endpoint_config in endpoint.host_config.0 .0 {
            if let DataProxyFeature::PROXY = endpoint_config.feature {
                endpoint_host_url = endpoint_config.url;
                break;
            }
        }

        let mut dp_conn = tonic_internal!(
            DataproxyUserServiceClient::connect(endpoint_host_url.clone()).await,
            "Could not connect to endpoint"
        );

        // Create GetCredentialsRequest with one-shot token in header ...
        let mut credentials_request = Request::new(GetCredentialsRequest {});
        credentials_request.metadata_mut().append(
            tonic_internal!(
                AsciiMetadataKey::from_bytes("Authorization".as_bytes()),
                "Request creation failed"
            ),
            tonic_internal!(
                AsciiMetadataValue::try_from(format!("Bearer {}", slt)),
                "Request creation failed"
            ),
        );

        let response = tonic_internal!(
            dp_conn.get_credentials(credentials_request).await,
            "Could not get S3 credentials from Dataproxy"
        )
        .into_inner();

        // Return S3 credentials to user
        let response = GetS3CredentialsUserResponse {
            s3_access_key: response.access_key,
            s3_secret_key: response.secret_key,
            s3_endpoint_url: endpoint_host_url.to_string(),
        };
        return_with_log!(response);
    }

    async fn get_dataproxy_token_user(
        &self,
        _request: Request<GetDataproxyTokenUserRequest>,
    ) -> Result<Response<GetDataproxyTokenUserResponse>, Status> {
        todo!()
    }
}
