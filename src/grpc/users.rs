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
use crate::utils::conversions::{as_api_token, convert_token_to_proto, get_token_from_md};
use anyhow::anyhow;

use aruna_rust_api::api::dataproxy::services::v2::dataproxy_user_service_client::DataproxyUserServiceClient;
use aruna_rust_api::api::dataproxy::services::v2::GetCredentialsRequest;
use aruna_rust_api::api::storage::models::v2::context::Context as ProtoContext;
use aruna_rust_api::api::storage::services::v2::get_endpoint_request::Endpoint as ApiEndpointEnum;
use aruna_rust_api::api::storage::services::v2::user_service_server::UserService;
use aruna_rust_api::api::storage::services::v2::{
    AcknowledgePersonalNotificationsRequest, AcknowledgePersonalNotificationsResponse,
    ActivateUserRequest, ActivateUserResponse, AddOidcProviderRequest, AddOidcProviderResponse,
    CreateApiTokenRequest, CreateApiTokenResponse, DeactivateUserRequest, DeactivateUserResponse,
    DeleteApiTokenRequest, DeleteApiTokenResponse, DeleteApiTokensRequest, DeleteApiTokensResponse,
    GetAllUsersRequest, GetAllUsersResponse, GetApiTokenRequest, GetApiTokenResponse,
    GetApiTokensRequest, GetApiTokensResponse, GetDataproxyTokenUserRequest,
    GetDataproxyTokenUserResponse, GetEndpointRequest, GetNotActivatedUsersRequest,
    GetNotActivatedUsersResponse, GetPersonalNotificationsRequest,
    GetPersonalNotificationsResponse, GetS3CredentialsUserRequest, GetS3CredentialsUserResponse,
    GetUserRedactedRequest, GetUserRedactedResponse, GetUserRequest, GetUserResponse,
    RegisterUserRequest, RegisterUserResponse, RemoveOidcProviderRequest,
    RemoveOidcProviderResponse, UpdateUserDisplayNameRequest, UpdateUserDisplayNameResponse,
    UpdateUserEmailRequest, UpdateUserEmailResponse,
};
use diesel_ulid::DieselUlid;
use std::str::FromStr;
use std::sync::Arc;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue};
use tonic::transport::Channel;
use tonic::transport::ClientTlsConfig;
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

        // Validate display name and email is filled
        if request.get_display_name().is_empty() {
            return Err(tonic::Status::invalid_argument("Display name is mandatory"));
        } else if request.get_email().is_empty() {
            return Err(tonic::Status::invalid_argument("Email is mandatory"));
        }

        let external_id = tonic_auth!(
            self.authorizer.check_unregistered_oidc(&token).await,
            "Unauthorized"
        );
        let (user_id, new_user) = tonic_internal!(
            self.database_handler
                .register_user(request, external_id)
                .await,
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
            token_secret,
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
            Some(token) => Some(as_api_token(token_id, token.clone())),
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
        );
        let user = tonic_invalid!(
            self.cache
                .get_user(&user_id)
                .ok_or_else(|| anyhow!("Not found")),
            "User not found"
        );
        let tokens = Vec::from_iter(
            user.attributes
                .0
                .tokens
                .into_iter()
                .map(|t| as_api_token(t.0, t.1)),
        );
        let response = GetApiTokensResponse { tokens };

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
        );
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
            // If admin requests users this gets a user context scope
            (Some(id), ctx) => {
                self.authorizer.check_permissions(&token, vec![ctx]).await?;
                id
            }
            // If user requests himself, this is a self context
            (None, ctx) => self.authorizer.check_permissions(&token, vec![ctx]).await?,
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
        tonic_auth!(
            self.authorizer
                .check_permissions(
                    &token,
                    vec![Context::user_ctx(
                        tonic_invalid!(
                            DieselUlid::from_str(&request.get_ref().user_id),
                            "Invalid user_id"
                        ),
                        DbPermissionLevel::READ
                    )]
                )
                .await,
            "Unauthorized"
        );
        let user_ulid = tonic_invalid!(
            DieselUlid::from_str(&request.get_ref().user_id),
            "Invalid user_id"
        );
        let user = self.cache.get_user(&user_ulid);
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
        let user = self.cache.get_all_users_proto().await;

        let response = GetAllUsersResponse { user };
        return_with_log!(response);
    }

    ///ToDo: Rust Doc
    async fn get_s3_credentials_user(
        &self,
        request: Request<GetS3CredentialsUserRequest>,
    ) -> Result<Response<GetS3CredentialsUserResponse>, Status> {
        log_received!(&request);

        // Consume gRPC request into its parts
        let (metadata, _, inner_request) = request.into_parts();

        // Check empty context if is registered user
        let token = tonic_auth!(get_token_from_md(&metadata), "Token authentication error");
        let (user_id, maybe_token, _) = tonic_auth!(
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
        let user = self
            .cache
            .get_user(&user_id)
            .ok_or_else(|| Status::not_found("User not found"))?;
        // Service accounts are not allowed to get additional trusted endpoints
        if user.attributes.0.service_account
            && !user
                .attributes
                .0
                .trusted_endpoints
                .contains_key(&endpoint_ulid)
        {
            return Err(Status::unauthenticated(
                "Service accounts are not allowed to add non-predefined endpoints",
            ));
        }

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
                maybe_token.map(|token_id| token_id.to_string()), // Token_Id of user token; None if OIDC
                Some(Intent {
                    target: endpoint_ulid,
                    action: Action::CreateSecrets
                }),
            ),
            "Token signing failed"
        );

        let user = tonic_internal!(
            self.database_handler
                .add_endpoint_to_user(user_id, endpoint.id)
                .await,
            "Failed to add endpoint to user"
        );

        self.cache.update_user(&user_id, user);

        // Request S3 credentials from Dataproxy
        let mut endpoint_host_url: String = String::new();
        let mut endpoint_s3_url: String = String::new();
        for endpoint_config in endpoint.host_config.0 .0 {
            match endpoint_config.feature {
                DataProxyFeature::GRPC => endpoint_host_url = endpoint_config.url,
                DataProxyFeature::S3 => endpoint_s3_url = endpoint_config.url,
            }
            if !endpoint_s3_url.is_empty() && !endpoint_host_url.is_empty() {
                break;
            }
        }

        // Check if dataproxy host url is tls
        let dp_endpoint = if endpoint_host_url.starts_with("https") {
            Channel::from_shared(endpoint_host_url)
                .map_err(|_| Status::internal("Could not connect to Dataproxy"))?
                .tls_config(ClientTlsConfig::new())
                .map_err(|_| Status::internal("Could not connect to Dataproxy"))?
        } else {
            Channel::from_shared(endpoint_host_url)
                .map_err(|_| Status::internal("Could not connect to Dataproxy"))?
        };

        let mut dp_conn = tonic_internal!(
            DataproxyUserServiceClient::connect(dp_endpoint).await,
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
            s3_endpoint_url: endpoint_s3_url.to_string(),
        };
        return_with_log!(response);
    }

    async fn get_dataproxy_token_user(
        &self,
        request: Request<GetDataproxyTokenUserRequest>,
    ) -> Result<Response<GetDataproxyTokenUserResponse>, Status> {
        log_received!(&request);

        // Consume gRPC request into its parts
        let (metadata, _, inner_request) = request.into_parts();

        // Check empty context if is registered user
        let token = tonic_auth!(get_token_from_md(&metadata), "Token authentication error");
        let (user_ulid, maybe_token, proxy_request) = tonic_auth!(
            self.authorizer
                .check_permissions_verbose(&token, vec![Context::default()])
                .await,
            "Unauthorized"
        );

        // Create token based on provided context
        let response_token = if let Some(context) = inner_request.context {
            match context.context {
                Some(con) => {
                    match con {
                        ProtoContext::S3Credentials(_) => {
                            tonic_internal!(
                                self.authorizer.token_handler.sign_dataproxy_slt(
                                    &user_ulid,
                                    maybe_token.map(|token_id| token_id.to_string()), // Token_Id of user token; None if OIDC
                                    Some(Intent {
                                        target: tonic_invalid!(
                                            DieselUlid::from_str(&inner_request.endpoint_id),
                                            "Invalid endpoint id format"
                                        ),
                                        action: Action::CreateSecrets
                                    }),
                                ),
                                "Token signing failed"
                            )
                        }
                        ProtoContext::Copy(_) => {
                            unimplemented!(
                                "Dataproxy data replication token creation not yet implemented"
                            )
                        }
                    }
                }
                None => return Err(Status::invalid_argument("Missing context action")),
            }
        } else if proxy_request {
            return Err(Status::invalid_argument("No context provided"));
        } else if let Ok(endpoint_ulid) = DieselUlid::from_str(&inner_request.endpoint_id) {
            tonic_internal!(
                self.authorizer.token_handler.sign_dataproxy_slt(
                    &user_ulid,
                    maybe_token.map(|token_id| token_id.to_string()), // Token_Id of user token; None if OIDC
                    Some(Intent {
                        target: endpoint_ulid,
                        action: Action::All
                    }),
                ),
                "Token signing failed"
            )
        } else {
            tonic_internal!(
                self.authorizer.token_handler.sign_dataproxy_slt(
                    &user_ulid,
                    maybe_token.map(|token_id| token_id.to_string()), // Token_Id of user token; None if OIDC
                    None,
                ),
                "Token signing failed"
            )
        };

        // Return token to user
        let response = GetDataproxyTokenUserResponse {
            token: response_token,
        };
        return_with_log!(response);
    }

    async fn get_personal_notifications(
        &self,
        request: Request<GetPersonalNotificationsRequest>,
    ) -> tonic::Result<Response<GetPersonalNotificationsResponse>> {
        log_received!(&request);

        // Consume gRPC request into its parts
        let (request_metadata, _, _) = request.into_parts();

        // Extract token from request and check permissions
        let token = tonic_auth!(
            get_token_from_md(&request_metadata),
            "Token authentication error"
        );

        let ctx = Context::self_ctx();
        let token_user_ulid = tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        // Fetch personal notifications from database
        let notifications = tonic_internal!(
            self.database_handler
                .get_persistent_notifications(token_user_ulid)
                .await,
            "Failed to fetch personal notifications"
        );

        // Return personal notifications
        let response = GetPersonalNotificationsResponse { notifications };
        return_with_log!(response);
    }

    async fn acknowledge_personal_notifications(
        &self,
        request: Request<AcknowledgePersonalNotificationsRequest>,
    ) -> tonic::Result<Response<AcknowledgePersonalNotificationsResponse>> {
        log_received!(&request);

        // Consume gRPC request into its parts
        let (request_metadata, _, inner_request) = request.into_parts();

        // Extract token from request and check permissions
        let token = tonic_auth!(
            get_token_from_md(&request_metadata),
            "Token authentication error"
        );

        let ctx = Context::self_ctx();
        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        // Acknowledge personal notifications in database
        tonic_internal!(
            self.database_handler
                .acknowledge_persistent_notifications(inner_request.notification_ids)
                .await,
            "Failed to acknowledge personal notifications"
        );

        // Return empty response on success
        let response = AcknowledgePersonalNotificationsResponse {};
        return_with_log!(response);
    }

    async fn add_oidc_provier(
        &self,
        _request: tonic::Request<AddOidcProviderRequest>,
    ) -> std::result::Result<tonic::Response<AddOidcProviderResponse>, tonic::Status> {
        todo!()
    }
    async fn remove_oidc_provider(
        &self,
        _request: tonic::Request<RemoveOidcProviderRequest>,
    ) -> std::result::Result<tonic::Response<RemoveOidcProviderResponse>, tonic::Status> {
        todo!()
    }
}
