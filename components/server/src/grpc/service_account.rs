use crate::auth::permission_handler::PermissionCheck;
use crate::auth::structs::{Context, ContextVariant};
use crate::auth::token_handler::{Action, Intent, ProcessedToken};
use crate::caching::cache::Cache;
use crate::database::crud::CrudDb;
use crate::database::dsls::user_dsl::User;
use crate::database::enums::{DbPermissionLevel, ObjectType};
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::service_account_request_types::{
    AddPubkeySvcAccount, AddTrustedEndpointSvcAccount, CreateDataProxyTokenSVCAccount,
    CreateS3CredsSvcAccount, CreateServiceAccount, CreateServiceAccountToken,
    DeleteS3CredsSvcAccount, DeleteServiceAccount, DeleteServiceAccountToken,
    DeleteServiceAccountTokens, GetEndpointInteractionInfos, GetS3CredentialsSVCAccount,
    GetServiceAccountInfo, GetServiceAccountToken, GetServiceAccountTokens,
    GetTokenAndServiceAccountInfo, RemoveDataproxyAttributeSvcAccount,
    RemoveTrustedEndpointSvcAccount,
};
use crate::middlelayer::user_request_types::DeleteProxyAttributeSource;
use crate::utils::conversions::users::convert_token_to_proto;
use crate::{auth::permission_handler::PermissionHandler, utils::grpc_utils::get_token_from_md};
use aruna_rust_api::api::storage::models::v2::context::Context as ProtoContext;
use aruna_rust_api::api::storage::services::v2::{
    service_account_service_server::ServiceAccountService, AddDataProxyAttributeUserRequest,
    CreateDataproxyTokenSvcAccountRequest, CreateDataproxyTokenSvcAccountResponse,
    CreateServiceAccountRequest, CreateServiceAccountResponse, CreateServiceAccountTokenRequest,
    CreateServiceAccountTokenResponse, DeleteServiceAccountRequest, DeleteServiceAccountResponse,
    DeleteServiceAccountTokenRequest, DeleteServiceAccountTokenResponse,
    DeleteServiceAccountTokensRequest, DeleteServiceAccountTokensResponse,
    GetS3CredentialsSvcAccountRequest, GetS3CredentialsSvcAccountResponse,
    GetServiceAccountTokenRequest, GetServiceAccountTokenResponse, GetServiceAccountTokensRequest,
    GetServiceAccountTokensResponse,
};
use aruna_rust_api::api::storage::services::v2::{
    AddDataProxyAttributeSvcAccountRequest, AddDataProxyAttributeSvcAccountResponse,
    AddPubkeySvcAccountRequest, AddPubkeySvcAccountResponse, AddTrustedEndpointsSvcAccountRequest,
    AddTrustedEndpointsSvcAccountResponse, CreateS3CredentialsSvcAccountRequest,
    CreateS3CredentialsSvcAccountResponse, DeleteS3CredentialsSvcAccountRequest,
    DeleteS3CredentialsSvcAccountResponse, RemoveDataProxyAttributeSvcAccountRequest,
    RemoveDataProxyAttributeSvcAccountResponse, RemoveTrustedEndpointsSvcAccountRequest,
    RemoveTrustedEndpointsSvcAccountResponse,
};
use diesel_ulid::DieselUlid;
use std::sync::Arc;
use tonic::{Request, Response, Result, Status};

crate::impl_grpc_server!(ServiceAccountServiceImpl);

#[tonic::async_trait]
impl ServiceAccountService for ServiceAccountServiceImpl {
    async fn create_service_account(
        &self,
        request: Request<CreateServiceAccountRequest>,
    ) -> Result<Response<CreateServiceAccountResponse>> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = CreateServiceAccount(request.into_inner());
        let (id, perm) = tonic_invalid!(request.get_permissions(), "Invalid request");
        if self
            .cache
            .get_object(&id)
            .ok_or_else(|| Status::not_found("Project not found"))?
            .object
            .object_type
            != ObjectType::PROJECT
        {
            return Err(Status::invalid_argument("Id does not match any projects"));
        }
        let ctx = vec![
            // Only Resource/Project Admins should be able to do this
            Context::res_ctx(id, DbPermissionLevel::ADMIN, false),
            // Admin must have matching permissions, in case a different permisson is given
            Context::res_ctx(id, perm.into_inner(), false),
        ];
        tonic_auth!(
            self.authorizer.check_permissions(&token, ctx).await,
            "Unauthorized"
        );
        let service_account = tonic_internal!(
            self.database_handler.create_service_account(request).await,
            "Internal create service account error"
        );
        let response = CreateServiceAccountResponse {
            service_account: Some(tonic_internal!(
                service_account.try_into(),
                "User conversion error"
            )),
        };
        return_with_log!(response);
    }
    async fn create_service_account_token(
        &self,
        request: Request<CreateServiceAccountTokenRequest>,
    ) -> Result<Response<CreateServiceAccountTokenResponse>> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = CreateServiceAccountToken(request.into_inner());
        let (id, perm) = tonic_invalid!(request.get_permissions(), "Invalid permissions provided");
        let ctx = Context::res_ctx(id, perm.into_inner(), false);
        tonic_auth!(
            // Server API tokens can only be created by admins
            self.authorizer
                .check_permissions(
                    &token,
                    vec![Context::res_ctx(id, DbPermissionLevel::ADMIN, false), ctx]
                )
                .await,
            "Unauthorized"
        );
        let (token, token_secret) = tonic_internal!(
            self.database_handler
                .create_service_account_token(self.authorizer.clone(), request)
                .await,
            "Internal create service account error"
        );
        let response = CreateServiceAccountTokenResponse {
            token,
            token_secret,
        };
        return_with_log!(response);
    }

    async fn get_service_account_token(
        &self,
        request: Request<GetServiceAccountTokenRequest>,
    ) -> Result<Response<GetServiceAccountTokenResponse>> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let client = tonic_internal!(
            self.database_handler.database.get_client().await,
            "Could not create database client"
        );
        let request = GetServiceAccountToken(request.into_inner());
        let (_, token_id) = tonic_invalid!(request.get_ids(), "Invalid id provided");
        let service_account = tonic_invalid!(
            request.get_service_account(&client).await,
            "Invalid request"
        );
        let ctx = tonic_internal!(
            GetServiceAccountToken::get_context(&service_account),
            "Error retrieving permissions"
        );
        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );
        let token = service_account
            .attributes
            .0
            .tokens
            .get(&token_id)
            .ok_or_else(|| Status::not_found("Token not found"))?;
        let (token_id, token) = token.pair();
        let token = Some(convert_token_to_proto(token_id, token.clone()));
        let response = GetServiceAccountTokenResponse { token };
        return_with_log!(response);
    }
    async fn get_service_account_tokens(
        &self,
        request: Request<GetServiceAccountTokensRequest>,
    ) -> Result<Response<GetServiceAccountTokensResponse>> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let client = tonic_internal!(
            self.database_handler.database.get_client().await,
            "Could not create database client"
        );
        let request = GetServiceAccountTokens(request.into_inner());
        let service_account = tonic_invalid!(
            request.get_service_account(&client).await,
            "Invalid request"
        );
        let ctx = tonic_internal!(
            GetServiceAccountTokens::get_context(&service_account),
            "Error retrieving permissions"
        );
        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );
        let tokens = service_account
            .attributes
            .0
            .tokens
            .iter()
            .map(|token_map| {
                let (id, token) = token_map.pair();
                convert_token_to_proto(id, token.clone())
            })
            .collect();
        let response = GetServiceAccountTokensResponse { tokens };
        return_with_log!(response);
    }

    async fn delete_service_account_token(
        &self,
        request: Request<DeleteServiceAccountTokenRequest>,
    ) -> Result<Response<DeleteServiceAccountTokenResponse>> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = DeleteServiceAccountToken(request.into_inner());
        let client = tonic_internal!(
            self.database_handler.database.get_client().await,
            "Could not create database client"
        );
        let service_account = tonic_invalid!(
            request.get_service_account(&client).await,
            "Invalid request"
        );
        let ctx = tonic_internal!(
            DeleteServiceAccountToken::get_context(&service_account),
            "Error retrieving permissions"
        );
        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );
        tonic_internal!(
            self.database_handler
                .delete_service_account_token(request)
                .await,
            "Error while deleting service account"
        );
        return_with_log!(DeleteServiceAccountTokenResponse {});
    }
    async fn delete_service_account_tokens(
        &self,
        request: Request<DeleteServiceAccountTokensRequest>,
    ) -> Result<Response<DeleteServiceAccountTokensResponse>> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = DeleteServiceAccountTokens(request.into_inner());
        let client = tonic_internal!(
            self.database_handler.database.get_client().await,
            "Could not create database client"
        );
        let service_account = tonic_invalid!(
            request.get_service_account(&client).await,
            "Invalid request"
        );
        let ctx = tonic_internal!(
            DeleteServiceAccountTokens::get_context(&service_account),
            "Error retrieving permissions"
        );
        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );
        tonic_internal!(
            self.database_handler
                .delete_service_account_tokens(request)
                .await,
            "Error while deleting service account"
        );
        return_with_log!(DeleteServiceAccountTokensResponse {});
    }
    async fn delete_service_account(
        &self,
        request: Request<DeleteServiceAccountRequest>,
    ) -> Result<Response<DeleteServiceAccountResponse>> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = DeleteServiceAccount(request.into_inner());
        let client = tonic_internal!(
            self.database_handler.database.get_client().await,
            "Could not create database client"
        );
        let service_account = tonic_invalid!(
            request.get_service_account(&client).await,
            "Invalid request"
        );
        let ctx = tonic_internal!(
            DeleteServiceAccount::get_context(&service_account),
            "Error retrieving permissions"
        );
        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );
        tonic_internal!(
            self.database_handler.delete_service_account(request).await,
            "Error while deleting service account"
        );
        return_with_log!(DeleteServiceAccountResponse {});
    }
    async fn get_s3_credentials_svc_account(
        &self,
        request: Request<GetS3CredentialsSvcAccountRequest>,
    ) -> Result<Response<GetS3CredentialsSvcAccountResponse>> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let request = GetS3CredentialsSVCAccount(request.into_inner());
        let (service_account, endpoint_id, token) =
            self.authorize_service_account(token, request).await?;

        let (s3_access_key, s3_secret_key, s3_endpoint_url) = tonic_internal!(
            self.database_handler
                .get_s3_credentials(
                    service_account.id,
                    token,
                    endpoint_id.to_string(),
                    &self.authorizer.token_handler
                )
                .await,
            "Internal error"
        );
        let response = GetS3CredentialsSvcAccountResponse {
            s3_access_key,
            s3_secret_key,
            s3_endpoint_url,
        };
        return_with_log!(response);
    }
    async fn create_dataproxy_token_svc_account(
        &self,
        request: Request<CreateDataproxyTokenSvcAccountRequest>,
    ) -> Result<Response<CreateDataproxyTokenSvcAccountResponse>> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = CreateDataProxyTokenSVCAccount(request.into_inner());

        let (service_account, ep_id, token) = self
            .authorize_service_account(token, request.clone())
            .await?;

        if !service_account
            .attributes
            .0
            .trusted_endpoints
            .contains_key(&ep_id)
        {
            return Err(Status::invalid_argument("Endpoint is not trusted"));
        };

        // Create token based on provided context
        let response_token = match request.0.context {
            Some(con) => match con.context {
                Some(ProtoContext::S3Credentials(_)) => {
                    tonic_internal!(
                        self.authorizer.token_handler.sign_dataproxy_slt(
                            &service_account.id,
                            token.map(|t| t.to_string()),
                            Some(Intent {
                                target: ep_id,
                                action: Action::CreateSecrets
                            }),
                        ),
                        "Token signing failed"
                    )
                }
                Some(ProtoContext::Copy(_)) => {
                    unimplemented!("Dataproxy data replication token creation not yet implemented")
                }
                _ => return Err(Status::invalid_argument("No context provided")),
            },
            None => return Err(Status::invalid_argument("No context provided")),
        };

        // Return token to user
        let response = CreateDataproxyTokenSvcAccountResponse {
            token: response_token,
        };
        return_with_log!(response);
    }
    // !!!!! TODO!!!!!
    async fn add_pubkey_svc_account(
        &self,
        request: Request<AddPubkeySvcAccountRequest>,
    ) -> Result<Response<AddPubkeySvcAccountResponse>, Status> {
        // Should only be done by Admins
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = AddPubkeySvcAccount(request.into_inner());
        let client = tonic_internal!(
            self.database_handler.database.get_client().await,
            "Could not create database client"
        );
        let service_account = tonic_invalid!(
            request.get_service_account(&client).await,
            "Invalid request"
        );
        let ctx = tonic_internal!(
            AddPubkeySvcAccount::get_context(&service_account),
            "Error retrieving permissions"
        );
        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );
        tonic_invalid!(
            self.database_handler
                .add_pubkey_to_user(request.0.public_key, service_account.id)
                .await,
            "Invalid request"
        );
        return_with_log!(AddPubkeySvcAccountResponse {});
    }
    async fn add_trusted_endpoints_svc_account(
        &self,
        request: Request<AddTrustedEndpointsSvcAccountRequest>,
    ) -> Result<Response<AddTrustedEndpointsSvcAccountResponse>, Status> {
        //Should only be done by Admins
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = AddTrustedEndpointSvcAccount(request.into_inner());
        let client = tonic_internal!(
            self.database_handler.database.get_client().await,
            "Could not create database client"
        );
        let service_account = tonic_invalid!(
            request.get_service_account(&client).await,
            "Invalid request"
        );
        let ctx = tonic_internal!(
            AddTrustedEndpointSvcAccount::get_context(&service_account),
            "Error retrieving permissions"
        );
        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );
        tonic_invalid!(
            self.database_handler
                .add_trusted_endpoint_to_user(
                    service_account.id,
                    aruna_rust_api::api::storage::services::v2::AddTrustedEndpointsUserRequest {
                        endpoint_id: request.0.endpoint_id
                    }
                )
                .await,
            "Invalid request"
        );
        return_with_log!(AddTrustedEndpointsSvcAccountResponse {});
    }
    async fn remove_trusted_endpoints_svc_account(
        &self,
        request: Request<RemoveTrustedEndpointsSvcAccountRequest>,
    ) -> Result<Response<RemoveTrustedEndpointsSvcAccountResponse>, Status> {
        // Should only be possible for Admins
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = RemoveTrustedEndpointSvcAccount(request.into_inner());
        let client = tonic_internal!(
            self.database_handler.database.get_client().await,
            "Could not create database client"
        );
        let service_account = tonic_invalid!(
            request.get_service_account(&client).await,
            "Invalid request"
        );
        let ctx = tonic_internal!(
            RemoveTrustedEndpointSvcAccount::get_context(&service_account),
            "Error retrieving permissions"
        );
        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );
        tonic_invalid!(
            self.database_handler
                .remove_trusted_endpoint_from_user(
                    service_account.id,
                    aruna_rust_api::api::storage::services::v2::RemoveTrustedEndpointsUserRequest {
                        endpoint_id: request.0.endpoint_id
                    }
                )
                .await,
            "Invalid request"
        );
        return_with_log!(RemoveTrustedEndpointsSvcAccountResponse {});
    }
    async fn add_data_proxy_attribute_svc_account(
        &self,
        request: Request<AddDataProxyAttributeSvcAccountRequest>,
    ) -> Result<Response<AddDataProxyAttributeSvcAccountResponse>, Status> {
        log_received!(&request);

        // Consume gRPC request into its parts
        let (request_metadata, _, inner_request) = request.into_parts();

        // Extract token from request and check permissions
        let token = tonic_auth!(
            get_token_from_md(&request_metadata),
            "Token authentication error"
        );

        let ctx = Context::proxy();
        let PermissionCheck { proxy_id, .. } = tonic_auth!(
            self.authorizer
                .check_permissions_verbose(&token, vec![ctx])
                .await,
            "Unauthorized"
        );
        let proxy_id = if let Some(id) = proxy_id {
            id
        } else {
            return Err(Status::unauthenticated("Unauthorized"));
        };

        // Remove trusted endpoints from user
        tonic_internal!(
            self.database_handler
                .add_data_proxy_attribute(
                    AddDataProxyAttributeUserRequest {
                        attribute: inner_request.attribute,
                        user_id: inner_request.svc_account_id
                    },
                    proxy_id
                )
                .await,
            "Failed to add endpoint to user"
        );

        // Return response
        return_with_log!(AddDataProxyAttributeSvcAccountResponse {});
    }
    async fn remove_data_proxy_attribute_svc_account(
        &self,
        request: Request<RemoveDataProxyAttributeSvcAccountRequest>,
    ) -> Result<Response<RemoveDataProxyAttributeSvcAccountResponse>, Status> {
        log_received!(&request);

        // Consume gRPC request into its parts
        let (request_metadata, _, inner_request) = request.into_parts();

        let request = RemoveDataproxyAttributeSvcAccount(inner_request);

        // Extract token from request and check permissions
        let token = tonic_auth!(
            get_token_from_md(&request_metadata),
            "Token authentication error"
        );
        let ProcessedToken {
            main_id, is_proxy, ..
        } = tonic_auth!(
            self.authorizer.token_handler.process_token(&token).await,
            "Unauthorized"
        );

        let id = if is_proxy {
            let ctx = Context::proxy();
            tonic_auth!(
                self.authorizer.check_permissions(&token, vec![ctx]).await,
                "Unauthorized"
            );
            DeleteProxyAttributeSource::Proxy(main_id)
        } else {
            let client = tonic_internal!(
                self.database_handler.database.get_client().await,
                "Could not create database client"
            );
            let service_account = tonic_invalid!(
                request.get_service_account(&client).await,
                "Invalid request"
            );
            let ctx = tonic_internal!(
                RemoveTrustedEndpointSvcAccount::get_context(&service_account),
                "Error retrieving permissions"
            );
            tonic_auth!(
                self.authorizer.check_permissions(&token, vec![ctx]).await,
                "Unauthorized"
            );
            DeleteProxyAttributeSource::User(service_account.id)
        };

        // Remove trusted endpoints from user
        tonic_internal!(
            self.database_handler
                .rm_data_proxy_attribute(request.get_user_request(), id)
                .await,
            "Failed to add endpoint to user"
        );

        // Return response
        return_with_log!(RemoveDataProxyAttributeSvcAccountResponse {});
    }
    async fn create_s3_credentials_svc_account(
        &self,
        request: Request<CreateS3CredentialsSvcAccountRequest>,
    ) -> Result<Response<CreateS3CredentialsSvcAccountResponse>> {
        //TODO: Should be allowed as a self service for service accounts
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = CreateS3CredsSvcAccount(request.into_inner());
        let (service_account, endpoint_id, token) =
            self.authorize_service_account(token, request).await?;

        // Also checks if user is service_account and if endpoint is in trusted endpoints
        let (s3_access_key, s3_secret_key, s3_endpoint_url) = tonic_internal!(
            self.database_handler
                .create_s3_credentials_with_user_token(
                    service_account.id,
                    endpoint_id.to_string(),
                    token,
                    &self.authorizer.token_handler
                )
                .await,
            "Internal error"
        );
        let response = CreateS3CredentialsSvcAccountResponse {
            s3_access_key,
            s3_secret_key,
            s3_endpoint_url,
        };
        return_with_log!(response);
    }
    async fn delete_s3_credentials_svc_account(
        &self,
        request: Request<DeleteS3CredentialsSvcAccountRequest>,
    ) -> Result<Response<DeleteS3CredentialsSvcAccountResponse>> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = DeleteS3CredsSvcAccount(request.into_inner());
        let (service_account, endpoint_id, token) =
            self.authorize_service_account(token, request).await?;

        tonic_internal!(
            self.database_handler
                .delete_s3_credentials_with_user_token(
                    service_account.id,
                    endpoint_id.to_string(),
                    token,
                    &self.authorizer.token_handler
                )
                .await,
            "Internal error"
        );
        return_with_log!(DeleteS3CredentialsSvcAccountResponse {});
    }
}
impl ServiceAccountServiceImpl {
    async fn authorize_service_account<T: GetEndpointInteractionInfos>(
        &self,
        token: String,
        request: T,
    ) -> Result<(User, DieselUlid, Option<DieselUlid>)> {
        let ProcessedToken { main_id, .. } = tonic_auth!(
            self.authorizer.token_handler.process_token(&token).await,
            "Unauthorized"
        );
        let is_self = self
            .cache
            .get_user(&main_id)
            .map(|user| user.attributes.0.service_account)
            .unwrap_or(false);
        let client = tonic_internal!(
            self.database_handler.database.get_client().await,
            "Could not create database client"
        );
        let (service_account_id, endpoint_id) =
            tonic_invalid!(request.get_ids(), "Invalid ids provided");

        let (service_account, token) = if !is_self {
            let service_account = tonic_invalid!(
                GetS3CredentialsSVCAccount::get_service_account(&service_account_id, &client).await,
                "Invalid request"
            );

            let (res_id, _) = tonic_internal!(
                CreateDataProxyTokenSVCAccount::get_permissions(&service_account),
                "Error retrieving permissions"
            );
            tonic_auth!(
                self.authorizer
                    .check_permissions(
                        &token,
                        vec![Context::res_ctx(res_id, DbPermissionLevel::ADMIN, false)]
                    )
                    .await,
                "Unauthorized"
            );
            (service_account, None)
        } else {
            let ctx = Context {
                variant: ContextVariant::SelfUser,
                allow_service_account: true,
                is_self: true,
            };
            let PermissionCheck {
                user_id: service_account,
                token,
                ..
            } = tonic_auth!(
                self.authorizer
                    .check_permissions_verbose(&token, vec![ctx])
                    .await,
                "Unauthorized"
            );
            if service_account != service_account_id {
                return Err(Status::unauthenticated("Unauthorized"));
            }
            (
                tonic_internal!(
                    User::get(service_account, &client).await,
                    "Internal database error"
                )
                .ok_or_else(|| Status::not_found("Service account not found"))?,
                token,
            )
        };
        Ok((service_account, endpoint_id, token))
    }
}
