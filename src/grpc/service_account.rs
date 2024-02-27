use crate::auth::structs::Context;
use crate::auth::token_handler::{Action, Intent};
use crate::caching::cache::Cache;
use crate::database::enums::DbPermissionLevel;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::service_account_request_types::{
    CreateDataProxyTokenSVCAccount, CreateServiceAccount, CreateServiceAccountToken,
    DeleteServiceAccount, DeleteServiceAccountToken, DeleteServiceAccountTokens,
    GetS3CredentialsSVCAccount, GetServiceAccountToken, GetServiceAccountTokens,
    SetServiceAccountPermission,
};
use crate::utils::conversions::users::convert_token_to_proto;
use crate::{auth::permission_handler::PermissionHandler, utils::grpc_utils::get_token_from_md};
use aruna_rust_api::api::storage::models::v2::context::Context as ProtoContext;
use aruna_rust_api::api::storage::services::v2::{
    service_account_service_server::ServiceAccountService, CreateDataproxyTokenSvcAccountRequest,
    CreateDataproxyTokenSvcAccountResponse, CreateServiceAccountRequest,
    CreateServiceAccountResponse, CreateServiceAccountTokenRequest,
    CreateServiceAccountTokenResponse, DeleteServiceAccountRequest, DeleteServiceAccountResponse,
    DeleteServiceAccountTokenRequest, DeleteServiceAccountTokenResponse,
    DeleteServiceAccountTokensRequest, DeleteServiceAccountTokensResponse,
    GetS3CredentialsSvcAccountRequest, GetS3CredentialsSvcAccountResponse,
    GetServiceAccountTokenRequest, GetServiceAccountTokenResponse, GetServiceAccountTokensRequest,
    GetServiceAccountTokensResponse, SetServiceAccountPermissionRequest,
    SetServiceAccountPermissionResponse,
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
use std::sync::Arc;
use tonic::{Request, Response, Result};

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
        let ctx = vec![
            Context::res_ctx(id, DbPermissionLevel::ADMIN, false),
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
            self.authorizer.check_permissions(&token, vec![ctx]).await,
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
    async fn set_service_account_permission(
        &self,
        request: Request<SetServiceAccountPermissionRequest>,
    ) -> Result<Response<SetServiceAccountPermissionResponse>> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = SetServiceAccountPermission(request.into_inner());
        let (id, perm) = tonic_invalid!(request.get_permissions(), "Invalid permissions provided");
        let wanted_ctx = Context::res_ctx(id, perm.into_inner(), false);
        let client = tonic_internal!(
            self.database_handler.database.get_client().await,
            "Error creating database client"
        );
        let (prev_id, prev_perm) = tonic_auth!(
            request.get_previous_perms(&client).await,
            "Invalid permissions"
        );
        let prev_ctx = Context::res_ctx(prev_id, prev_perm.into_inner(), false);
        tonic_auth!(
            self.authorizer
                .check_permissions(
                    &token,
                    vec![
                        wanted_ctx,
                        prev_ctx,
                        Context::res_ctx(id, DbPermissionLevel::ADMIN, false),
                        Context::res_ctx(prev_id, DbPermissionLevel::ADMIN, false)
                    ]
                )
                .await,
            "Unauthorized"
        );

        let service_account = tonic_internal!(
            self.database_handler
                .set_service_account_permission(request)
                .await,
            "Internal create service account error"
        );
        let response = SetServiceAccountPermissionResponse {
            service_account: Some(tonic_internal!(
                service_account.try_into(),
                "User conversion error"
            )),
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
        let (_, token_id) = tonic_invalid!(request.get_ids(), "Invalid id providied");
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
            .ok_or_else(|| tonic::Status::not_found("Token not found"))?;
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
            GetServiceAccountToken::get_context(&service_account),
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
            GetServiceAccountToken::get_context(&service_account),
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
            GetServiceAccountToken::get_context(&service_account),
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
            GetServiceAccountToken::get_context(&service_account),
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
        let client = tonic_internal!(
            self.database_handler.database.get_client().await,
            "Could not create database client"
        );
        let service_account = tonic_invalid!(
            request.get_service_account(&client).await,
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

        let response = tonic_internal!(
            self.database_handler
                .get_credentials_svc_account(self.authorizer.clone(), request)
                .await,
            "Internal error"
        );
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
        let client = tonic_internal!(
            self.database_handler.database.get_client().await,
            "Could not create database client"
        );
        let service_account = tonic_invalid!(
            request.get_service_account(&client).await,
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

        // Create token based on provided context
        let (service_account_id, endpoint_id) =
            tonic_invalid!(request.get_ids(), "Invalid ids provided");
        let response_token = match request.0.context {
            Some(con) => {
                match con.context {
                    Some(ProtoContext::S3Credentials(_)) => {
                        tonic_internal!(
                            self.authorizer.token_handler.sign_dataproxy_slt(
                                &service_account_id,
                                None, // Token_Id of user token; None if OIDC
                                Some(Intent {
                                    target: endpoint_id,
                                    action: Action::CreateSecrets
                                }),
                            ),
                            "Token signing failed"
                        )
                    }
                    Some(ProtoContext::Copy(_)) => {
                        unimplemented!(
                            "Dataproxy data replication token creation not yet implemented"
                        )
                    }
                    _ => return Err(tonic::Status::invalid_argument("No context provided")),
                }
            }
            None => return Err(tonic::Status::invalid_argument("No context provided")),
        };

        tonic_internal!(
            self.database_handler
                .add_endpoint_to_user(service_account_id, endpoint_id)
                .await,
            "Failed to add endpoint to user"
        );

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
    ) -> Result<Response<AddPubkeySvcAccountResponse>, tonic::Status> {
        //TODO
        Err(tonic::Status::unimplemented(
            "Adding pubkeys is currently unimplemented",
        ))
    }
    async fn add_trusted_endpoints_svc_account(
        &self,
        request: Request<AddTrustedEndpointsSvcAccountRequest>,
    ) -> Result<Response<AddTrustedEndpointsSvcAccountResponse>, tonic::Status> {
        //TODO
        Err(tonic::Status::unimplemented(
            "Adding trusted endpoints is currently unimplemented",
        ))
    }
    async fn remove_trusted_endpoints_svc_account(
        &self,
        request: Request<RemoveTrustedEndpointsSvcAccountRequest>,
    ) -> Result<Response<RemoveTrustedEndpointsSvcAccountResponse>, tonic::Status> {
        //TODO
        Err(tonic::Status::unimplemented(
            "Removing trusted endpoints is currently unimplemented",
        ))
    }
    async fn add_data_proxy_attribute_svc_account(
        &self,
        request: Request<AddDataProxyAttributeSvcAccountRequest>,
    ) -> Result<Response<AddDataProxyAttributeSvcAccountResponse>, tonic::Status> {
        //TODO
        Err(tonic::Status::unimplemented(
            "Adding data proxy attributes is currently unimplemented",
        ))
    }
    async fn remove_data_proxy_attribute_svc_account(
        &self,
        request: Request<RemoveDataProxyAttributeSvcAccountRequest>,
    ) -> Result<Response<RemoveDataProxyAttributeSvcAccountResponse>, tonic::Status> {
        //TODO
        Err(tonic::Status::unimplemented(
            "Removing data proxy attributes is currently unimplemented",
        ))
    }
    async fn create_s3_credentials_svc_account(
        &self,
        request: Request<CreateS3CredentialsSvcAccountRequest>,
    ) -> Result<Response<CreateS3CredentialsSvcAccountResponse>> {
        Err(tonic::Status::unimplemented(
            "Creating s3 credentials for service accounts is currently unimplemented",
        ))
    }
    async fn delete_s3_credentials_svc_account(
        &self,
        request: Request<DeleteS3CredentialsSvcAccountRequest>,
    ) -> Result<Response<DeleteS3CredentialsSvcAccountResponse>> {
        Err(tonic::Status::unimplemented(
            "Deleting s3 credentials for service accounts is currently unimplemented",
        ))
    }
}
