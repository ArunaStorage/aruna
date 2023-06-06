use super::authz::Authz;
use crate::database::connection::Database;
use crate::error::ArunaError;
use aruna_rust_api::api::internal::v1::{
    internal_authorize_service_server::InternalAuthorizeService, Authorization, AuthorizeRequest,
    AuthorizeResponse, GetSecretRequest, GetSecretResponse, GetTokenFromSecretRequest,
    GetTokenFromSecretResponse,
};
use std::time::Duration;
use std::{str::FromStr, time::SystemTime};

use chrono::Utc;
use std::sync::Arc;
use tokio::task;
use tonic::Code;

// This macro automatically creates the Impl struct with all associated fields
crate::impl_grpc_server!(InternalAuthorizeServiceImpl);

#[tonic::async_trait]
impl InternalAuthorizeService for InternalAuthorizeServiceImpl {
    async fn authorize(
        &self,
        _request: tonic::Request<AuthorizeRequest>,
    ) -> Result<tonic::Response<AuthorizeResponse>, tonic::Status> {
        return Err(tonic::Status::unimplemented("In development"));
    }

    /// Re-generate the access secret associated with the provided access key.
    ///
    /// ## Arguments:
    ///
    ///
    /// ## Returns:
    ///
    ///
    /// ## Behaviour:
    ///
    async fn get_secret(
        &self,
        request: tonic::Request<GetSecretRequest>,
    ) -> Result<tonic::Response<GetSecretResponse>, tonic::Status> {
        // Consume gRPC request
        let inner_request = request.into_inner();

        // Extract AccessKey which is (everytime?) the token id
        let token_id = diesel_ulid::DieselUlid::from_str(&inner_request.accesskey)
            .map_err(ArunaError::from)?;

        // Fetch token from database only by its id
        let database_clone = self.database.clone();
        let api_token = task::spawn_blocking(move || database_clone.get_api_token_by_id(&token_id))
            .await
            .map_err(ArunaError::from)??;

        // Check if token is not expired
        if api_token
            .expires_at
            .ok_or_else(|| {
                tonic::Status::new(
                    Code::InvalidArgument,
                    format!("Token {token_id} has no expiry date"),
                )
            })?
            .timestamp()
            < Utc::now().timestamp()
        {
            return Err(tonic::Status::new(
                Code::InvalidArgument,
                format!("Token {token_id} is expired"),
            ));
        }

        // Return gRPC response
        Ok(tonic::Response::new(GetSecretResponse {
            authorization: Some(Authorization {
                secretkey: api_token.secretkey,
                accesskey: api_token.id.to_string(),
            }),
        }))
    }

    /// Generates a short-lived token for authorization from s3 access credentials
    ///
    /// ## Arguments:
    ///
    ///
    /// ## Returns:
    ///
    ///
    /// ## Behaviour:
    ///
    async fn get_token_from_secret(
        &self,
        request: tonic::Request<GetTokenFromSecretRequest>,
    ) -> Result<tonic::Response<GetTokenFromSecretResponse>, tonic::Status> {
        let inner_request = request.into_inner();

        let token_id = &inner_request
            .authorization
            .ok_or_else(|| {
                tonic::Status::new(Code::Unauthenticated, format!("Missing access key"))
            })?
            .accesskey;

        let authz_clone = self.authz.clone();

        let expiry = Some(
            SystemTime::now()
                .checked_add(Duration::new(300, 0))
                .ok_or_else(|| {
                    tonic::Status::new(Code::Internal, format!("Set system time error"))
                })?
                .into(),
        );
        let api_token = task::spawn_blocking(move || authz_clone.sign_new_token(&token_id, expiry))
            .await
            .map_err(ArunaError::from)?
            .await?;

        // Return gRPC response
        Ok(tonic::Response::new(GetTokenFromSecretResponse {
            token: api_token,
        }))
    }
}
