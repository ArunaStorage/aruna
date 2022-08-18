use tonic::Response;

use super::authz::{Authz, Context};

use crate::api::aruna::api::storage::services::v1::user_service_server::UserService;
use crate::api::aruna::api::storage::services::v1::*;
use crate::database::connection::Database;
use crate::database::models::enums::{Resources, UserRights};

use std::sync::Arc;

pub struct UserServiceImpl {
    database: Arc<Database>,
    authz: Arc<Authz>,
}

impl UserServiceImpl {
    pub async fn new(db: Arc<Database>, authz: Arc<Authz>) -> Self {
        UserServiceImpl {
            database: db,
            authz,
        }
    }
}

#[tonic::async_trait]
impl UserService for UserServiceImpl {
    /// RegisterUser registers a new user from OIDC
    async fn register_user(
        &self,
        request: tonic::Request<RegisterUserRequest>,
    ) -> Result<tonic::Response<RegisterUserResponse>, tonic::Status> {
        let subject_id = self.authz.validate_oidc_only(request.metadata()).await?;
        Ok(Response::new(
            self.database
                .register_user(request.into_inner(), subject_id)?,
        ))
    }
    /// CreateAPIToken Creates an API token to authenticate
    async fn create_api_token(
        &self,
        request: tonic::Request<CreateApiTokenRequest>,
    ) -> Result<tonic::Response<CreateApiTokenResponse>, tonic::Status> {
        if Authz::is_oidc_from_metadata(request.metadata()).await? {
            let user_subject = self
                .authz
                .validate_and_query_token(request.metadata())
                .await?;

            let result = self.database.create_api_token(
                request.into_inner(),
                user_subject,
                self.authz.get_decoding_serial().await,
            );

            return Ok(Response::new(result?));
        } else {
            let user_id = self
                .authz
                .authorize(
                    request.metadata(),
                    Context {
                        user_right: UserRights::READ,
                        resource_type: Resources::PROJECT,
                        resource_id: uuid::Uuid::default(),
                        admin: false,
                        personal: true,
                        oidc_context: false,
                    },
                )
                .await?;
            let result = self.database.create_api_token(
                request.into_inner(),
                user_id,
                self.authz.get_decoding_serial().await,
            );

            return Ok(Response::new(result?));
        }
    }
    /// Returns one API token by id
    async fn get_api_token(
        &self,
        request: tonic::Request<GetApiTokenRequest>,
    ) -> Result<tonic::Response<GetApiTokenResponse>, tonic::Status> {
        let user_id = self
            .authz
            .authorize(
                request.metadata(),
                Context {
                    user_right: UserRights::READ,
                    resource_type: Resources::PROJECT,
                    resource_id: uuid::Uuid::default(),
                    admin: false,
                    personal: true,
                    oidc_context: false,
                },
            )
            .await?;

        Ok(Response::new(
            self.database.get_api_token(request.into_inner(), user_id)?,
        ))
    }
    /// Returns all API token for a specific user
    async fn get_api_tokens(
        &self,
        request: tonic::Request<GetApiTokensRequest>,
    ) -> Result<tonic::Response<GetApiTokensResponse>, tonic::Status> {
        let user_id = self
            .authz
            .authorize(
                request.metadata(),
                Context {
                    user_right: UserRights::READ,
                    resource_type: Resources::PROJECT,
                    resource_id: uuid::Uuid::default(),
                    admin: false,
                    personal: true,
                    oidc_context: false,
                },
            )
            .await?;

        Ok(Response::new(
            self.database
                .get_api_tokens(request.into_inner(), user_id)?,
        ))
    }
    /// DeleteAPITokenRequest Deletes the specified API Token
    async fn delete_api_token(
        &self,
        request: tonic::Request<DeleteApiTokenRequest>,
    ) -> Result<tonic::Response<DeleteApiTokenResponse>, tonic::Status> {
        todo!()
    }
    /// DeleteAPITokenRequest Deletes the specified API Token
    async fn delete_api_tokens(
        &self,
        request: tonic::Request<DeleteApiTokensRequest>,
    ) -> Result<tonic::Response<DeleteApiTokensResponse>, tonic::Status> {
        todo!()
    }
    /// UserWhoAmI is a request that returns the user information of the current
    /// user
    async fn user_who_am_i(
        &self,
        request: tonic::Request<UserWhoAmIRequest>,
    ) -> Result<tonic::Response<UserWhoAmIResponse>, tonic::Status> {
        todo!()
    }
    /// UserWhoAmI is a request that returns the user information of the current
    /// user
    async fn update_user_display_name(
        &self,
        request: tonic::Request<UpdateUserDisplayNameRequest>,
    ) -> Result<tonic::Response<UpdateUserDisplayNameResponse>, tonic::Status> {
        todo!()
    }
    async fn get_user_projects(
        &self,
        request: tonic::Request<GetUserProjectsRequest>,
    ) -> Result<tonic::Response<GetUserProjectsResponse>, tonic::Status> {
        todo!()
    }
}
