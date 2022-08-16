use super::authz::Authz;
use crate::api::aruna::api::storage::services::v1::auth_service_server::AuthService;
use crate::api::aruna::api::storage::services::v1::*;
use crate::database::connection::Database;
use std::sync::Arc;

pub struct AuthServiceImpl {
    database: Arc<Database>,
    authz: Arc<Authz>,
}

impl AuthServiceImpl {
    pub async fn new(db: Arc<Database>, authz: Arc<Authz>) -> Self {
        AuthServiceImpl {
            database: db,
            authz,
        }
    }
}

#[tonic::async_trait]
impl AuthService for AuthServiceImpl {
    /// RegisterUser registers a new user from OIDC
    async fn register_user(
        &self,
        _request: tonic::Request<RegisterUserRequest>,
    ) -> Result<tonic::Response<RegisterUserResponse>, tonic::Status> {
        todo!()
    }
    /// CreateAPIToken Creates an API token to authenticate
    async fn create_api_token(
        &self,
        _request: tonic::Request<CreateApiTokenRequest>,
    ) -> Result<tonic::Response<CreateApiTokenResponse>, tonic::Status> {
        todo!()
    }
    /// Returns one API token by id
    async fn get_api_token(
        &self,
        _request: tonic::Request<GetApiTokenRequest>,
    ) -> Result<tonic::Response<GetApiTokenResponse>, tonic::Status> {
        todo!()
    }
    /// Returns all API token for a specific user
    async fn get_api_tokens(
        &self,
        _request: tonic::Request<GetApiTokensRequest>,
    ) -> Result<tonic::Response<GetApiTokensResponse>, tonic::Status> {
        todo!()
    }
    /// DeleteAPITokenRequest Deletes the specified API Token
    async fn delete_api_token(
        &self,
        _request: tonic::Request<DeleteApiTokenRequest>,
    ) -> Result<tonic::Response<DeleteApiTokenResponse>, tonic::Status> {
        todo!()
    }
    /// DeleteAPITokenRequest Deletes the specified API Token
    async fn delete_api_tokens(
        &self,
        _request: tonic::Request<DeleteApiTokensRequest>,
    ) -> Result<tonic::Response<DeleteApiTokensResponse>, tonic::Status> {
        todo!()
    }
    /// This creates a new authorization group.option
    /// All users and collections are bundled in a authorization group.
    async fn create_project(
        &self,
        _request: tonic::Request<CreateProjectRequest>,
    ) -> Result<tonic::Response<CreateProjectResponse>, tonic::Status> {
        todo!()
    }
    /// AddUserToProject Adds a new user to a given project by its id
    async fn add_user_to_project(
        &self,
        _request: tonic::Request<AddUserToProjectRequest>,
    ) -> Result<tonic::Response<AddUserToProjectResponse>, tonic::Status> {
        todo!()
    }
    /// GetProjectCollections Returns all collections that belong to a certain
    /// project
    async fn get_project_collections(
        &self,
        _request: tonic::Request<GetProjectCollectionsRequest>,
    ) -> Result<tonic::Response<GetProjectCollectionsResponse>, tonic::Status> {
        todo!()
    }
    /// GetUserCollections Returns all collections that a specified user has access
    /// to
    async fn get_user_collections(
        &self,
        _request: tonic::Request<GetUserCollectionsRequest>,
    ) -> Result<tonic::Response<GetUserCollectionsResponse>, tonic::Status> {
        todo!()
    }
    /// GetProject Returns the specified project
    async fn get_project(
        &self,
        _request: tonic::Request<GetProjectRequest>,
    ) -> Result<tonic::Response<GetProjectResponse>, tonic::Status> {
        todo!()
    }
    /// This will destroy the project and all its associated data.
    /// including users, collections, and API tokens and all data associated with
    /// them.
    async fn destroy_project(
        &self,
        _request: tonic::Request<DestroyProjectRequest>,
    ) -> Result<tonic::Response<DestroyProjectResponse>, tonic::Status> {
        todo!()
    }
}
