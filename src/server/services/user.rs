//! This file contains the gRPC implementation for the UserService Server
//! it handle all personal interaction with users and their access tokens
//!
//! Mainly this is used to:
//! - Register a user
//! - Create an api_token
//! - Get existing api_tokens
//! - Delete api_tokens
//! - Get personal user_information
//! - Get all projects a user is member of
//!
use super::authz::Authz;
use crate::database::connection::Database;
use crate::database::crud::utils::{map_permissions, EMAIL_SCHEMA};
use crate::error::ArunaError;
use crate::server::mail_client::MailClient;
use crate::server::services::utils::{format_grpc_request, format_grpc_response};
use aruna_rust_api::api::storage::services::v1::user_service_server::UserService;
use aruna_rust_api::api::storage::services::v1::*;
use std::sync::Arc;
use tokio::task;
use tonic::{Request, Response};

// This automatically creates the UserServiceImpl struct and ::new methods
crate::impl_grpc_server!(UserServiceImpl, mail_client: Option<MailClient>);

/// Trait created by tonic based on gRPC service definitions from .proto files
/// .proto files defined in ArunaAPI repo
#[tonic::async_trait]
impl UserService for UserServiceImpl {
    /// RegisterUser registers a new user that has authenticated via OIDC
    ///
    ///  ## Arguments
    ///
    /// * request: RegisterUserRequest: gRPC request, that contains only a display name
    ///
    /// ## Returns
    ///
    /// * Result<tonic::Response<RegisterUserResponse>, tonic::Status>: Contains the generated UUID for the user
    ///  This id should be used as identity for all user associations in the aruna application
    ///
    async fn register_user(
        &self,
        request: tonic::Request<RegisterUserRequest>,
    ) -> Result<tonic::Response<RegisterUserResponse>, tonic::Status> {
        log::info!("Received RegisterUserRequest.");
        log::debug!("{}", format_grpc_request(&request));

        if Authz::is_oidc_from_metadata(request.metadata()).await? {
            // Get subject from OIDC context in metadata
            let subject_id = self.authz.validate_oidc_only(request.metadata()).await?;

            // Create user in db and return response
            let response = Response::new(
                self.database
                    .register_user(request.into_inner(), subject_id)?,
            );

            log::info!("Sending RegisterUserResponse back to client.");
            log::debug!("{}", format_grpc_response(&response));
            Ok(response)
        } else {
            Err(tonic::Status::invalid_argument(
                "ArunaToken not allowed, use OIDC Token.",
            ))
        }
    }

    /// Deactivate user deactivates an activated user.
    ///
    ///  ## Arguments
    ///
    /// * request: DeactivateUserRequest: gRPC request, that contains the user_id
    ///
    /// ## Returns
    ///
    /// * Result<tonic::Response<DeactivateUserResponse>, tonic::Status>: Empty response indicates success
    ///
    async fn deactivate_user(
        &self,
        request: tonic::Request<DeactivateUserRequest>,
    ) -> Result<tonic::Response<DeactivateUserResponse>, tonic::Status> {
        log::info!("Received DeactivateUserRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Check permissions
        let token_user_uuid = self.authz.admin_authorize(request.metadata()).await?;

        // Consume gRPC request
        let inner_request = request.into_inner();

        // Evaluate user id from request
        let user_uuid = if inner_request.user_id.is_empty() {
            token_user_uuid
        } else {
            uuid::Uuid::parse_str(&inner_request.user_id).map_err(|_| {
                ArunaError::InvalidRequest("Can not parse provided user id".to_string())
            })?
        };

        // Deactivate user with user_uuid
        let database_clone = self.database.clone();
        task::spawn_blocking(move || database_clone.deactivate_user(&user_uuid))
            .await
            .map_err(ArunaError::from)??;

        // Return gRPC response
        let response = tonic::Response::new(DeactivateUserResponse {});

        log::info!("Sending DeactivateUserResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        return Ok(response);
    }

    /// Activate user activates a not activated but registered user
    ///
    ///  ## Arguments
    ///
    /// * request: ActivateUserRequest: gRPC request, that contains the user_id
    ///
    /// ## Returns
    ///
    /// * Result<tonic::Response<ActivateUserResponse>, tonic::Status>: Placeholder, currently empty
    ///
    async fn activate_user(
        &self,
        request: tonic::Request<ActivateUserRequest>,
    ) -> Result<tonic::Response<ActivateUserResponse>, tonic::Status> {
        log::info!("Received ActivateUserRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // For now only admins can activate "new" users
        self.authz.admin_authorize(request.metadata()).await?;
        // Activate the user
        let response = Response::new(self.database.activate_user(request.into_inner())?);

        log::info!("Sending ActivateUserRequest back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }

    /// CreateAPIToken creates a new API token, users must use a token for all requests except this one and register_user
    ///
    ///  ## Arguments
    ///
    /// * request: CreateApiTokenRequest: gRPC request, that contains only a display name
    ///
    /// ## Returns
    ///
    /// * Result<tonic::Response<CreateApiTokenResponse>, tonic::Status>: Contains the token_information
    ///   and the token_secret. This secret is used to authenticate against all endpoints in this api.
    ///
    ///   Attention: This secret can not be regenerated once issued.
    ///   Create a new token and delete the old one if the secret gets lost.
    ///
    async fn create_api_token(
        &self,
        request: tonic::Request<CreateApiTokenRequest>,
    ) -> Result<tonic::Response<CreateApiTokenResponse>, tonic::Status> {
        log::info!("Received CreateApiTokenRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // If the token is an oidc token
        if Authz::is_oidc_from_metadata(request.metadata()).await? {
            // Validate the token and query the subject
            let user_id = self.authz.personal_authorize(request.metadata()).await?;

            if !request.get_ref().project_id.is_empty()
                || !request.get_ref().collection_id.is_empty()
            {
                return Err(
                    tonic::Status::invalid_argument(
                        "OIDC requests can only authorize personal tokens: project_id and collection_id must be empty"
                    )
                );
            }

            // Create the API token in the database
            let (token_descr, access_key, secret_key) = self.database.create_api_token(
                request.into_inner(),
                user_id,
                self.authz.get_decoding_serial().await,
            )?;

            // Sign the token and create a new "secret" this
            // should be used to authenticate
            // Attention: This can not be regenerated, once issued this information is gone
            // Create a new token and delete the old one if the secret gets lost
            let token_secret = self
                .authz
                .sign_new_token(&token_descr.id, token_descr.expires_at.clone())
                .await?;

            // Convert to gRPC response and return
            let response = Response::new(CreateApiTokenResponse {
                token: Some(token_descr),
                token_secret,
                s3_access_key: access_key,
                s3_secret_key: secret_key,
            });

            log::info!("Sending CreateApiTokenResponse back to client.");
            log::debug!("{}", format_grpc_response(&response));
            return Ok(response);
            // Second branch if the request is issued via an personal aruna token
        } else {
            // Query user_id
            let user_id = self.authz.personal_authorize(request.metadata()).await?;

            if !request.get_ref().collection_id.is_empty() {
                let col_id =
                    uuid::Uuid::parse_str(&request.get_ref().collection_id).map_err(|_| {
                        ArunaError::InvalidRequest("Can not parse collection_id".to_string())
                    })?;
                self.authz
                    .collection_authorize(
                        request.metadata(),
                        col_id,
                        map_permissions(request.get_ref().permission()).ok_or_else(|| {
                            ArunaError::InvalidRequest("Can not parse permissions".to_string())
                        })?,
                    )
                    .await?;
            }

            if !request.get_ref().project_id.is_empty() {
                let proj_id =
                    uuid::Uuid::parse_str(&request.get_ref().project_id).map_err(|_| {
                        ArunaError::InvalidRequest("Can not parse project_id".to_string())
                    })?;
                self.authz
                    .project_authorize(
                        request.metadata(),
                        proj_id,
                        map_permissions(request.get_ref().permission()).ok_or_else(|| {
                            ArunaError::InvalidRequest("Can not parse permissions".to_string())
                        })?,
                    )
                    .await?;
            }

            // Create token in database and return the description
            let (token_descr, access_key, secret_key) = self.database.create_api_token(
                request.into_inner(),
                user_id,
                self.authz.get_decoding_serial().await,
            )?;

            // Sign a new secret for this token
            // Attention: This can not be regenerated, once issued this information is gone
            // Create a new token and delete the old one if the secret gets lost
            let token_secret = self
                .authz
                .sign_new_token(&token_descr.id, token_descr.expires_at.clone())
                .await?;

            // Parse to gRPC response and return it
            let response = Response::new(CreateApiTokenResponse {
                token: Some(token_descr),
                token_secret,
                s3_access_key: access_key,
                s3_secret_key: secret_key,
            });

            log::info!("Sending CreateApiTokenResponse back to client.");
            log::debug!("{}", format_grpc_response(&response));
            return Ok(response);
        }
    }

    /// Returns one API token by id or name
    ///
    ///  ## Arguments
    ///
    /// * request: GetApiTokenRequest: gRPC request, that contains the requested token_id or token_name
    ///
    /// ## Returns
    ///
    /// * Result<tonic::Response<GetApiTokenResponse>, tonic::Status>: Contains the token_information
    ///   without the token_secret.
    ///
    async fn get_api_token(
        &self,
        request: tonic::Request<GetApiTokenRequest>,
    ) -> Result<tonic::Response<GetApiTokenResponse>, tonic::Status> {
        log::info!("Received GetApiTokenRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Authenticate (personally) and get the user_id
        let user_id = self.authz.personal_authorize(request.metadata()).await?;

        // Execute the request and return the gRPC response
        let response = Response::new(self.database.get_api_token(request.into_inner(), user_id)?);

        log::info!("Sending GetApiTokenResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }

    /// Returns all API token for a specific user
    ///
    ///  ## Arguments
    ///
    /// * request: GetApiTokensRequest: Placeholder, currently empty
    ///
    /// ## Returns
    ///
    /// * Result<tonic::Response<GetApiTokensResponse>, tonic::Status>: Contains a list with all token_informations
    ///   without the token_secrets.
    ///
    async fn get_api_tokens(
        &self,
        request: tonic::Request<GetApiTokensRequest>,
    ) -> Result<tonic::Response<GetApiTokensResponse>, tonic::Status> {
        log::info!("Received GetApiTokensRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Authenticate (personally) the user and get the user_id
        let user_id = self.authz.personal_authorize(request.metadata()).await?;

        // Execute the db request and directly return as gRPC response
        let response = Response::new(
            self.database
                .get_api_tokens(request.into_inner(), user_id)?,
        );

        log::info!("Sending GetApiTokensResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }

    /// DeleteAPITokenRequest Deletes the specified API Token
    ///
    ///  ## Arguments
    ///
    /// * request: DeleteApiTokenRequest: Contains a token_id to delete
    ///
    /// ## Returns
    ///
    /// * Result<tonic::Response<DeleteApiTokenResponse>, tonic::Status>: Placeholder response, if status::ok -> response was successfull
    ///
    async fn delete_api_token(
        &self,
        request: tonic::Request<DeleteApiTokenRequest>,
    ) -> Result<tonic::Response<DeleteApiTokenResponse>, tonic::Status> {
        log::info!("Received DeleteApiTokenRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Authenticate (personally) the user and get the user_id
        let user_id = self.authz.personal_authorize(request.metadata()).await?;

        // Delete the token and return the (empty) response
        let response = Response::new(
            self.database
                .delete_api_token(request.into_inner(), user_id)?,
        );

        log::info!("Sending DeleteApiTokenResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }

    /// DeleteAPITokens deletes all API Tokens from a user
    /// this request can either be issued by the user itself or by an admin for another user.
    /// It is intended to be used to invalidate all tokens for a user when this user gets comprimised.
    /// The user has to use its OIDC token to create new api_tokens
    ///
    /// ## Arguments
    ///
    /// * request: DeleteApiTokensRequest: Contains a user_id (only for admin use)
    ///
    /// ## Returns
    ///
    /// * Result<tonic::Response<DeleteApiTokensResponse>, tonic::Status>: Placeholder response, if status::ok -> response was successfull
    ///
    async fn delete_api_tokens(
        &self,
        request: tonic::Request<DeleteApiTokensRequest>,
    ) -> Result<tonic::Response<DeleteApiTokensResponse>, tonic::Status> {
        log::info!("Received DeleteApiTokenRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Check if user_id is empty
        if request.get_ref().user_id.is_empty() {
            // Authenticate personally
            let user_id = self.authz.personal_authorize(request.metadata()).await?;

            // Execute the request in a personal context
            // Delete all tokens for the user
            let response = Response::new(
                self.database
                    .delete_api_tokens(request.into_inner(), user_id)?,
            );

            log::info!("Sending DeleteApiTokensResponse back to client.");
            log::debug!("{}", format_grpc_response(&response));
            return Ok(response);
            // This should only be used as admin
            // If a non admin issues this request for himself
            // this might fail with an unauthenticated error
        } else {
            // Authorize as admin
            self.authz.admin_authorize(request.metadata()).await?;

            // Parse the request body and get the user_id
            let parsed_body_uid =
                uuid::Uuid::parse_str(&request.get_ref().user_id).map_err(ArunaError::from)?;

            // Delete all tokens for this user and return response (empty)
            let response = Response::new(
                self.database
                    .delete_api_tokens(request.into_inner(), parsed_body_uid)?,
            );

            log::info!("Sending DeleteApiTokensResponse back to client.");
            log::debug!("{}", format_grpc_response(&response));
            return Ok(response);
        }
    }

    /// UserWhoAmI is a request that returns the user information of the current user
    ///
    /// ## Arguments
    ///
    /// * request: GetUserRequest: Contains optional UserID -> Only available with global admin permissions
    ///
    /// ## Returns
    ///
    /// * Result<tonic::Response<GetUserResponse>, tonic::Status>: UserInformation like, id, displayname, active status and user_permissions for each project etc.
    ///
    async fn get_user(
        &self,
        request: tonic::Request<GetUserRequest>,
    ) -> Result<tonic::Response<GetUserResponse>, tonic::Status> {
        log::info!("Received GetUserRequest.");
        log::debug!("{}", format_grpc_request(&request));

        let user_id = if request.get_ref().user_id.is_empty() {
            // Personal authorize
            self.authz.personal_authorize(request.metadata()).await?
        } else {
            // Admin authorize if not personal user_id
            let parsed_id = uuid::Uuid::parse_str(&request.get_ref().user_id)
                .map_err(|_| ArunaError::InvalidRequest("Unable to parse user_uuid".to_string()))?;
            self.authz.admin_authorize(request.metadata()).await?;
            parsed_id
        };

        // Get personal user info and return the gRPC repsonse
        let response = Response::new(self.database.get_user(user_id)?);

        log::info!("Sending GetUserResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }

    /// UpdateUserDisplayName request changed the display_name of the current user to a new value
    /// This name is optional and has only cosmetic value to better identify otherwise cryptic UUIDs
    ///
    ///
    /// ## Arguments
    ///
    /// * request: UpdateUserDisplayNameRequest: Contains the new user display_name
    ///
    /// ## Returns
    ///
    /// * Result<tonic::Response<UpdateUserDisplayNameResponse>, tonic::Status>: UserInformation like, id, displayname, active status etc.
    ///
    async fn update_user_display_name(
        &self,
        request: tonic::Request<UpdateUserDisplayNameRequest>,
    ) -> Result<tonic::Response<UpdateUserDisplayNameResponse>, tonic::Status> {
        log::info!("Received UpdateUserDisplayNameRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Authenticate the user personally
        let user_id = self.authz.personal_authorize(request.metadata()).await?;

        // Update the display_name and return the new user_info
        let response = Response::new(
            self.database
                .update_user_display_name(request.into_inner(), user_id)?,
        );

        log::info!("Sending UpdateUserDisplayNameResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }

    /// UpdateUserEmail request changes the email of the current user to a new value
    /// The email is optional and can be empty if the user does not want to receive notifications via email.
    ///
    ///
    /// ## Arguments
    ///
    /// * request: UpdateUserEmailRequest: Contains the new email address
    ///
    /// ## Returns
    ///
    /// * Result<tonic::Response<UpdateUserEmailResponse>, tonic::Status>: UserInformation like, id, displayname, active status etc.
    ///
    async fn update_user_email(
        &self,
        request: Request<UpdateUserEmailRequest>,
    ) -> Result<Response<UpdateUserEmailResponse>, tonic::Status> {
        log::info!("Received UpdateUserEmailRequest.");
        log::debug!("{}", format_grpc_request(&request));

        if !EMAIL_SCHEMA.is_match(&request.get_ref().new_email) {
            return Err(tonic::Status::invalid_argument("Invalid email format"));
        }

        // Authenticate the user personally
        let user_id = self.authz.personal_authorize(request.metadata()).await?;

        // Update the display_name and return the new user_info
        let response = Response::new(
            self.database
                .update_user_email(request.into_inner(), user_id)?,
        );

        log::info!("Sending UpdateUserEmailResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }

    /// Requests a list of all projects a user is member of
    /// This request can either be executed personally or via an admin for another user
    ///
    /// ## Arguments
    ///
    /// * request: GetUserProjectsRequest: Contains the new user display_name
    ///
    /// ## Returns
    ///
    /// * Result<tonic::Response<GetUserProjectsResponse>, tonic::Status>: List with all projects the user is part of
    ///
    async fn get_user_projects(
        &self,
        request: tonic::Request<GetUserProjectsRequest>,
    ) -> Result<tonic::Response<GetUserProjectsResponse>, tonic::Status> {
        log::info!("Received GetUserProjectsRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Check if user_id is empty
        if request.get_ref().user_id.is_empty() {
            // Authenticate personally
            let user_id = self.authz.personal_authorize(request.metadata()).await?;

            // Get all projects and return a list as gRPC response
            let response = Response::new(
                self.database
                    .get_user_projects(request.into_inner(), user_id)?,
            );

            log::info!("Sending GetUserProjectsResponse back to client.");
            log::debug!("{}", format_grpc_response(&response));
            return Ok(response);
            // Otherwise this must be authenticated as admin
        } else {
            // Authenticate as admin
            self.authz.admin_authorize(request.metadata()).await?;

            // Parse the user_id from the request body
            let parsed_body_uid =
                uuid::Uuid::parse_str(&request.get_ref().user_id).map_err(ArunaError::from)?;

            // Get all projects for a user and return the list as gRPC response
            let response = Response::new(
                self.database
                    .get_user_projects(request.into_inner(), parsed_body_uid)?,
            );

            log::info!("Sending GetUserProjectsResponse back to client.");
            log::debug!("{}", format_grpc_response(&response));
            return Ok(response);
        }
    }

    /// GetUsersUnregisters returns all users that have not been activated yet.
    /// TODO: Rename request to e.g. "GetUsersNotActivated"
    ///
    ///  ## Arguments
    ///
    /// * request: ActivateUserRequest: gRPC request, that contains the user_id
    ///
    /// ## Returns
    ///
    /// * Result<tonic::Response<ActivateUserResponse>, tonic::Status>: Placeholder, currently empty
    ///
    async fn get_not_activated_users(
        &self,
        request: tonic::Request<GetNotActivatedUsersRequest>,
    ) -> Result<tonic::Response<GetNotActivatedUsersResponse>, tonic::Status> {
        log::info!("Received GetUsersUnregisteredRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // For now only admins can activate "new" users
        let _user_id = self.authz.admin_authorize(request.metadata()).await?;
        // Create user in db and return response
        let response = Response::new(
            self.database
                .get_not_activated_users(request.into_inner(), _user_id)?,
        );

        log::info!("Sending GetUsersUnregisteredResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }

    /// Fetches all users with permission informations.
    ///
    /// ## Arguments
    ///
    /// * request: GetAllUsersRequest: Contains flag if user permissions shall be included in response
    ///
    /// ## Returns
    ///
    /// * Result<tonic::Response<GetAllUsersResponse>, tonic::Status>:
    /// UserInformation like, id, displayname, active status and user_permissions for each project etc. of all registered users
    ///
    async fn get_all_users(
        &self,
        request: tonic::Request<GetAllUsersRequest>,
    ) -> Result<tonic::Response<GetAllUsersResponse>, tonic::Status> {
        log::info!("Received GetAllUsersRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Check permissions
        self.authz.admin_authorize(request.metadata()).await?;

        // Consume gRPC request
        let inner_request = request.into_inner();

        // Deactivate user with user_uuid
        let database_clone = self.database.clone();
        let response = task::spawn_blocking(move || {
            database_clone.get_all_users(inner_request.include_permissions)
        })
        .await
        .map_err(ArunaError::from)??;

        // Return gRPC response
        let grpc_response = tonic::Response::new(response);

        log::info!("Sending GetAllUsersResponse back to client.");
        log::debug!("{}", format_grpc_response(&grpc_response));
        return Ok(grpc_response);
    }
}
