//! This file contains all database methods that handle user specific actions
//!
//! Mainly this is used to:
//! - Register a user
//! - Create an api_token
//! - Get existing api_tokens
//! - Delete api_tokens
//! - Get personal user_information
//! - Get all projects a user is member of
//!
use super::utils::*;
use crate::database::connection::Database;
use crate::database::models::auth::{ApiToken, Project as ProjectDB, User, UserPermission};
use crate::database::models::enums::UserRights;
use crate::database::schema::api_tokens::dsl::api_tokens;
use crate::error::ArunaError;

use aruna_rust_api::api::storage::models::v1::{
    ProjectPermission, Token, TokenType, User as gRPCUser,
};
use aruna_rust_api::api::storage::services::v1::{
    ActivateUserRequest, ActivateUserResponse, CreateApiTokenRequest, DeleteApiTokenRequest,
    DeleteApiTokenResponse, DeleteApiTokensRequest, DeleteApiTokensResponse, GetAllUsersResponse,
    GetApiTokenRequest, GetApiTokenResponse, GetApiTokensRequest, GetApiTokensResponse,
    GetNotActivatedUsersRequest, GetNotActivatedUsersResponse, GetUserProjectsRequest,
    GetUserProjectsResponse, GetUserResponse, RegisterUserRequest, RegisterUserResponse,
    UpdateUserDisplayNameRequest, UpdateUserDisplayNameResponse, UpdateUserEmailRequest,
    UpdateUserEmailResponse, UserProject, UserWithPerms,
};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::str::FromStr;

use crate::database::schema::users::dsl::users;
use chrono::Utc;
use diesel::{delete, insert_into};
use diesel::{prelude::*, sql_query, sql_types::Uuid, update};

impl Database {
    /// Registers a new (unregistered) user by its oidc `external_id`
    /// Registers a new (unregistered) user by its oidc `external_id`
    /// This must be called once in order to complete the registration for Aruna
    /// Users can provide an optional display_name to use e.g. for websites clients etc.
    /// TODO: GDPR notice -> This request could be used to get consent from the user to process some personal data.
    ///
    /// ## Arguments
    ///
    /// * request: RegisterUserRequest: The original gRPC request, contains only a display name
    /// * ext_id: String: The external OIDC id this will be determined by the authorization flow
    ///
    /// ## Returns
    ///
    /// * Result<RegisterUserResponse, ArunaError>: RegisterUserResponse only contains the generated UUID for the user
    ///  This id should be used as identity for all associations inside the Aruna application
    ///
    pub fn register_user(
        &self,
        request: RegisterUserRequest,
        ext_id: String,
    ) -> Result<RegisterUserResponse, ArunaError> {
        use crate::database::schema::users::dsl::*;
        use diesel::result::Error;

        // Create a new DB user object
        let db_user = User {
            id: diesel_ulid::DieselUlid::generate(),
            display_name: request.display_name,
            external_id: ext_id,
            active: false,
            is_service_account: false,
            email: request.email,
        };

        // Insert the user and return the user_id
        let user_id = self
            .pg_connection
            .get()?
            .transaction::<diesel_ulid::DieselUlid, Error, _>(|conn| {
                insert_into(users)
                    .values(&db_user)
                    .returning(id)
                    .get_result(conn)
            })?;

        // Create successfull response with user_id
        Ok(RegisterUserResponse {
            user_id: user_id.to_string(),
        })
    }

    /// Activates a registered user, only activated users can create new api tokens.
    ///
    /// ## Arguments
    ///
    /// * request: ActivateUserRequest the user_id that should be activated
    ///
    /// ## Returns
    ///
    /// * Result<ActivateUserResponse, ArunaError>: Placeholder, currently empty response
    ///
    pub fn activate_user(
        &self,
        request: ActivateUserRequest,
    ) -> Result<ActivateUserResponse, ArunaError> {
        use crate::database::schema::users::dsl::*;
        use diesel::result::Error;

        let user_id = diesel_ulid::DieselUlid::from_str(&request.user_id)?;

        // Update the user
        self.pg_connection
            .get()?
            .transaction::<_, Error, _>(|conn| {
                update(users)
                    .filter(id.eq(user_id))
                    .set(active.eq(true))
                    .execute(conn)?;
                Ok(())
            })?;

        // Create successfull response
        Ok(ActivateUserResponse {})
    }

    /// Deactivates a registered user.
    ///
    /// ## Arguments
    ///
    /// * user_uuid: &diesel_ulid::DieselUlid`: the user id that should be deactivated
    ///
    /// ## Returns
    ///
    /// * `Result<(), ArunaError>`: Ok() result return indicates success
    ///
    pub fn deactivate_user(&self, user_uuid: &diesel_ulid::DieselUlid) -> Result<(), ArunaError> {
        use crate::database::schema::users::dsl::*;

        // Update the user
        self.pg_connection
            .get()?
            .transaction::<_, ArunaError, _>(|conn| {
                update(users)
                    .filter(id.eq(user_uuid))
                    .set(active.eq(false))
                    .execute(conn)?;
                Ok(())
            })?;

        // Create successful response
        Ok(())
    }

    /// Creates a new API Token. This request can be made either with an already existing
    /// ArunaToken or with an OIDC Token from an supported OIDC provider. This request
    /// can only succeed if the user has already registered a new account.
    ///
    /// ## Arguments
    ///
    /// * request: CreateApiTokenRequest: The original gRPC request, contains only a token name
    /// * user_id: String: OIDC or Aruna UserID
    /// * pubkey_id: The reference to the serial of the currently used signing key
    ///   If this signing key is deleted all associated tokens should be deleted too.
    ///
    /// ## Returns
    ///
    /// * Result<Token, ArunaError>: Token Response, this does not contain the signed Token
    ///   Only informations from the database
    ///
    pub fn create_api_token(
        &self,
        request: CreateApiTokenRequest,
        user_id: diesel_ulid::DieselUlid,
        pubkey_id: i64,
    ) -> Result<(Token, String, String), ArunaError> {
        // Generate a new UUID for the token
        let new_uid = diesel_ulid::DieselUlid::generate();

        // Get the expiry_time
        let expiry_time = request.expires_at.clone();
        // Parse it to Option<NaiveDateTime>
        let exp_time = match expiry_time {
            Some(t) => t.timestamp.map(|t| {
                chrono::NaiveDateTime::from_timestamp_opt(t.seconds, 0).unwrap_or_default()
            }),
            None => None,
        };

        // Parse the optional project_id -> only used for scoped tokens
        let parsed_project_id = diesel_ulid::DieselUlid::from_str(&request.project_id).ok();
        // Parse the optional collection_id -> only used for scoped tokens
        let parsed_collection_id = diesel_ulid::DieselUlid::from_str(&request.collection_id).ok();
        // Parse the permissions, should already be validated by the request
        let user_right_db: Option<UserRights> = map_permissions(request.permission());
        // Create a token_type variable for the response
        let mut token_type = TokenType::Personal;
        // If on of collection_id or project_id is not empty this is a scoped token
        if parsed_collection_id.is_some() || parsed_project_id.is_some() {
            token_type = TokenType::Scoped;
            // Either collection or project should be set, not both!
            if parsed_collection_id.is_some() == parsed_project_id.is_some() {
                return Err(ArunaError::InvalidRequest(
                    "Cannot set collection_id and project_id at once both should be exclusive"
                        .to_owned(),
                ));
            }
        }

        // Create random access_key
        let secret_key: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(30)
            .map(char::from)
            .collect();

        // Create the new DB APIToken
        let new_token = ApiToken {
            id: new_uid,
            creator_user_id: user_id,
            name: Some(request.name),
            pub_key: pubkey_id,
            created_at: Utc::now().naive_local(),
            expires_at: exp_time,
            project_id: parsed_project_id,
            collection_id: parsed_collection_id,
            user_right: user_right_db,
            secretkey: secret_key.to_string(),
            used_at: Utc::now().naive_local(),
            is_session: false,
        };

        use crate::database::schema::api_tokens::dsl::*;
        use diesel::result::Error;

        // Insert the token in the DB
        let api_token = self
            .pg_connection
            .get()?
            .transaction::<ApiToken, Error, _>(|conn| {
                insert_into(api_tokens).values(&new_token).get_result(conn)
            })?;

        // Parse the returned time to prost_type::Timestamp
        let expires_at_time = match api_token.expires_at {
            Some(time) => Some(naivedatetime_to_prost_time(time)?),
            None => None,
        };

        // Create the response
        Ok((
            Token {
                id: api_token.id.to_string(),
                name: api_token.name.unwrap_or_default(),
                token_type: token_type as i32,
                created_at: Some(naivedatetime_to_prost_time(api_token.created_at)?),
                expires_at: expires_at_time,
                collection_id: option_to_string(api_token.collection_id).unwrap_or_default(),
                project_id: option_to_string(api_token.project_id).unwrap_or_default(),
                permission: map_permissions_rev(api_token.user_right),
                is_session: api_token.is_session,
                used_at: Some(naivedatetime_to_prost_time(api_token.used_at)?),
            },
            api_token.id.to_string(),
            secret_key,
        ))
    }

    /// Gets a specific API Token by id from the user. Users can get the ID either from their signed token or
    /// via the GetTokensRequest that returns all user tokens.
    ///
    /// ## Arguments
    ///
    /// * request: GetApiTokenRequest: The original gRPC request, contains only a token name
    /// * user_id: String: Aruna UserID this request can only be used with `personal` Aruna Tokens, not with OIDC tokens
    ///
    /// ## Returns
    ///
    /// * Result<GetApiTokenResponse, ArunaError>: Token Response, this does not contain the signed Token only
    ///   all Database information
    ///
    pub fn get_api_token(
        &self,
        request: GetApiTokenRequest,
        user_id: diesel_ulid::DieselUlid,
    ) -> Result<GetApiTokenResponse, ArunaError> {
        use crate::database::schema::api_tokens::dsl::*;
        use diesel::result::Error as dError;

        // Parse the token_id from the request
        let token_id = if !request.token_id.is_empty() {
            Some(diesel_ulid::DieselUlid::from_str(&request.token_id)?)
        } else {
            None
        };

        // Either the token_id or the token_name has to be specified
        if token_id.is_none() {
            return Err(
                ArunaError::InvalidRequest(
                    "token_id for the token must be specified use: get_api_tokens for a list of all tokens".to_string()
                )
            );
        }

        // Query db to get the token bubble up error if nothing is found
        let api_token = self
            .pg_connection
            .get()?
            .transaction::<ApiToken, dError, _>(|conn| {
                if token_id.is_some() {
                    api_tokens
                        .filter(id.eq(token_id.unwrap_or_default()))
                        .filter(creator_user_id.eq(user_id))
                        .first::<ApiToken>(conn)
                } else {
                    Err(dError::NotFound)
                }
            })?;

        // Parse the expiry time to be prost_type format
        let expires_at_time = match api_token.expires_at {
            Some(time) => Some(naivedatetime_to_prost_time(time)?),
            None => None,
        };

        // Parse the token_type
        let token_type = if api_token.collection_id.is_some() || api_token.project_id.is_some() {
            2
        } else {
            1
        };

        // Build the response message
        Ok(GetApiTokenResponse {
            token: Some(Token {
                id: api_token.id.to_string(),
                name: api_token.name.unwrap_or_default(),
                token_type,
                created_at: Some(naivedatetime_to_prost_time(api_token.created_at)?),
                expires_at: expires_at_time,
                collection_id: option_to_string(api_token.collection_id).unwrap_or_default(),
                project_id: option_to_string(api_token.project_id).unwrap_or_default(),
                permission: map_permissions_rev(api_token.user_right),
                is_session: api_token.is_session,
                used_at: Some(naivedatetime_to_prost_time(api_token.used_at)?),
            }),
        })
    }

    /// Gets a specific API Token by its unique id.
    ///
    /// ## Arguments
    ///
    /// * token_id: &diesel_ulid::DieselUlid: Unique api token id
    ///
    /// ## Returns
    ///
    /// * Result<ApiToken, ArunaError>: Token Response, this does not contain the signed Token only
    ///   all Database information
    pub fn get_api_token_by_id(
        &self,
        token_id: &diesel_ulid::DieselUlid,
    ) -> Result<ApiToken, ArunaError> {
        use crate::database::schema::api_tokens::dsl as tokens_dsl;

        // Fetch token info from database
        let api_token = self
            .pg_connection
            .get()?
            .transaction::<ApiToken, ArunaError, _>(|conn| {
                Ok(api_tokens
                    .filter(tokens_dsl::id.eq(token_id))
                    .first::<ApiToken>(conn)?)
            })?;

        Ok(api_token)
    }

    /// Gets all API Tokens from a user.
    ///
    /// ## Arguments
    ///
    /// * _request: GetApiTokensRequest: Placeholder, currently not used.
    /// * user_id: String: Aruna UserID this request can only be used with `personal` Aruna Tokens, not with OIDC tokens
    ///
    /// ## Returns
    ///
    /// * Result<GetApiTokenResponse, ArunaError>: Token Response, this does not contain the signed Token only
    ///   all Database information
    ///

    pub fn get_api_tokens(
        &self,
        _request: GetApiTokensRequest,
        user_id: diesel_ulid::DieselUlid,
    ) -> Result<GetApiTokensResponse, ArunaError> {
        use crate::database::schema::api_tokens::dsl::*;
        use diesel::result::Error as dError;

        // Query db for all tokens
        let atoken_result = self
            .pg_connection
            .get()?
            .transaction::<Vec<ApiToken>, dError, _>(|conn| {
                api_tokens
                    .filter(creator_user_id.eq(user_id))
                    .load::<ApiToken>(conn)
            })?;

        // Convert all db tokens to gRPC format
        let converted = atoken_result
            .iter()
            .map(|api_token| {
                // Parse expiry_time
                let expires_at_time = match api_token.expires_at {
                    Some(time) => Some(naivedatetime_to_prost_time(time)?),
                    None => None,
                };

                // Parse token_type
                let token_type =
                    if api_token.collection_id.is_some() || api_token.project_id.is_some() {
                        2
                    } else {
                        1
                    };

                // Return gRPC formatted token
                Ok(Token {
                    id: api_token.id.to_string(),
                    // Abomination made by borrow_checker
                    // if someone knows a better way, feel free to add a PR
                    name: api_token
                        .name
                        .as_ref()
                        .unwrap_or(&"".to_string())
                        .to_string(),
                    token_type,
                    created_at: Some(naivedatetime_to_prost_time(api_token.created_at)?),
                    expires_at: expires_at_time,
                    collection_id: option_to_string(api_token.collection_id).unwrap_or_default(),
                    project_id: option_to_string(api_token.project_id).unwrap_or_default(),
                    permission: map_permissions_rev(api_token.user_right),
                    is_session: api_token.is_session,
                    used_at: Some(naivedatetime_to_prost_time(api_token.used_at)?),
                })
            })
            .collect::<Result<Vec<_>, ArunaError>>()?;

        // return the converted gRPC token_list
        Ok(GetApiTokensResponse { token: converted })
    }

    /// Delete a specific API Token (by token_id)
    ///
    /// ## Arguments
    ///
    /// * request: DeleteApiTokenRequest: Contains the token_id that should be deleted
    /// * user_id: String: Aruna UserID this request can only be used with `personal` Aruna Tokens, not with OIDC tokens
    ///
    /// ## Returns
    ///
    /// * Result<DeleteApiTokenResponse, ArunaError>: Placeholder, currently unused. Non error response means success.
    ///
    pub fn delete_api_token(
        &self,
        request: DeleteApiTokenRequest,
        user_id: diesel_ulid::DieselUlid,
    ) -> Result<DeleteApiTokenResponse, ArunaError> {
        use crate::database::schema::api_tokens::dsl::*;
        use diesel::result::Error as dError;

        // Get token_id from request
        let token_id_request = diesel_ulid::DieselUlid::from_str(&request.token_id)?;

        // Execute db delete
        self.pg_connection
            .get()?
            .transaction::<_, dError, _>(|conn| {
                delete(api_tokens)
                    .filter(id.eq(token_id_request))
                    .filter(creator_user_id.eq(user_id))
                    .execute(conn)?;
                Ok(())
            })?;

        // Return nothing for success, otherwise bubble up error
        Ok(DeleteApiTokenResponse {})
    }

    /// Deletes all API Tokens for a specific user. This request can either be used personally or by an admin.
    /// As a result the user has to recreate API_Tokens using its OIDC Token. This request is meant to be used
    /// as a security measure if someone is not sure if their tokens were comprimised.
    ///
    /// ## Arguments
    ///
    /// * request: DeleteApiTokensRequest: Placeholder, currently not in use.
    /// * user_id: String: User_ID either from the personal token or from the request authorized by an admin.
    ///
    /// ## Returns
    ///
    /// * Result<DeleteApiTokensResponse, ArunaError>: Placeholder, currently unused. Non error response means success.
    ///
    pub fn delete_api_tokens(
        &self,
        _request: DeleteApiTokensRequest,
        user_id: diesel_ulid::DieselUlid,
    ) -> Result<DeleteApiTokensResponse, ArunaError> {
        use crate::database::schema::api_tokens::dsl::*;
        use diesel::result::Error as dError;

        // Delete all tokens
        self.pg_connection
            .get()?
            .transaction::<_, dError, _>(|conn| {
                delete(api_tokens)
                    .filter(creator_user_id.eq(user_id))
                    .execute(conn)?;
                Ok(())
            })?;

        // Return nothing if successfull, otherwise bubble up error
        Ok(DeleteApiTokensResponse {})
    }

    /// Request that returns personal user_information.
    ///
    /// ## Arguments
    ///
    /// * user_id: String: user_id validated by personal aruna_token
    ///
    /// ## Returns
    ///
    /// * Result<UserWhoAmIResponse, ArunaError>: Basic information about the requesting user, id, displayname etc.
    ///
    pub fn get_user(
        &self,
        req_user_id: diesel_ulid::DieselUlid,
    ) -> Result<GetUserResponse, ArunaError> {
        use crate::database::schema::user_permissions::dsl::*;
        use crate::database::schema::users::dsl::*;
        use diesel::result::Error as dError;

        // Query the user information
        let (user_info, perm, is_admin) =
            self.pg_connection
                .get()?
                .transaction::<(Option<User>, Vec<UserPermission>, bool), dError, _>(|conn| {
                    let user = users
                        .filter(crate::database::schema::users::id.eq(req_user_id))
                        .first::<User>(conn)
                        .optional()?;
                    if let Some(u) = user {
                        let uperm = user_permissions
                            .filter(crate::database::schema::user_permissions::user_id.eq(u.id))
                            .load::<UserPermission>(conn)
                            .optional()?;
                        let perm_vec = uperm.unwrap_or_default();

                        let admin_user_perm = sql_query(
                            "SELECT uperm.id, uperm.user_id, uperm.user_right, uperm.project_id 
                           FROM user_permissions AS uperm 
                           JOIN projects AS p 
                           ON p.id = uperm.project_id 
                           WHERE uperm.user_id = $1
                           AND p.flag & 1 = 1
                           LIMIT 1",
                        )
                        .bind::<Uuid, _>(u.id)
                        .get_result::<UserPermission>(conn)
                        .optional()?;

                        Ok((Some(u), perm_vec, admin_user_perm.is_some()))
                    } else {
                        Ok((None, Vec::new(), false))
                    }
                })?;

        // Convert information to gRPC format
        Ok(GetUserResponse {
            user: user_info.clone().map(|u| gRPCUser {
                id: u.id.to_string(),
                display_name: u.display_name,
                external_id: u.external_id,
                active: u.active,
                is_admin,
                is_service_account: u.is_service_account,
                email: u.email,
            }),
            project_permissions: perm
                .iter()
                .map(|elem| ProjectPermission {
                    user_id: user_info.clone().unwrap_or_default().id.to_string(),
                    project_id: elem.project_id.to_string(),
                    permission: map_permissions_rev(Some(elem.user_right)),
                    // TODO: check for service account
                    service_account: false,
                })
                .collect::<Vec<_>>(),
        })
    }

    /// Request that returns personal user information of all registered users.
    ///
    /// ## Arguments
    ///
    /// * include_permissions: &bool: Flag if permissions shall be included in response
    ///
    /// ## Returns
    ///
    /// * Result<GetAllUsersResponse, ArunaError>: Response contains vector with user information of all users
    ///
    pub fn get_all_users(
        &self,
        include_permissions: bool,
    ) -> Result<GetAllUsersResponse, ArunaError> {
        use crate::database::schema::user_permissions::dsl::*;

        // Query the user information
        let user_infos = self
            .pg_connection
            .get()?
            .transaction::<Vec<UserWithPerms>, ArunaError, _>(|conn| {
                let db_users: Vec<User> = users.load::<User>(conn)?;

                let mut users_with_permissions = Vec::new();
                for user in db_users {
                    let admin_user_perm = sql_query(
                        "SELECT uperm.id, uperm.user_id, uperm.user_right, uperm.project_id
                           FROM user_permissions AS uperm
                           JOIN projects AS p
                           ON p.id = uperm.project_id
                           WHERE uperm.user_id = $1
                           AND p.flag & 1 = 1
                           LIMIT 1",
                    )
                    .bind::<Uuid, _>(user.id)
                    .get_result::<UserPermission>(conn)
                    .optional()?;

                    let proto_user = gRPCUser {
                        id: user.id.to_string(),
                        external_id: user.external_id.to_string(),
                        display_name: user.display_name.to_string(),
                        active: user.active,
                        is_admin: admin_user_perm.is_some(),
                        is_service_account: user.is_service_account,
                        email: user.email.to_string(),
                    };

                    if include_permissions {
                        let all_user_permissions = user_permissions
                            .filter(crate::database::schema::user_permissions::user_id.eq(&user.id))
                            .load::<UserPermission>(conn)?;

                        users_with_permissions.push(UserWithPerms {
                            user: Some(proto_user),
                            project_perms: all_user_permissions
                                .iter()
                                .map(|perm| ProjectPermission {
                                    user_id: perm.user_id.to_string(),
                                    project_id: perm.project_id.to_string(),
                                    permission: map_permissions_rev(Some(perm.user_right)),
                                    service_account: user.is_service_account,
                                })
                                .collect::<Vec<_>>(),
                        })
                    } else {
                        users_with_permissions.push(UserWithPerms {
                            user: Some(proto_user),
                            project_perms: vec![],
                        })
                    }
                }

                Ok(users_with_permissions)
            })?;

        // Convert information to gRPC format
        Ok(GetAllUsersResponse {
            user_with_perms: user_infos,
        })
    }

    /// Updates the display_name for the requesting user.
    ///
    /// ## Arguments
    ///
    /// * request: UpdateUserDisplayNameRequest: Contains the new display_name
    /// * user_id: String: user_id validated by personal aruna_token
    ///
    /// ## Returns
    ///
    /// * Result<UpdateUserDisplayNameResponse, ArunaError>: Basic information about the (updated) requesting user, id, displayname etc.
    ///
    pub fn update_user_display_name(
        &self,
        request: UpdateUserDisplayNameRequest,
        user_id: diesel_ulid::DieselUlid,
    ) -> Result<UpdateUserDisplayNameResponse, ArunaError> {
        use crate::database::schema::users::dsl::*;
        use diesel::result::Error as dError;

        // Update user display_name in Database return "new" name
        let (user, is_admin) = self
            .pg_connection
            .get()?
            .transaction::<(User, bool), dError, _>(|conn| {
                let admin_user_perm = sql_query(
                    "SELECT uperm.id, uperm.user_id, uperm.user_right, uperm.project_id 
                       FROM user_permissions AS uperm 
                       JOIN projects AS p 
                       ON p.id = uperm.project_id 
                       WHERE uperm.user_id = $1
                       AND p.flag & 1 = 1
                       LIMIT 1",
                )
                .bind::<Uuid, _>(user_id)
                .get_result::<UserPermission>(conn)
                .optional()?;
                let update_ret = update(users.filter(id.eq(user_id)))
                    .set(display_name.eq(request.new_display_name))
                    .get_result(conn)?;
                Ok((update_ret, admin_user_perm.is_some()))
            })?;

        // Parse to gRPC format and return
        Ok(UpdateUserDisplayNameResponse {
            user: Some(gRPCUser {
                id: user.id.to_string(),
                display_name: user.display_name,
                external_id: user.external_id,
                active: user.active,
                is_admin,
                is_service_account: user.is_service_account,
                email: user.email,
            }),
        })
    }

    /// Updates the email for the requesting user.
    ///
    /// ## Arguments
    ///
    /// * request: UpdateUserDisplayNameRequest: Contains the new email
    /// * user_id: String: User id validated by personal aruna_token
    ///
    /// ## Returns
    ///
    /// * Result<UpdateUserDisplayNameResponse, ArunaError>:
    /// Basic information about the (updated) requesting user, id, displayname, etc.
    ///
    pub fn update_user_email(
        &self,
        request: UpdateUserEmailRequest,
        user_uuid: diesel_ulid::DieselUlid,
    ) -> Result<UpdateUserEmailResponse, ArunaError> {
        use crate::database::schema::users::dsl::*;

        // Update user display_name in Database return "new" name
        let (user, is_admin) = self
            .pg_connection
            .get()?
            .transaction::<(User, bool), ArunaError, _>(|conn| {
                let admin_user_perm = sql_query(
                    "SELECT uperm.id, uperm.user_id, uperm.user_right, uperm.project_id
                       FROM user_permissions AS uperm
                       JOIN projects AS p
                       ON p.id = uperm.project_id
                       WHERE uperm.user_id = $1
                       AND p.flag & 1 = 1
                       LIMIT 1",
                )
                .bind::<Uuid, _>(user_uuid)
                .get_result::<UserPermission>(conn)
                .optional()?;

                let update_ret = update(users.filter(id.eq(&user_uuid)))
                    .set(email.eq(&request.new_email))
                    .get_result(conn)?;

                Ok((update_ret, admin_user_perm.is_some()))
            })?;

        // Parse to gRPC format and return
        Ok(UpdateUserEmailResponse {
            user: Some(gRPCUser {
                id: user.id.to_string(),
                display_name: user.display_name,
                external_id: user.external_id,
                active: user.active,
                is_admin,
                is_service_account: user.is_service_account,
                email: user.email,
            }),
        })
    }

    /// Returns all projects the user is currently member of.
    ///
    /// ## Arguments
    ///
    /// * _request: GetUserProjectsRequest: Placeholder, currently not in use.
    /// * user_id: String: user_id validated by personal aruna_token or user_id specified by an admin
    ///
    /// ## Returns
    ///
    /// * Result<GetUserProjectsResponse, ArunaError>: Contains a list of all projects the user is currently a member of
    ///
    pub fn get_user_projects(
        &self,
        _request: GetUserProjectsRequest,
        user_grpc_id: diesel_ulid::DieselUlid,
    ) -> Result<GetUserProjectsResponse, ArunaError> {
        use crate::database::schema::projects::dsl::*;
        use crate::database::schema::user_permissions::dsl::*;
        use diesel::result::Error as dError;

        // Get all projects of a user based on the join of user_permissions and projects
        let user = self
            .pg_connection
            .get()?
            .transaction::<Vec<ProjectDB>, dError, _>(|conn| {
                projects
                    .inner_join(user_permissions)
                    .filter(user_id.eq(user_grpc_id))
                    .select(ProjectDB::as_select())
                    .load(conn)
            })?;

        // Parse the information into a shortened gRPC user_project info
        let user_proj = user
            .iter()
            .map(|proj| UserProject {
                id: proj.id.to_string(),
                name: proj.name.clone(),
                description: proj.description.clone(),
            })
            .collect::<Vec<_>>();

        // Return the gRPC response
        Ok(GetUserProjectsResponse {
            projects: user_proj,
        })
    }

    /// Returns all users that are not yet activated.
    ///
    /// ## Arguments
    ///
    /// * _request: GetNotActivatedUsersRequest: Placeholder, currently not in use.
    /// * _user_grpc_id: String: user_id validated by personal aruna_token or user_id specified by an admin
    ///
    /// ## Returns
    ///
    /// * Result<GetNotActivatedUsersResponse, ArunaError>: Contains a list of all users that are not yet activated
    ///
    pub fn get_not_activated_users(
        &self,
        _request: GetNotActivatedUsersRequest,
        _user_grpc_id: diesel_ulid::DieselUlid,
    ) -> Result<GetNotActivatedUsersResponse, ArunaError> {
        use crate::database::schema::users::dsl::*;
        use diesel::result::Error as dError;

        // Get all projects of a user based on the join of user_permissions and projects
        let ret_users = self
            .pg_connection
            .get()?
            .transaction::<Option<Vec<User>>, dError, _>(|conn| {
                users
                    .filter(active.eq(false))
                    .get_results::<User>(conn)
                    .optional()
            })?;

        // Parse the information into a shortened gRPC user_project info
        let grpc_users = match ret_users {
            Some(u) => u
                .iter()
                .map(|us| gRPCUser {
                    id: us.id.to_string(),
                    external_id: us.external_id.to_string(),
                    display_name: us.display_name.to_string(),
                    active: us.active,
                    is_admin: us.active,
                    is_service_account: us.is_service_account,
                    email: us.email.to_string(),
                })
                .collect::<Vec<_>>(),
            None => Vec::new(),
        };

        // Return the gRPC response
        Ok(GetNotActivatedUsersResponse { users: grpc_users })
    }
}
