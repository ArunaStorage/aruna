use super::utils::*;
use crate::api::aruna::api::storage::models::v1::{Token, TokenType};
use crate::api::aruna::api::storage::services::v1::{
    CreateApiTokenRequest, CreateApiTokenResponse, GetApiTokenRequest, GetApiTokenResponse,
    GetApiTokensRequest, GetApiTokensResponse, RegisterUserRequest, RegisterUserResponse,
};
use crate::database::connection::Database;
use crate::database::models::auth::{ApiToken, User};

use crate::database::models::enums::UserRights;
use crate::error::ArunaError;

use chrono::Local;
use diesel::insert_into;
use diesel::prelude::*;

impl Database {
    /// Registers a new (unregistered) user by its oidc `external_id`
    /// This must be called once in order to complete the registration for Aruna
    /// Users can provide an optional display_name to use e.g. for websites clients etc.
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
    //

    pub fn register_user(
        &self,
        request: RegisterUserRequest,
        ext_id: String,
    ) -> Result<RegisterUserResponse, ArunaError> {
        use crate::database::schema::users::dsl::*;
        use diesel::result::Error;

        let db_user = User {
            id: uuid::Uuid::new_v4(),
            display_name: request.display_name,
            external_id: ext_id,
            active: false,
        };

        let user_id = self
            .pg_connection
            .get()?
            .transaction::<uuid::Uuid, Error, _>(|conn| {
                insert_into(users)
                    .values(&db_user)
                    .returning(id)
                    .get_result(conn)
            })?;

        Ok(RegisterUserResponse {
            user_id: user_id.to_string(),
        })
    }

    pub fn create_api_token(
        &self,
        request: CreateApiTokenRequest,
        user_id: uuid::Uuid,
        pubkey_id: i64,
    ) -> Result<CreateApiTokenResponse, ArunaError> {
        let new_uid = uuid::Uuid::new_v4();

        let expiry_time = request.expires_at.clone();
        let exp_time = match expiry_time {
            Some(t) => t
                .timestamp
                .map(|t| chrono::NaiveDateTime::from_timestamp(t.seconds, 0)),
            None => None,
        };

        let parsed_project_id = uuid::Uuid::parse_str(&request.project_id).ok();
        let parsed_collection_id = uuid::Uuid::parse_str(&request.collection_id).ok();
        let user_right_db: Option<UserRights> = map_permissions(request.permission());
        let mut token_type = TokenType::Personal;
        if parsed_collection_id.is_some() || parsed_project_id.is_some() {
            token_type = TokenType::Scoped;
            // Only allow one of both to be set
            if parsed_collection_id.is_some() == parsed_project_id.is_some() {
                return Err(ArunaError::InvalidRequest(
                    "Cannot set collection_id and project_id at once both should be exclusive"
                        .to_owned(),
                ));
            }
        }

        let new_token = ApiToken {
            id: new_uid,
            creator_user_id: user_id,
            name: Some(request.name),
            pub_key: pubkey_id,
            created_at: Local::now().naive_local(),
            expires_at: exp_time,
            project_id: parsed_project_id,
            collection_id: parsed_collection_id,
            user_right: user_right_db,
        };

        use crate::database::schema::api_tokens::dsl::*;
        use diesel::result::Error;

        let api_token = self
            .pg_connection
            .get()?
            .transaction::<ApiToken, Error, _>(|conn| {
                insert_into(api_tokens).values(&new_token).get_result(conn)
            })?;

        let expires_at_time = match api_token.expires_at {
            Some(time) => Some(naivedatetime_to_prost_time(time)?),
            None => None,
        };

        Ok(CreateApiTokenResponse {
            token: Some(Token {
                id: api_token.id.to_string(),
                name: api_token.name.unwrap_or_default(),
                token_type: token_type as i32,
                created_at: Some(naivedatetime_to_prost_time(api_token.created_at)?),
                expires_at: expires_at_time,
                collection_id: option_to_string(api_token.collection_id).unwrap_or_default(),
                project_id: option_to_string(api_token.project_id).unwrap_or_default(),
                permission: map_permissions_rev(api_token.user_right),
            }),
        })
    }

    pub fn get_api_token(
        &self,
        request: GetApiTokenRequest,
        user_id: uuid::Uuid,
    ) -> Result<GetApiTokenResponse, ArunaError> {
        use crate::database::schema::api_tokens::dsl::*;
        use diesel::result::Error as dError;

        let token_id = if !request.token_id.is_empty() {
            Some(uuid::Uuid::parse_str(&request.token_id)?)
        } else {
            None
        };

        let token_name = if !request.name.is_empty() {
            Some(request.name)
        } else {
            None
        };

        if token_id.is_none() && token_name.is_none() {
            return Err(ArunaError::InvalidRequest("token_id or name for the token must be specified use: get_api_tokens for a list of all tokens".to_string()));
        }

        let api_token = self
            .pg_connection
            .get()?
            .transaction::<ApiToken, dError, _>(|conn| {
                if token_id.is_some() {
                    api_tokens
                        .filter(id.eq(token_id.unwrap_or_default()))
                        .filter(creator_user_id.eq(user_id))
                        .first::<ApiToken>(conn)
                } else if token_name.is_some() {
                    api_tokens
                        .filter(name.eq(token_name.unwrap_or_default()))
                        .filter(creator_user_id.eq(user_id))
                        .first::<ApiToken>(conn)
                } else {
                    Err(dError::NotFound)
                }
            })?;

        let expires_at_time = match api_token.expires_at {
            Some(time) => Some(naivedatetime_to_prost_time(time)?),
            None => None,
        };

        let token_type = if api_token.collection_id.is_some() || api_token.project_id.is_some() {
            2
        } else {
            1
        };

        Ok(GetApiTokenResponse {
            token: Some(Token {
                id: api_token.id.to_string(),
                name: api_token.name.unwrap_or_default(),
                token_type: token_type as i32,
                created_at: Some(naivedatetime_to_prost_time(api_token.created_at)?),
                expires_at: expires_at_time,
                collection_id: option_to_string(api_token.collection_id).unwrap_or_default(),
                project_id: option_to_string(api_token.project_id).unwrap_or_default(),
                permission: map_permissions_rev(api_token.user_right),
            }),
        })
    }

    pub fn get_api_tokens(
        &self,
        _request: GetApiTokensRequest,
        user_id: uuid::Uuid,
    ) -> Result<GetApiTokensResponse, ArunaError> {
        use crate::database::schema::api_tokens::dsl::*;
        use diesel::result::Error as dError;

        let atoken_result = self
            .pg_connection
            .get()?
            .transaction::<Vec<ApiToken>, dError, _>(|conn| {
                api_tokens
                    .filter(creator_user_id.eq(user_id))
                    .load::<ApiToken>(conn)
            })?;

        let converted = atoken_result
            .iter()
            .map(|api_token| {
                let expires_at_time = match api_token.expires_at {
                    Some(time) => Some(naivedatetime_to_prost_time(time)?),
                    None => None,
                };

                let token_type =
                    if api_token.collection_id.is_some() || api_token.project_id.is_some() {
                        2
                    } else {
                        1
                    };

                Ok(Token {
                    id: api_token.id.to_string(),
                    // Abomination made by borrow_checker
                    // if someone knows a better way, feel free to add a PR
                    name: api_token
                        .name
                        .as_ref()
                        .unwrap_or(&"".to_string())
                        .to_string(),
                    token_type: token_type as i32,
                    created_at: Some(naivedatetime_to_prost_time(api_token.created_at)?),
                    expires_at: expires_at_time,
                    collection_id: option_to_string(api_token.collection_id).unwrap_or_default(),
                    project_id: option_to_string(api_token.project_id).unwrap_or_default(),
                    permission: map_permissions_rev(api_token.user_right),
                })
            })
            .collect::<Result<Vec<_>, ArunaError>>()?;

        Ok(GetApiTokensResponse { token: converted })
    }
}
