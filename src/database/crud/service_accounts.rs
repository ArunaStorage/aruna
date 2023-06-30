use std::str::FromStr;

use aruna_rust_api::api::storage::{
    models::v1::Token,
    services::v1::{
        CreateServiceAccountRequest, CreateServiceAccountResponse,
        CreateServiceAccountTokenRequest, CreateServiceAccountTokenResponse,
        DeleteServiceAccountRequest, DeleteServiceAccountResponse,
        DeleteServiceAccountTokenRequest, DeleteServiceAccountTokenResponse,
        DeleteServiceAccountTokensRequest, DeleteServiceAccountTokensResponse,
        GetServiceAccountTokenRequest, GetServiceAccountTokenResponse,
        GetServiceAccountTokensRequest, GetServiceAccountTokensResponse,
        GetServiceAccountsByProjectRequest, GetServiceAccountsByProjectResponse, ServiceAccount,
        SetServiceAccountPermissionRequest, SetServiceAccountPermissionResponse,
    },
};
use chrono::Utc;
use diesel::{
    delete, insert_into, BelongingToDsl, Connection, ExpressionMethods, QueryDsl, RunQueryDsl,
};
use diesel_ulid::DieselUlid;
use rand::{distributions::Alphanumeric, thread_rng, Rng};

use crate::{
    database::{
        connection::Database,
        crud::utils::{map_permissions, map_permissions_rev},
        models::auth::{ApiToken, User, UserPermission},
    },
    error::ArunaError,
};

impl Database {
    pub fn create_service_account(
        &self,
        request: CreateServiceAccountRequest,
    ) -> Result<CreateServiceAccountResponse, ArunaError> {
        use crate::database::schema::user_permissions::dsl::*;
        use crate::database::schema::users::dsl::*;

        // Create a new DB user object
        let db_user = User {
            id: diesel_ulid::DieselUlid::generate(),
            display_name: request.name.to_string(),
            external_id: "".to_string(),
            active: true,
            is_service_account: true,
            email: "".to_string(),
        };

        let user_perm = UserPermission {
            id: diesel_ulid::DieselUlid::generate(),
            user_id: db_user.id,
            user_right: map_permissions(request.permission().clone()).ok_or(
                ArunaError::InvalidRequest("Invalid svc account permission".to_string()),
            )?,
            project_id: diesel_ulid::DieselUlid::from_str(&request.project_id)?,
        };

        // Insert the user and return the user_id
        let uid = self
            .pg_connection
            .get()?
            .transaction::<diesel_ulid::DieselUlid, ArunaError, _>(|conn| {
                let uid = insert_into(users)
                    .values(&db_user)
                    .returning(crate::database::schema::users::id)
                    .get_result(conn)?;
                insert_into(user_permissions)
                    .values(&user_perm)
                    .execute(conn)?;
                Ok(uid)
            })?;

        Ok(CreateServiceAccountResponse {
            service_account: Some(ServiceAccount {
                svc_account_id: uid.to_string(),
                project_id: request.project_id.to_string(),
                name: request.name.to_string(),
                permission: request.permission.clone(),
            }),
        })
    }

    pub fn create_service_account_token(
        &self,
        request: CreateServiceAccountTokenRequest,
        pubkey_ref: i64,
    ) -> Result<CreateServiceAccountTokenResponse, ArunaError> {
        use crate::database::schema::api_tokens::dsl::*;
        use crate::database::schema::users::dsl::*;

        // Get the svc account ulid
        let svc_account_id = diesel_ulid::DieselUlid::from_str(&request.svc_account_id)?;

        let exp_time = match &request.expires_at {
            Some(t) => {
                Some(chrono::NaiveDateTime::from_timestamp_opt(t.seconds, 0).unwrap_or_default())
            }
            None => None,
        };

        // Create random access_key
        let secret_key: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(30)
            .map(char::from)
            .collect();

        let perm_to_insert = map_permissions(request.permission());

        let (proj, col) = match request.collection_id.is_empty() {
            true => match perm_to_insert {
                Some(_) => (Some(DieselUlid::from_str(&request.project_id)?), None),
                None => (None, None),
            },
            false => (None, Some(DieselUlid::from_str(&request.collection_id)?)),
        };

        let token_to_insert = ApiToken {
            id: DieselUlid::generate(),
            creator_user_id: svc_account_id,
            pub_key: pubkey_ref,
            name: match request.name.is_empty() {
                true => None,
                false => Some(request.name.to_string()),
            },
            created_at: Utc::now().naive_local(),
            expires_at: exp_time,
            project_id: proj,
            collection_id: col,
            user_right: perm_to_insert.clone(),
            secretkey: secret_key,
            used_at: Utc::now().naive_local(),
            is_session: false,
        };

        // Insert the user and return the user_id
        self
            .pg_connection
            .get()?
            .transaction::<(), ArunaError, _>(|conn| {
                let account: User = users
                    .filter(crate::database::schema::users::id.eq(svc_account_id))
                    .first::<User>(conn)?;
                if !account.is_service_account {
                    return Err(ArunaError::InvalidRequest(
                        "Account is no svc account".to_string(),
                    ));
                }

                let perm: UserPermission =
                    UserPermission::belonging_to(&account).first::<UserPermission>(conn)?;
                if let Some(p) = perm_to_insert {
                    if proj.is_some() {
                        if perm.user_right < p {
                            return Err(ArunaError::InvalidRequest("Invalid permission, service account has lower global project permissions.".to_string()))
                        }
                    }
                };

                insert_into(api_tokens).values(&token_to_insert).execute(conn)?;

                Ok(())
            })?;

        Ok(CreateServiceAccountTokenResponse {
            token: Some(Token::try_from(token_to_insert.clone())?),
            token_secret: String::new(),
            s3_access_key: token_to_insert.id.to_string(),
            s3_secret_key: token_to_insert.secretkey.to_string(),
        })
    }

    pub fn set_service_account_permission(
        &self,
        request: SetServiceAccountPermissionRequest,
    ) -> Result<SetServiceAccountPermissionResponse, ArunaError> {
        use crate::database::schema::user_permissions::dsl::*;
        use crate::database::schema::users::dsl::*;

        let account_id = DieselUlid::from_str(&request.svc_account_id)?;
        let new_perm = map_permissions(request.new_permission()).ok_or_else(|| {
            ArunaError::InvalidRequest(
                "Invalid permission handle for svc account update".to_string(),
            )
        })?;

        // Insert the user and return the user_id
        self.pg_connection
            .get()?
            .transaction::<SetServiceAccountPermissionResponse, ArunaError, _>(|conn| {
                let old_permission: UserPermission = user_permissions
                    .filter(crate::database::schema::user_permissions::user_id.eq(&account_id))
                    .first::<UserPermission>(conn)?;

                let new_perm = diesel::update(user_permissions)
                    .filter(crate::database::schema::user_permissions::id.eq(&old_permission.id))
                    .set(crate::database::schema::user_permissions::user_right.eq(new_perm))
                    .get_result::<UserPermission>(conn)?;

                let get_svc_account: User = users
                    .filter(crate::database::schema::users::id.eq(&new_perm.user_id))
                    .first::<User>(conn)?;

                Ok(SetServiceAccountPermissionResponse {
                    service_account: Some(ServiceAccount {
                        svc_account_id: new_perm.user_id.to_string(),
                        project_id: new_perm.project_id.to_string(),
                        name: get_svc_account.display_name,
                        permission: request.new_permission,
                    }),
                })
            })
    }

    pub fn get_service_account_token(
        &self,
        request: GetServiceAccountTokenRequest,
    ) -> Result<GetServiceAccountTokenResponse, ArunaError> {
        use crate::database::schema::api_tokens::dsl::*;

        let account_id = DieselUlid::from_str(&request.svc_account_id)?;
        let token_id = DieselUlid::from_str(&request.token_id)?;

        // Insert the user and return the user_id
        self.pg_connection
            .get()?
            .transaction::<GetServiceAccountTokenResponse, ArunaError, _>(|conn| {
                let atoken: ApiToken = api_tokens
                    .filter(crate::database::schema::api_tokens::id.eq(&token_id))
                    .filter(crate::database::schema::api_tokens::creator_user_id.eq(&account_id))
                    .first::<ApiToken>(conn)?;

                Ok(GetServiceAccountTokenResponse {
                    token: Some(Token::try_from(atoken)?),
                })
            })
    }

    pub fn get_service_account_tokens(
        &self,
        request: GetServiceAccountTokensRequest,
    ) -> Result<GetServiceAccountTokensResponse, ArunaError> {
        use crate::database::schema::api_tokens::dsl::*;

        let account_id = DieselUlid::from_str(&request.svc_account_id)?;

        // Insert the user and return the user_id
        self.pg_connection
            .get()?
            .transaction::<GetServiceAccountTokensResponse, ArunaError, _>(|conn| {
                let atoken: Vec<ApiToken> = api_tokens
                    .filter(crate::database::schema::api_tokens::creator_user_id.eq(&account_id))
                    .load::<ApiToken>(conn)?;
                let as_grpc = atoken
                    .into_iter()
                    .map(Token::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(GetServiceAccountTokensResponse { token: as_grpc })
            })
    }

    pub fn get_service_accounts_by_project(
        &self,
        request: GetServiceAccountsByProjectRequest,
    ) -> Result<GetServiceAccountsByProjectResponse, ArunaError> {
        use crate::database::schema::user_permissions::dsl::*;
        use crate::database::schema::users::dsl::*;

        let parse_p_id = DieselUlid::from_str(&request.project_id)?;

        // Insert the user and return the user_id
        self.pg_connection
            .get()?
            .transaction::<GetServiceAccountsByProjectResponse, ArunaError, _>(|conn| {
                let perms: Vec<UserPermission> = user_permissions
                    .filter(crate::database::schema::user_permissions::project_id.eq(&parse_p_id))
                    .load::<UserPermission>(conn)?;

                let uids = perms.iter().map(|e| e.id).collect::<Vec<_>>();

                let svc_acc_users: Vec<User> = users
                    .filter(crate::database::schema::users::id.eq_any(&uids))
                    .filter(crate::database::schema::users::is_service_account.eq(true))
                    .load::<User>(conn)?;

                let mut mapped_svc_accounts = Vec::with_capacity(svc_acc_users.len());

                'outer: for perm in perms {
                    for svc in &svc_acc_users {
                        if perm.user_id == svc.id {
                            mapped_svc_accounts.push(ServiceAccount {
                                svc_account_id: svc.id.to_string(),
                                project_id: parse_p_id.to_string(),
                                name: svc.display_name.to_string(),
                                permission: map_permissions_rev(Some(perm.user_right)),
                            });
                            continue 'outer;
                        }
                    }
                }
                Ok(GetServiceAccountsByProjectResponse {
                    svc_accounts: mapped_svc_accounts,
                })
            })
    }

    pub fn delete_service_account_token(
        &self,
        request: DeleteServiceAccountTokenRequest,
    ) -> Result<DeleteServiceAccountTokenResponse, ArunaError> {
        use crate::database::schema::api_tokens::dsl::*;

        let svc_account = DieselUlid::from_str(&request.svc_account_id)?;
        let token_id = DieselUlid::from_str(&request.token_id)?;

        // Insert the user and return the user_id
        self.pg_connection
            .get()?
            .transaction::<DeleteServiceAccountTokenResponse, ArunaError, _>(|conn| {
                delete(api_tokens)
                    .filter(crate::database::schema::api_tokens::id.eq(&token_id))
                    .filter(crate::database::schema::api_tokens::creator_user_id.eq(&svc_account))
                    .execute(conn)?;
                Ok(DeleteServiceAccountTokenResponse {})
            })
    }

    pub fn delete_service_account_tokens(
        &self,
        request: DeleteServiceAccountTokensRequest,
    ) -> Result<DeleteServiceAccountTokensResponse, ArunaError> {
        use crate::database::schema::api_tokens::dsl::*;

        let svc_account = DieselUlid::from_str(&request.svc_account_id)?;

        // Insert the user and return the user_id
        self.pg_connection
            .get()?
            .transaction::<DeleteServiceAccountTokensResponse, ArunaError, _>(|conn| {
                delete(api_tokens)
                    .filter(crate::database::schema::api_tokens::creator_user_id.eq(&svc_account))
                    .execute(conn)?;
                Ok(DeleteServiceAccountTokensResponse {})
            })
    }

    pub fn delete_service_account(
        &self,
        request: DeleteServiceAccountRequest,
    ) -> Result<DeleteServiceAccountResponse, ArunaError> {
        use crate::database::schema::api_tokens::dsl::*;
        use crate::database::schema::user_permissions::dsl::*;
        use crate::database::schema::users::dsl::*;

        let svc_account = DieselUlid::from_str(&request.svc_account_id)?;

        // Insert the user and return the user_id
        self.pg_connection
            .get()?
            .transaction::<DeleteServiceAccountResponse, ArunaError, _>(|conn| {
                delete(api_tokens)
                    .filter(crate::database::schema::api_tokens::creator_user_id.eq(&svc_account))
                    .execute(conn)?;

                delete(user_permissions)
                    .filter(crate::database::schema::user_permissions::user_id.eq(&svc_account))
                    .execute(conn)?;

                delete(users)
                    .filter(crate::database::schema::users::id.eq(&svc_account))
                    .execute(conn)?;

                Ok(DeleteServiceAccountResponse {})
            })
    }
}
