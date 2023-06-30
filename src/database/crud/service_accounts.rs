use std::str::FromStr;

use aruna_rust_api::api::storage::{
    models::v1::Token,
    services::v1::{
        CreateServiceAccountRequest, CreateServiceAccountResponse,
        CreateServiceAccountTokenRequest, CreateServiceAccountTokenResponse, ServiceAccount,
    },
};
use chrono::Utc;
use diesel::{insert_into, BelongingToDsl, Connection, ExpressionMethods, QueryDsl, RunQueryDsl};
use diesel_ulid::DieselUlid;
use rand::{distributions::Alphanumeric, thread_rng, Rng};

use crate::{
    database::{
        connection::Database,
        crud::utils::map_permissions,
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
        creator: DieselUlid,
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
            creator_user_id: creator,
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
}
