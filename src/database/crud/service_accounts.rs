use std::str::FromStr;

use aruna_rust_api::api::storage::services::v1::{
    CreateServiceAccountRequest, CreateServiceAccountResponse, ServiceAccount,
};
use diesel::{insert_into, Connection, RunQueryDsl};

use crate::{
    database::{
        connection::Database,
        crud::utils::map_permissions,
        models::auth::{User, UserPermission},
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
}
