use super::collection::*;
use super::enums::*;
use crate::database::crud::utils::map_permissions_rev;
use crate::database::crud::utils::naivedatetime_to_prost_time;
use crate::database::crud::utils::option_to_string;
use crate::database::schema::*;
use crate::error::ArunaError;
use aruna_rust_api::api::storage::models::v1::Token;
use aruna_rust_api::api::storage::models::v1::TokenType;

#[derive(Queryable, Insertable, Identifiable, Debug)]
pub struct IdentityProvider {
    pub id: diesel_ulid::DieselUlid,
    pub name: String,
    pub idp_type: IdentityProviderType,
}

#[derive(Queryable, Insertable, Identifiable, Debug, Clone, Default)]
pub struct User {
    pub id: diesel_ulid::DieselUlid,
    pub external_id: String,
    pub display_name: String,
    pub active: bool,
    pub is_service_account: bool,
    pub email: String,
}

#[derive(Associations, Queryable, Insertable, Identifiable, Debug)]
#[diesel(belongs_to(IdentityProvider, foreign_key = idp_id))]
#[diesel(belongs_to(User))]
pub struct ExternalUserId {
    pub id: diesel_ulid::DieselUlid,
    pub user_id: diesel_ulid::DieselUlid,
    pub external_id: String,
    pub idp_id: diesel_ulid::DieselUlid,
}

#[derive(
    Associations, Queryable, Insertable, Identifiable, Selectable, QueryableByName, Debug, Clone,
)]
#[diesel(belongs_to(User, foreign_key = created_by))]
#[diesel(table_name = projects)]
pub struct Project {
    pub id: diesel_ulid::DieselUlid,
    pub name: String,
    pub description: String,
    pub flag: i64,
    pub created_at: chrono::NaiveDateTime,
    pub created_by: diesel_ulid::DieselUlid,
}

#[derive(Associations, Queryable, Insertable, Identifiable, Debug, Selectable, QueryableByName)]
#[diesel(table_name = user_permissions)]
#[diesel(belongs_to(User))]
#[diesel(belongs_to(Project))]
pub struct UserPermission {
    pub id: diesel_ulid::DieselUlid,
    pub user_id: diesel_ulid::DieselUlid,
    pub user_right: UserRights,
    pub project_id: diesel_ulid::DieselUlid,
}

/// Tokentypes:
/// Personal -> project_id && collection_id == None
///          -> Tokenpermission == Vec<UserPermission>
/// Scoped   -> project_id || collection_id != None
///          -> ApiToken.user_right

#[derive(Associations, Queryable, Insertable, Identifiable, Debug, Clone)]
#[diesel(belongs_to(User, foreign_key = creator_user_id))]
#[diesel(belongs_to(Project))]
#[diesel(belongs_to(Collection))]
#[diesel(belongs_to(PubKey, foreign_key = pub_key))]
pub struct ApiToken {
    pub id: diesel_ulid::DieselUlid,
    pub creator_user_id: diesel_ulid::DieselUlid,
    pub pub_key: i64,
    pub name: Option<String>,
    pub created_at: chrono::NaiveDateTime,
    pub expires_at: Option<chrono::NaiveDateTime>,
    pub project_id: Option<diesel_ulid::DieselUlid>,
    pub collection_id: Option<diesel_ulid::DieselUlid>,
    pub user_right: Option<UserRights>,
    pub secretkey: String,
    pub used_at: chrono::NaiveDateTime,
    pub is_session: bool,
}

#[derive(Queryable, Identifiable, Debug)]
pub struct PubKey {
    pub id: i64,
    pub pubkey: String,
}

#[derive(Insertable, Identifiable, Debug)]
#[diesel(table_name = pub_keys)]
pub struct PubKeyInsert {
    pub id: Option<i64>,
    pub pubkey: String,
}

impl TryFrom<ApiToken> for Token {
    type Error = ArunaError;

    fn try_from(value: ApiToken) -> Result<Self, Self::Error> {
        let token_type = if value.collection_id.is_some() || value.project_id.is_some() {
            TokenType::Scoped
        } else {
            TokenType::Personal
        };

        Ok(Token {
            id: value.id.to_string(),
            name: value.name.unwrap_or_default(),
            token_type: token_type as i32,
            created_at: Some(naivedatetime_to_prost_time(value.created_at)?),
            expires_at: value
                .expires_at
                .map(|v| naivedatetime_to_prost_time(v).ok())
                .flatten(),
            collection_id: option_to_string(value.collection_id).unwrap_or_default(),
            project_id: option_to_string(value.project_id).unwrap_or_default(),
            permission: map_permissions_rev(value.user_right),
            is_session: value.is_session,
            used_at: Some(naivedatetime_to_prost_time(value.used_at)?),
        })
    }
}
