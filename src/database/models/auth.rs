use super::collection::*;
use super::enums::*;
use uuid;

use crate::database::schema::*;

#[derive(Queryable, Insertable, Identifiable, Debug)]
pub struct IdentityProvider {
    pub id: uuid::Uuid,
    pub name: String,
    pub idp_type: IdentityProviderType,
}

#[derive(Queryable, Insertable, Identifiable, Debug)]
pub struct User {
    pub id: uuid::Uuid,
    pub external_id: String,
    pub display_name: String,
    pub active: bool,
}

#[derive(Associations, Queryable, Insertable, Identifiable, Debug)]
#[diesel(belongs_to(IdentityProvider, foreign_key = idp_id))]
#[diesel(belongs_to(User))]
pub struct ExternalUserId {
    pub id: uuid::Uuid,
    pub user_id: uuid::Uuid,
    pub external_id: String,
    pub idp_id: uuid::Uuid,
}

#[derive(Associations, Queryable, Insertable, Identifiable, Selectable, QueryableByName, Debug)]
#[diesel(belongs_to(User, foreign_key = created_by))]
#[diesel(table_name = projects)]
pub struct Project {
    pub id: uuid::Uuid,
    pub name: String,
    pub description: String,
    pub flag: i64,
    pub created_at: chrono::NaiveDateTime,
    pub created_by: uuid::Uuid,
}

#[derive(Associations, Queryable, Insertable, Identifiable, Debug, Selectable, QueryableByName)]
#[diesel(table_name = user_permissions)]
#[diesel(belongs_to(User))]
#[diesel(belongs_to(Project))]
pub struct UserPermission {
    pub id: uuid::Uuid,
    pub user_id: uuid::Uuid,
    pub user_right: UserRights,
    pub project_id: uuid::Uuid,
}

/// Tokentypes:
/// Personal -> project_id && collection_id == None
///          -> Tokenpermission == Vec<UserPermission>
/// Scoped   -> project_id || collection_id != None
///          -> ApiToken.user_right

#[derive(Associations, Queryable, Insertable, Identifiable, Debug)]
#[diesel(belongs_to(User, foreign_key = creator_user_id))]
#[diesel(belongs_to(Project))]
#[diesel(belongs_to(Collection))]
#[diesel(belongs_to(PubKey, foreign_key = pub_key))]
pub struct ApiToken {
    pub id: uuid::Uuid,
    pub creator_user_id: uuid::Uuid,
    pub pub_key: i64,
    pub name: Option<String>,
    pub created_at: chrono::NaiveDateTime,
    pub expires_at: Option<chrono::NaiveDateTime>,
    pub project_id: Option<uuid::Uuid>,
    pub collection_id: Option<uuid::Uuid>,
    pub user_right: Option<UserRights>,
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