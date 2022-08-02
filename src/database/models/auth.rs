use super::enums::*;
use crate::database::schema::*;
use uuid;

#[derive(Queryable, Insertable, Identifiable, Debug)]
pub struct IdentityProvider {
    pub id: uuid::Uuid,
    pub name: String,
    pub idp_type: IdentityProviderType,
}

#[derive(Queryable, Insertable, Identifiable, Debug)]
pub struct User {
    pub id: uuid::Uuid,
    pub display_name: String,
    pub active: bool,
}

#[derive(Queryable, Insertable, Identifiable, Debug)]
#[diesel(belongs_to(IdentityProvider))]
#[diesel(belongs_to(User))]
pub struct ExternalUserId {
    pub id: uuid::Uuid,
    pub user_id: uuid::Uuid,
    pub external_id: String,
    pub idp_id: uuid::Uuid,
}

#[derive(Queryable, Insertable, Identifiable, Debug)]
#[diesel(belongs_to(User))]
pub struct Project {
    pub id: uuid::Uuid,
    pub name: String,
    pub description: String,
    pub created_at: Option<chrono::NaiveDate>,
    pub created_by: uuid::Uuid,
}

#[derive(Queryable, Insertable, Identifiable, Debug)]
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

#[derive(Queryable, Insertable, Identifiable, Debug)]
#[diesel(belongs_to(User))]
#[diesel(belongs_to(Project))]
#[diesel(belongs_to(Collection))]
pub struct ApiToken {
    pub id: uuid::Uuid,
    pub creator_user_id: uuid::Uuid,
    pub token: String,
    pub created_at: chrono::NaiveDate,
    pub expires_at: Option<chrono::NaiveDate>,
    pub project_id: Option<uuid::Uuid>,
    pub collection_id: Option<uuid::Uuid>,
    pub user_right: Option<UserRights>,
}
