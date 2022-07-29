use uuid;

use super::schema::external_user_ids;
use super::schema::identity_providers;
use super::schema::projects;
use super::schema::user_permissions;
use super::schema::users;
//
#[derive(Queryable, Insertable, Identifiable, Debug)]
pub struct IdentityProvider {
    pub id: uuid::Uuid,
    pub name: String,
    pub idp_type: String,
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
    pub user_right: String,
    pub project_id: uuid::Uuid,
}

#[derive(Queryable, Insertable, Identifiable, Debug)]
#[diesel(belongs_to(User))]
#[diesel(belongs_to(Project))]
pub struct UserPermission {
    pub id: uuid::Uuid,
    pub user_id: uuid::Uuid,
    pub user_right: String,
    pub project_id: uuid::Uuid,
}
// #[derive(Queryable, Insertable, Identifiable, Debug)]
// pub struct Label {
//     pub id: uuid::Uuid,
//     pub key: String,
//     pub value: String,
// }
//
// #[derive(Queryable, Insertable, Associations, Debug)]
// #[diesel(belongs_to(Collection))]
// #[diesel(belongs_to(Label))]
// pub struct CollectionLabel {
//     pub collection_id: uuid::Uuid,
//     pub label_id: uuid::Uuid,
// }
