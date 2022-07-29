use uuid;

use super::schema::*;
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
#[table_name = "collection_version"]
#[diesel(belongs_to(Collection))]
pub struct CollectionVersion {
    pub id: uuid::Uuid,
    pub major: i64,
    pub minor: i64,
    pub patch: i64,
}

#[derive(Queryable, Insertable, Identifiable, Debug)]
#[diesel(belongs_to(User))]
#[diesel(belongs_to(CollectionVersion))]
#[diesel(belongs_to(Project))]
pub struct Collection {
    pub id: uuid::Uuid,
    pub name: String,
    pub description: String,
    pub created_at: Option<chrono::NaiveDate>,
    pub created_by: uuid::Uuid,
    pub version_id: uuid::Uuid,
    pub dataclass: String,
    pub project_id: uuid::Uuid,
}

#[derive(Queryable, Insertable, Identifiable, Debug)]
#[table_name = "collection_key_value"]
#[diesel(belongs_to(Collection))]
pub struct CollectionKeyValue {
    pub id: uuid::Uuid,
    pub collection_id: uuid::Uuid,
    pub key: String,
    pub value: String,
    pub key_value_type: String,
}
