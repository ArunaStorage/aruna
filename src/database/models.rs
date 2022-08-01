use super::schema::*;
use diesel_derive_enum::*;
use uuid;

#[derive(Debug, DbEnum)]
#[DieselTypePath = "sql_types::ObjectStatus"]
pub enum ObjectStatus {
    INITIALIZING,
    AVAILABLE,
    UNAVAILABLE,
    ERROR,
}

#[derive(Debug, DbEnum)]
#[DieselTypePath = "sql_types::EndpointType"]
pub enum EndpointType {
    INITIALIZING,
    AVAILABLE,
    UNAVAILABLE,
    ERROR,
}

#[derive(Debug, DbEnum)]
#[DieselTypePath = "sql_types::Dataclass"]
pub enum Dataclass {
    PUBLIC,
    PRIVATE,
    CONFIDENTIAL,
    PROTECTED,
}

#[derive(Debug, DbEnum)]
#[DieselTypePath = "sql_types::SourceType"]
pub enum SourceType {
    S3,
    URL,
    DOI,
}

#[derive(Debug, DbEnum)]
#[DieselTypePath = "sql_types::KeyValueType"]
pub enum KeyValueType {
    LABEL,
    URL,
}

#[derive(Debug, DbEnum)]
#[DieselTypePath = "sql_types::IdentityProviderType"]
pub enum IdentityProviderType {
    OIDC,
}

#[derive(Debug, DbEnum)]
#[DieselTypePath = "sql_types::UserRights"]
pub enum UserRights {
    READ,
    APPEND,
    MODIFY,
    WRITE,
    ADMIN,
}

#[derive(Debug, DbEnum)]
#[DieselTypePath = "sql_types::Resources"]
pub enum Resources {
    PROJECT,
    COLLECTION,
    OBJECT,
    OBJECTGROUP,
}

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

#[derive(Queryable, Insertable, Identifiable, Debug)]
#[diesel(table_name = collection_version)]
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
    pub dataclass: Dataclass,
    pub project_id: uuid::Uuid,
}

#[derive(Queryable, Insertable, Identifiable, Debug)]
#[diesel(table_name = collection_key_value)]
#[diesel(belongs_to(Collection))]
pub struct CollectionKeyValue {
    pub id: uuid::Uuid,
    pub collection_id: uuid::Uuid,
    pub key: String,
    pub value: String,
    pub key_value_type: KeyValueType,
}

#[derive(Queryable, Insertable, Identifiable, Debug)]
#[diesel(belongs_to(Collection))]
pub struct RequiredLabel {
    pub id: uuid::Uuid,
    pub collection_id: uuid::Uuid,
    pub label_key: String,
}

#[derive(Queryable, Insertable, Identifiable, Debug)]
pub struct Source {
    pub id: uuid::Uuid,
    pub link: String,
    pub source_type: SourceType,
}

#[derive(Queryable, Insertable, Identifiable, Debug)]
#[diesel(belongs_to(User))]
#[diesel(belongs_to(Source))]
#[diesel(belongs_to(Object))]
pub struct Object {
    pub id: uuid::Uuid,
    pub revision_number: i64,
    pub filename: String,
    pub created_at: Option<chrono::NaiveDate>,
    pub created_by: uuid::Uuid,
    pub content_len: i64,
    pub object_status: ObjectStatus,
    pub dataclass: Dataclass,
    pub source_id: uuid::Uuid,
    pub origin_id: Option<uuid::Uuid>,
    pub origin_revision: Option<i64>,
}

#[derive(Queryable, Insertable, Identifiable, Debug)]
pub struct Endpoint {
    pub id: uuid::Uuid,
    pub endpoint_type: EndpointType,
    pub proxy_hostname: String,
    pub internal_hostname: String,
    pub documentation_path: String,
    pub is_public: bool,
}

#[derive(Queryable, Insertable, Identifiable, Debug)]
#[diesel(belongs_to(Endpoint))]
#[diesel(belongs_to(Object))]
pub struct ObjectLocation {
    pub id: uuid::Uuid,
    pub bucket: String,
    pub path: String,
    pub endpoint_id: uuid::Uuid,
    pub object_id: uuid::Uuid,
    pub object_revision: i64,
    pub is_primary: bool,
}

#[derive(Queryable, Insertable, Identifiable, Debug)]
pub struct HashType {
    pub id: uuid::Uuid,
    pub name: String,
}

#[derive(Queryable, Insertable, Identifiable, Debug)]
#[diesel(belongs_to(Object))]
#[diesel(belongs_to(HashType))]
#[diesel(table_name = hashes)]
pub struct Hash {
    pub id: uuid::Uuid,
    pub hash: String,
    pub object_id: uuid::Uuid,
    pub object_revision: i64,
    pub hash_type: uuid::Uuid,
}

#[derive(Queryable, Insertable, Identifiable, Debug)]
#[diesel(table_name = object_key_value)]
#[diesel(belongs_to(Object))]
pub struct ObjectKeyValue {
    pub id: uuid::Uuid,
    pub object_id: uuid::Uuid,
    pub object_revision: i64,
    pub key: String,
    pub value: String,
    pub key_value_type: KeyValueType,
}

#[derive(Queryable, Insertable, Identifiable, Debug)]
#[diesel(belongs_to(User))]
pub struct ObjectGroup {
    pub id: uuid::Uuid,
    pub revision_number: i64,
    pub name: String,
    pub description: String,
    pub created_at: Option<chrono::NaiveDate>,
    pub created_by: uuid::Uuid,
}

#[derive(Queryable, Insertable, Identifiable, Debug)]
#[diesel(table_name = object_group_key_value)]
#[diesel(belongs_to(ObjectGroup))]
pub struct ObjectGroupKeyValue {
    pub id: uuid::Uuid,
    pub object_group_id: uuid::Uuid,
    pub object_group_revision: i64,
    pub key: String,
    pub value: String,
    pub key_value_type: KeyValueType,
}

#[derive(Queryable, Insertable, Identifiable, Debug)]
#[diesel(belongs_to(Collection))]
#[diesel(belongs_to(Object))]
pub struct CollectionObject {
    pub id: uuid::Uuid,
    pub collection_id: uuid::Uuid,
    pub object_id: uuid::Uuid,
    pub object_revision: i64,
    pub is_specification: bool,
    pub writeable: bool,
}

#[derive(Queryable, Insertable, Identifiable, Debug)]
#[diesel(belongs_to(Collection))]
#[diesel(belongs_to(ObjectGroup))]
pub struct CollectionObjectGroup {
    pub id: uuid::Uuid,
    pub collection_id: uuid::Uuid,
    pub object_group_id: uuid::Uuid,
    pub object_group_revision: i64,
    pub writeable: bool,
}

#[derive(Queryable, Insertable, Identifiable, Debug)]
#[diesel(belongs_to(ObjectGroup))]
#[diesel(belongs_to(Object))]
pub struct ObjectGroupObject {
    pub id: uuid::Uuid,
    pub object_group_id: uuid::Uuid,
    pub object_group_revision: i64,
    pub object_id: uuid::Uuid,
    pub object_revision: i64,
    pub is_meta: bool,
    pub writeable: bool,
}

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
    pub user_right: UserRights,
}

#[derive(Queryable, Insertable, Identifiable, Debug)]
pub struct NotificationStreamGroup {
    pub id: uuid::Uuid,
    pub subject: String,
    pub resource_id: uuid::Uuid,
    pub resource_type: Resources,
    pub notify_on_sub_resources: bool,
}
