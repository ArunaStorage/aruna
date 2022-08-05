use super::enums::*;
use crate::database::schema::*;
use uuid;

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
    pub shared_revision_id: uuid::Uuid,
    pub revision_number: i64,
    pub filename: String,
    pub created_at: chrono::NaiveDateTime,
    pub created_by: uuid::Uuid,
    pub content_len: i64,
    pub object_status: ObjectStatus,
    pub dataclass: Dataclass,
    pub source_id: Option<uuid::Uuid>,
    pub origin_id: Option<uuid::Uuid>,
}

#[derive(Queryable, Insertable, Identifiable, Debug)]
pub struct Endpoint {
    pub id: uuid::Uuid,
    pub endpoint_type: EndpointType,
    pub proxy_hostname: String,
    pub internal_hostname: String,
    pub documentation_path: Option<String>,
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
    pub hash_type: uuid::Uuid,
}

#[derive(Queryable, Insertable, Identifiable, Debug)]
#[diesel(table_name = object_key_value)]
#[diesel(belongs_to(Object))]
pub struct ObjectKeyValue {
    pub id: uuid::Uuid,
    pub object_id: uuid::Uuid,
    pub key: String,
    pub value: String,
    pub key_value_type: KeyValueType,
}
