use super::auth::*;
use super::enums::*;
use super::traits::IsKeyValue;
use super::traits::ToDbKeyValue;
use crate::database::schema::*;
use uuid;

#[derive(Queryable, Insertable, Identifiable, Debug, Clone)]
pub struct Source {
    pub id: uuid::Uuid,
    pub link: String,
    pub source_type: SourceType,
}

#[derive(Associations, Queryable, Insertable, Identifiable, Debug, Selectable, Clone)]
#[diesel(belongs_to(User, foreign_key = created_by))]
#[diesel(belongs_to(Source))]
#[diesel(belongs_to(Object, foreign_key = origin_id))]
#[diesel(table_name = objects)]
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

#[derive(Queryable, Insertable, Identifiable, Clone, Debug)]
pub struct Endpoint {
    pub id: uuid::Uuid,
    pub endpoint_type: EndpointType,
    pub proxy_hostname: String,
    pub name: String,
    pub internal_hostname: String,
    pub documentation_path: Option<String>,
    pub is_public: bool,
}

#[derive(Associations, Queryable, Insertable, Identifiable, Debug)]
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

#[derive(Associations, Queryable, Insertable, Identifiable, Debug, Clone)]
#[diesel(belongs_to(Object))]
#[diesel(table_name = hashes)]
pub struct Hash {
    pub id: uuid::Uuid,
    pub hash: String,
    pub object_id: uuid::Uuid,
    pub hash_type: HashType,
}

#[derive(Associations, Queryable, Insertable, Identifiable, Debug)]
#[diesel(table_name = object_key_value)]
#[diesel(belongs_to(Object))]
pub struct ObjectKeyValue {
    pub id: uuid::Uuid,
    pub object_id: uuid::Uuid,
    pub key: String,
    pub value: String,
    pub key_value_type: KeyValueType,
}

impl IsKeyValue for ObjectKeyValue {
    fn get_key(&self) -> &str {
        &self.key
    }

    fn get_value(&self) -> &str {
        &self.value
    }

    fn get_associated_uuid(&self) -> &uuid::Uuid {
        &self.object_id
    }

    fn get_type(&self) -> &KeyValueType {
        &self.key_value_type
    }
}

impl ToDbKeyValue for ObjectKeyValue {
    fn new_kv<ObjectKeyValue>(
        key: &str,
        value: &str,
        belongs_to: uuid::Uuid,
        kv_type: KeyValueType,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4(),
            object_id: belongs_to,
            key: key.to_string(),
            value: value.to_string(),
            key_value_type: kv_type,
        }
    }
}
