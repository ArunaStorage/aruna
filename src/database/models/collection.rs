use super::enums::*;
use crate::database::schema::*;
use uuid;

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
    pub shared_version_id: uuid::Uuid,
    pub name: String,
    pub description: String,
    pub created_at: Option<chrono::NaiveDate>,
    pub created_by: uuid::Uuid,
    pub version_id: Option<uuid::Uuid>,
    pub dataclass: Option<Dataclass>,
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
#[diesel(belongs_to(Collection))]
#[diesel(belongs_to(Object))]
pub struct CollectionObject {
    pub id: uuid::Uuid,
    pub collection_id: uuid::Uuid,
    pub object_id: uuid::Uuid,
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
    pub writeable: bool,
}
