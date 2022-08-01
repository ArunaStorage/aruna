use super::enums::*;
use crate::database::schema::*;
use uuid;

#[derive(Queryable, Insertable, Identifiable, Debug)]
#[diesel(belongs_to(User))]
pub struct ObjectGroup {
    pub id: uuid::Uuid,
    pub shared_revision_id: uuid::Uuid,
    pub revision_number: i64,
    pub name: Option<String>,
    pub description: Option<String>,
    pub created_at: Option<chrono::NaiveDate>,
    pub created_by: uuid::Uuid,
}

#[derive(Queryable, Insertable, Identifiable, Debug)]
#[diesel(table_name = object_group_key_value)]
#[diesel(belongs_to(ObjectGroup))]
pub struct ObjectGroupKeyValue {
    pub id: uuid::Uuid,
    pub object_group_id: uuid::Uuid,
    pub key: String,
    pub value: String,
    pub key_value_type: KeyValueType,
}

#[derive(Queryable, Insertable, Identifiable, Debug)]
#[diesel(belongs_to(ObjectGroup))]
#[diesel(belongs_to(Object))]
pub struct ObjectGroupObject {
    pub id: uuid::Uuid,
    pub object_group_id: uuid::Uuid,
    pub object_id: uuid::Uuid,
    pub is_meta: bool,
    pub writeable: bool,
}
