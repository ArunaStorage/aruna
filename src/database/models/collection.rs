use super::auth::*;
use super::enums::*;
use super::object::*;
use super::object_group::*;
use super::traits::IsKeyValue;
use super::traits::ToDbKeyValue;
use crate::database::schema::*;
use uuid;

#[derive(Queryable, Insertable, Identifiable, Debug, Clone)]
#[diesel(table_name = collection_version)]
pub struct CollectionVersion {
    pub id: uuid::Uuid,
    pub major: i64,
    pub minor: i64,
    pub patch: i64,
}

impl PartialEq for CollectionVersion {
    fn eq(&self, other: &Self) -> bool {
        self.major == other.major && self.minor == other.minor && self.patch == other.patch
    }
}

impl PartialOrd for CollectionVersion {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // Compare major first continue if equal
        match self.major.partial_cmp(&other.major) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => {
                return ord;
            }
        }
        // Compare minor continue if equal
        match self.minor.partial_cmp(&other.minor) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => {
                return ord;
            }
        }
        // Lastly compare patch
        self.patch.partial_cmp(&other.patch)
    }
}

#[derive(Associations, Queryable, Insertable, Identifiable, Debug, AsChangeset, Clone)]
#[diesel(belongs_to(User, foreign_key = created_by))]
#[diesel(belongs_to(CollectionVersion, foreign_key = version_id))]
#[diesel(belongs_to(Project))]
pub struct Collection {
    pub id: uuid::Uuid,
    pub shared_version_id: uuid::Uuid,
    pub name: String,
    pub description: String,
    pub created_at: chrono::NaiveDateTime,
    pub created_by: uuid::Uuid,
    pub version_id: Option<uuid::Uuid>,
    pub dataclass: Option<Dataclass>,
    pub project_id: uuid::Uuid,
}

#[derive(Associations, Queryable, Insertable, Identifiable, Debug, Clone)]
#[diesel(table_name = collection_key_value)]
#[diesel(belongs_to(Collection))]
pub struct CollectionKeyValue {
    pub id: uuid::Uuid,
    pub collection_id: uuid::Uuid,
    pub key: String,
    pub value: String,
    pub key_value_type: KeyValueType,
}

impl IsKeyValue for CollectionKeyValue {
    fn get_key(&self) -> &str {
        &self.key
    }

    fn get_value(&self) -> &str {
        &self.value
    }

    fn get_associated_uuid(&self) -> &uuid::Uuid {
        &self.collection_id
    }

    fn get_type(&self) -> &KeyValueType {
        &self.key_value_type
    }
}
impl ToDbKeyValue for CollectionKeyValue {
    fn new_kv<CollectionKeyValue>(
        key: &str,
        value: &str,
        belongs_to: uuid::Uuid,
        kv_type: KeyValueType,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4(),
            collection_id: belongs_to,
            key: key.to_string(),
            value: value.to_string(),
            key_value_type: kv_type,
        }
    }
}

#[derive(Associations, Queryable, Insertable, Identifiable, Debug, Clone)]
#[diesel(belongs_to(Collection))]
pub struct RequiredLabel {
    pub id: uuid::Uuid,
    pub collection_id: uuid::Uuid,
    pub label_key: String,
}

#[derive(
    Associations, Queryable, Insertable, Identifiable, Debug, Clone, Selectable, PartialEq, Eq,
)]
#[diesel(belongs_to(Collection))]
#[diesel(belongs_to(Object))]
#[diesel(table_name = collection_objects)]
pub struct CollectionObject {
    pub id: uuid::Uuid,
    pub collection_id: uuid::Uuid,
    pub object_id: uuid::Uuid,
    pub is_latest: bool,
    pub auto_update: bool,
    pub is_specification: bool,
    pub writeable: bool,
    pub reference_status: ReferenceStatus,
}

#[derive(Associations, Queryable, Insertable, Identifiable, Debug, Clone)]
#[diesel(belongs_to(Collection))]
#[diesel(belongs_to(ObjectGroup))]
pub struct CollectionObjectGroup {
    pub id: uuid::Uuid,
    pub collection_id: uuid::Uuid,
    pub object_group_id: uuid::Uuid,
    pub writeable: bool,
}
