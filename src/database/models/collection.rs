use super::auth::*;
use super::enums::*;
use super::object::*;
use super::object_group::*;
use super::traits::IsKeyValue;
use super::traits::ToDbKeyValue;
use crate::database::schema::*;

#[derive(Queryable, Insertable, Identifiable, Debug, Clone, Selectable)]
#[diesel(table_name = collection_version)]
pub struct CollectionVersion {
    pub id: diesel_ulid::DieselUlid,
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
    pub id: diesel_ulid::DieselUlid,
    pub shared_version_id: diesel_ulid::DieselUlid,
    pub name: String,
    pub description: String,
    pub created_at: chrono::NaiveDateTime,
    pub created_by: diesel_ulid::DieselUlid,
    pub version_id: Option<diesel_ulid::DieselUlid>,
    pub dataclass: Option<Dataclass>,
    pub project_id: diesel_ulid::DieselUlid,
}

#[derive(Associations, Queryable, Insertable, Identifiable, Debug, Clone)]
#[diesel(table_name = collection_key_value)]
#[diesel(belongs_to(Collection))]
pub struct CollectionKeyValue {
    pub id: diesel_ulid::DieselUlid,
    pub collection_id: diesel_ulid::DieselUlid,
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

    fn get_associated_uuid(&self) -> &diesel_ulid::DieselUlid {
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
        belongs_to: diesel_ulid::DieselUlid,
        kv_type: KeyValueType,
    ) -> Self {
        Self {
            id: diesel_ulid::DieselUlid::generate(),
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
    pub id: diesel_ulid::DieselUlid,
    pub collection_id: diesel_ulid::DieselUlid,
    pub label_key: String,
}

#[derive(
    Associations, Queryable, Insertable, Identifiable, Debug, Clone, Selectable, PartialEq, Eq,
)]
#[diesel(belongs_to(Collection))]
#[diesel(belongs_to(Object))]
#[diesel(table_name = collection_objects)]
pub struct CollectionObject {
    pub id: diesel_ulid::DieselUlid,
    pub collection_id: diesel_ulid::DieselUlid,
    pub object_id: diesel_ulid::DieselUlid,
    pub is_latest: bool,
    pub auto_update: bool,
    pub is_specification: bool,
    pub writeable: bool,
    pub reference_status: ReferenceStatus,
}

#[derive(Associations, Queryable, Insertable, Identifiable, Selectable, Debug, Clone)]
#[diesel(belongs_to(Collection))]
#[diesel(belongs_to(ObjectGroup))]
pub struct CollectionObjectGroup {
    pub id: diesel_ulid::DieselUlid,
    pub collection_id: diesel_ulid::DieselUlid,
    pub object_group_id: diesel_ulid::DieselUlid,
    pub writeable: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collection_key_value_test() {
        let test_kv_label_oid = diesel_ulid::DieselUlid::generate();

        let test_kv_label = CollectionKeyValue::new_kv::<CollectionKeyValue>(
            "test_key",
            "test_value",
            test_kv_label_oid,
            KeyValueType::LABEL,
        );

        assert_eq!(*test_kv_label.get_associated_uuid(), test_kv_label_oid);
        assert_eq!(test_kv_label.get_key(), "test_key".to_string());
        assert_eq!(test_kv_label.get_value(), "test_value".to_string());
        assert_eq!(*test_kv_label.get_type(), KeyValueType::LABEL);

        let test_kv_hook_oid = diesel_ulid::DieselUlid::generate();

        let test_kv_hook = CollectionKeyValue::new_kv::<CollectionKeyValue>(
            "test_key_hook",
            "test_value_hook",
            test_kv_hook_oid,
            KeyValueType::HOOK,
        );

        assert_eq!(*test_kv_hook.get_associated_uuid(), test_kv_hook_oid);
        assert_eq!(test_kv_hook.get_key(), "test_key_hook".to_string());
        assert_eq!(test_kv_hook.get_value(), "test_value_hook".to_string());
        assert_eq!(*test_kv_hook.get_type(), KeyValueType::HOOK);
    }

    #[test]
    fn collection_version_conversion_test() {
        let v1 = CollectionVersion {
            id: diesel_ulid::DieselUlid::generate(),
            major: 1,
            minor: 0,
            patch: 0,
        };

        let v1_2 = CollectionVersion {
            id: diesel_ulid::DieselUlid::generate(),
            major: 1,
            minor: 0,
            patch: 0,
        };

        let v0_1 = CollectionVersion {
            id: diesel_ulid::DieselUlid::generate(),
            major: 0,
            minor: 1,
            patch: 0,
        };

        let v0_0_1 = CollectionVersion {
            id: diesel_ulid::DieselUlid::generate(),
            major: 0,
            minor: 0,
            patch: 1,
        };

        let v0_0_2 = CollectionVersion {
            id: diesel_ulid::DieselUlid::generate(),
            major: 0,
            minor: 0,
            patch: 2,
        };

        let v5_0_1 = CollectionVersion {
            id: diesel_ulid::DieselUlid::generate(),
            major: 5,
            minor: 0,
            patch: 1,
        };

        assert!(v1.eq(&v1_2));
        assert!(v1.ge(&v0_1));
        assert!(v0_0_2.gt(&v0_0_1));
        assert!(v0_1.ge(&v0_0_1));
        assert!(v0_1.lt(&v5_0_1));
    }
}
