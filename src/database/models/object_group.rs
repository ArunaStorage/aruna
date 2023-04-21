use super::auth::*;
use super::enums::*;
use super::object::*;
use super::traits::IsKeyValue;
use super::traits::ToDbKeyValue;
use crate::database::schema::*;

#[derive(
    Associations, Queryable, Insertable, Identifiable, Debug, Selectable, Clone, AsChangeset,
)]
#[diesel(belongs_to(User, foreign_key = created_by))]
#[diesel(table_name = object_groups)]
pub struct ObjectGroup {
    pub id: diesel_ulid::DieselUlid,
    pub shared_revision_id: diesel_ulid::DieselUlid,
    pub revision_number: i64,
    pub name: Option<String>,
    pub description: Option<String>,
    pub created_at: chrono::NaiveDateTime,
    pub created_by: diesel_ulid::DieselUlid,
}

#[derive(Associations, Queryable, Insertable, Identifiable, Debug)]
#[diesel(table_name = object_group_key_value)]
#[diesel(belongs_to(ObjectGroup))]
pub struct ObjectGroupKeyValue {
    pub id: diesel_ulid::DieselUlid,
    pub object_group_id: diesel_ulid::DieselUlid,
    pub key: String,
    pub value: String,
    pub key_value_type: KeyValueType,
}

impl IsKeyValue for ObjectGroupKeyValue {
    fn get_key(&self) -> &str {
        &self.key
    }

    fn get_value(&self) -> &str {
        &self.value
    }

    fn get_associated_uuid(&self) -> &diesel_ulid::DieselUlid {
        &self.object_group_id
    }

    fn get_type(&self) -> &KeyValueType {
        &self.key_value_type
    }
}

impl ToDbKeyValue for ObjectGroupKeyValue {
    fn new_kv<ObjectGroupKeyValue>(
        key: &str,
        value: &str,
        belongs_to: diesel_ulid::DieselUlid,
        kv_type: KeyValueType,
    ) -> Self {
        Self {
            id: diesel_ulid::DieselUlid::generate(),
            object_group_id: belongs_to,
            key: key.to_string(),
            value: value.to_string(),
            key_value_type: kv_type,
        }
    }
}

#[derive(Associations, Queryable, Insertable, Identifiable, Debug, Selectable)]
#[diesel(belongs_to(ObjectGroup))]
#[diesel(belongs_to(Object))]
#[diesel(table_name = object_group_objects)]
pub struct ObjectGroupObject {
    pub id: diesel_ulid::DieselUlid,
    pub object_id: diesel_ulid::DieselUlid,
    pub object_group_id: diesel_ulid::DieselUlid,
    pub is_meta: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_group_is_key_value_test() {
        let test_kv_label_oid = diesel_ulid::DieselUlid::generate();

        let test_kv_label = ObjectGroupKeyValue::new_kv::<ObjectGroupKeyValue>(
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

        let test_kv_hook = ObjectGroupKeyValue::new_kv::<ObjectGroupKeyValue>(
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
}
