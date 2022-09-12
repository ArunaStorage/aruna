use super::enums::KeyValueType;

pub trait IsKeyValue {
    fn get_key(&self) -> &str;
    fn get_value(&self) -> &str;
    fn get_associated_uuid(&self) -> &uuid::Uuid;
    fn get_type(&self) -> &KeyValueType;
}

pub trait ToDbKeyValue {
    fn new_kv<T>(key: &str, value: &str, belongs_to: uuid::Uuid, kv_type: KeyValueType) -> Self;
}
