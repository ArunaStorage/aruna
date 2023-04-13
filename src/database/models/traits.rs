use super::enums::KeyValueType;

pub trait IsKeyValue {
    fn get_key(&self) -> &str;
    fn get_value(&self) -> &str;
    fn get_associated_uuid(&self) -> &diesel_ulid::DieselUlid;
    fn get_type(&self) -> &KeyValueType;
}

pub trait ToDbKeyValue {
    fn new_kv<T>(key: &str, value: &str, belongs_to: diesel_ulid::DieselUlid, kv_type: KeyValueType) -> Self;
}
