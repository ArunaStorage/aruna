use super::enums::KeyValueType;

pub trait IsKeyValue {
    fn get_key(&self) -> &str;
    fn get_value(&self) -> &str;
    fn get_associated_uuid(&self) -> &uuid::Uuid;
    fn get_type(&self) -> &KeyValueType;
}
