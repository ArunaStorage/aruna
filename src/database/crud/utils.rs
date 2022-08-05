use crate::{api::aruna::api::storage::models::v1::KeyValue, database::models};

pub fn _to_collection_key_values(
    labels: Vec<KeyValue>,
    hooks: Vec<KeyValue>,
    uuid: uuid::Uuid,
) -> Vec<models::collection::CollectionKeyValue> {
    todo!()
}
