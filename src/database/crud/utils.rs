use crate::database::models::collection::CollectionKeyValue;
use crate::{api::aruna::api::storage::models::v1::KeyValue, database::models};

/// This is a helper function that converts two
/// gRPC Vec<KeyValue> for labels and hooks
/// associated to
pub fn to_collection_key_values(
    labels: Vec<KeyValue>,
    hooks: Vec<KeyValue>,
    collection_uuid: uuid::Uuid,
) -> Vec<CollectionKeyValue> {
    labels
        .iter()
        .map(|keyvalue| CollectionKeyValue {
            id: uuid::Uuid::new_v4(),
            collection_id: collection_uuid,
            key: keyvalue.key.clone(),
            value: keyvalue.value.clone(),
            key_value_type: models::enums::KeyValueType::LABEL,
        })
        .chain(hooks.iter().map(|keyvalue| CollectionKeyValue {
            id: uuid::Uuid::new_v4(),
            collection_id: collection_uuid,
            key: keyvalue.key.clone(),
            value: keyvalue.value.clone(),
            key_value_type: models::enums::KeyValueType::HOOK,
        }))
        .collect::<Vec<_>>()
}
