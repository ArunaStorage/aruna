use uuid::Uuid;

use crate::api::aruna::api::storage::models::v1::KeyValue;

use crate::database::models::collection::CollectionKeyValue;
use crate::database::models::enums::KeyValueType;
use crate::database::models::object::ObjectKeyValue;

/// This is a helper function that converts two
/// gRPC `Vec<KeyValue>` for labels and hooks
/// to one database `Vec<CollectionKeyValue>`
///
/// ## Arguments
///
/// * `labels` - A vector containing the label key-value pairs
/// * `hooks` - A vector containing the hook key-value pairs
/// * `collection_uuid` - Id of the associated collection
///
/// ## Returns
///
/// * `Vec<CollectionKeyValue` - A vector containing all key-value pairs in their database representation
///
pub fn to_collection_key_values(
    labels: Vec<KeyValue>,
    hooks: Vec<KeyValue>,
    collection_uuid: Uuid,
) -> Vec<CollectionKeyValue> {
    labels
        .iter()
        .map(|keyvalue| CollectionKeyValue {
            id: Uuid::new_v4(),
            collection_id: collection_uuid,
            key: keyvalue.key.clone(),
            value: keyvalue.value.clone(),
            key_value_type: KeyValueType::LABEL,
        })
        .chain(hooks.iter().map(|keyvalue| CollectionKeyValue {
            id: Uuid::new_v4(),
            collection_id: collection_uuid,
            key: keyvalue.key.clone(),
            value: keyvalue.value.clone(),
            key_value_type: KeyValueType::HOOK,
        }))
        .collect::<Vec<_>>()
}

/// This is the reverse of `to_collection_key_values`
/// It converts a vector of database `CollectionKeyValue`s to
/// two vectors of gRPC KeyValue types, one for labels, one for hooks
///
/// ## Arguments
///
/// * `collection_kv` - A vector containing database CollectionKeyValues
///
/// ## Returns
///
/// * `Vec<KeyValue>` - A vector containing the label key-value pairs
/// * `Vec<KeyValue>` - A vector containing the hook key-value pairs
///
pub fn from_collection_key_values(
    collection_kv: Vec<CollectionKeyValue>,
) -> (Vec<KeyValue>, Vec<KeyValue>) {
    let (labels, hooks): (Vec<CollectionKeyValue>, Vec<CollectionKeyValue>) = collection_kv
        .into_iter()
        .partition(|elem| elem.key_value_type == KeyValueType::LABEL);

    (
        labels
            .into_iter()
            .map(|elem| KeyValue {
                key: elem.key,
                value: elem.value,
            })
            .collect::<Vec<KeyValue>>(),
        hooks
            .into_iter()
            .map(|elem| KeyValue {
                key: elem.key,
                value: elem.value,
            })
            .collect::<Vec<KeyValue>>(),
    )
}

/// Converts key-value pairs from their gRPC representation to the database model.
/// This function does not check if the pair for the specific object already exists in the database.
///
/// ## Arguments
///
/// * `labels` - A vector containing the label key-value pairs
/// * `hooks` - A vector containing the hook key-value pairs
/// * `uuid` - Id of the associated object
///
/// ## Returns
///
/// * `Vec<CollectionKeyValue` - A vector containing all key-value pairs in their database representation
///
pub fn to_object_key_values(
    labels: Vec<KeyValue>,
    hooks: Vec<KeyValue>,
    object_uuid: Uuid,
) -> Vec<ObjectKeyValue> {
    let mut db_key_value: Vec<ObjectKeyValue> = Vec::new();

    for label in labels {
        db_key_value.push(ObjectKeyValue {
            id: Uuid::new_v4(),
            object_id: object_uuid,
            key: label.key,
            value: label.value,
            key_value_type: KeyValueType::LABEL,
        });
    }

    for hook in hooks {
        db_key_value.push(ObjectKeyValue {
            id: Uuid::new_v4(),
            object_id: object_uuid,
            key: hook.key,
            value: hook.value,
            key_value_type: KeyValueType::HOOK,
        });
    }

    return db_key_value;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::aruna::api::storage::models::v1::KeyValue;
    use std::any::type_name;

    #[test]
    fn test_convert_to_collection_key_value() {
        let labels: Vec<KeyValue> = vec![
            KeyValue {
                key: "Key_01".to_string(),
                value: "Value_01".to_string(),
            },
            KeyValue {
                key: "Key_02".to_string(),
                value: "Value_02".to_string(),
            },
        ];

        let hooks: Vec<KeyValue> = vec![
            KeyValue {
                key: "AnalyzeMe".to_string(),
                value: "https://worker08.computational.bio.uni-giessen.de/workflow".to_string(),
            },
            KeyValue {
                key: "ValidateMe".to_string(),
                value: "<url-to-validation-server>".to_string(),
            },
        ];

        let db_pairs = to_collection_key_values(labels, hooks, uuid::Uuid::default());

        assert_eq!(4, db_pairs.len());
        assert_eq!("Key_01", db_pairs.get(0).unwrap().key);

        for pair in db_pairs {
            assert_eq!(
                "aruna_server::database::models::collection::CollectionKeyValue",
                type_of(pair)
            );
        }
    }

    #[test]
    fn test_convert_from_collection_key_value() {
        let labels: Vec<CollectionKeyValue> = vec![
            CollectionKeyValue {
                id: uuid::Uuid::new_v4(),
                collection_id: uuid::Uuid::new_v4(),
                key: "label".to_string(),
                value: "bar1".to_string(),
                key_value_type: KeyValueType::LABEL,
            },
            CollectionKeyValue {
                id: uuid::Uuid::new_v4(),
                collection_id: uuid::Uuid::new_v4(),
                key: "label".to_string(),
                value: "bar1".to_string(),
                key_value_type: KeyValueType::LABEL,
            },
            CollectionKeyValue {
                id: uuid::Uuid::new_v4(),
                collection_id: uuid::Uuid::new_v4(),
                key: "hook".to_string(),
                value: "bar1".to_string(),
                key_value_type: KeyValueType::HOOK,
            },
            CollectionKeyValue {
                id: uuid::Uuid::new_v4(),
                collection_id: uuid::Uuid::new_v4(),
                key: "hook".to_string(),
                value: "bar1".to_string(),
                key_value_type: KeyValueType::HOOK,
            },
        ];

        let (label_result, hooks_result) = from_collection_key_values(labels);

        assert_eq!(2, label_result.len());
        assert_eq!(2, hooks_result.len());
        for x in label_result {
            assert!(x.key == "label".to_string())
        }

        for x in hooks_result {
            assert!(x.key == "hook".to_string())
        }
    }

    #[test]
    fn test_convert_object_key_value() {
        let labels: Vec<KeyValue> = vec![
            KeyValue {
                key: "Key_01".to_string(),
                value: "Value_01".to_string(),
            },
            KeyValue {
                key: "Key_02".to_string(),
                value: "Value_02".to_string(),
            },
        ];

        let hooks: Vec<KeyValue> = vec![
            KeyValue {
                key: "AnalyzeMe".to_string(),
                value: "https://worker08.computational.bio.uni-giessen.de/workflow".to_string(),
            },
            KeyValue {
                key: "ValidateMe".to_string(),
                value: "<url-to-validation-server>".to_string(),
            },
        ];

        let db_pairs = to_object_key_values(labels, hooks, uuid::Uuid::default());

        assert_eq!(4, db_pairs.len());

        assert_eq!("Key_01", db_pairs.get(0).unwrap().key);
        assert_eq!("Value_01", db_pairs.get(0).unwrap().value);
        assert_eq!("Key_02", db_pairs.get(1).unwrap().key);
        assert_eq!("Value_02", db_pairs.get(1).unwrap().value);
        assert_eq!("AnalyzeMe", db_pairs.get(2).unwrap().key);
        assert_eq!("https://worker08.computational.bio.uni-giessen.de/workflow", db_pairs.get(2).unwrap().value);
        assert_eq!("ValidateMe", db_pairs.get(3).unwrap().key);
        assert_eq!("<url-to-validation-server>", db_pairs.get(3).unwrap().value);

        for pair in db_pairs {
            assert_eq!(
                "aruna_server::database::models::object::ObjectKeyValue",
                type_of(pair)
            );
        }
    }

    fn type_of<T>(_: T) -> &'static str {
        type_name::<T>()
    }
}
