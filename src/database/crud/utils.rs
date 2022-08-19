use chrono::{Datelike, Timelike};
use uuid::Uuid;

use crate::api::aruna::api::storage::models::v1::KeyValue;

use crate::database::models::collection::CollectionKeyValue;
use crate::database::models::enums::{KeyValueType, UserRights};
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

/// Converts a chrono::NaiveDateTime to a prost_types::Timestamp
/// This converts types with the `as` keyword. It should be safe
/// because hours, minutes etc. should never exceed the u8 bounds.
///
/// ## Arguments
///
/// * `ndt` : chrono::NaiveDateTime
///
/// ## Returns
///
/// * `Result<prost_types::Timestamp, prost_types::TimestampError>`
///

pub fn naivedatetime_to_prost_time(
    ndt: chrono::NaiveDateTime,
) -> Result<prost_types::Timestamp, prost_types::TimestampError> {
    prost_types::Timestamp::date_time(
        ndt.date().year().into(),
        ndt.date().month() as u8,
        ndt.date().day() as u8,
        ndt.time().hour() as u8,
        ndt.time().minute() as u8,
        ndt.time().second() as u8,
    )
}

/// Generic option to Option<String>
/// This function converts an Option of generic type to an Option of type String
///
/// ## Arguments
///
/// * `generic_option` an Option of a generic Type T, this Type must be ToString,
///
/// ## Returns
///
/// * `Option<String>` Associated option as String
///
pub fn option_to_string<T>(generic_option: Option<T>) -> Option<String>
where
    T: ToString,
{
    generic_option.map(|t| t.to_string())
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

    db_key_value
}

/// This is the reverse of `to_object_key_values`
/// It converts a vector of database `ObjectKeyValue`s to
/// two vectors of gRPC KeyValue types, one for labels, one for hooks.
///
/// ## Arguments
///
/// * `object_key_values` - A vector containing database CollectionKeyValues
///
/// ## Returns
///
/// * `Vec<KeyValue>` - A vector containing the label key-value pairs
/// * `Vec<KeyValue>` - A vector containing the hook key-value pairs
///
pub fn from_object_key_values(
    object_key_values: Vec<ObjectKeyValue>,
) -> (Vec<KeyValue>, Vec<KeyValue>) {
    let (labels, hooks): (Vec<ObjectKeyValue>, Vec<ObjectKeyValue>) = object_key_values
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

/// This helper function maps gRPC permissions to
/// associated DB user_rights, it is mainly used in the creation of new api_tokens
///
/// ## Arguments
///
/// * `perm` Permission - gRPC permission
///
/// ## Returns
///
/// * `Option<UserRights>` - Optional user_rights
//
pub fn map_permissions(
    perm: crate::api::aruna::api::storage::models::v1::Permission,
) -> Option<UserRights> {
    match perm {
        crate::api::aruna::api::storage::models::v1::Permission::Unspecified => None,
        crate::api::aruna::api::storage::models::v1::Permission::Read => Some(UserRights::READ),
        crate::api::aruna::api::storage::models::v1::Permission::Append => Some(UserRights::APPEND),
        crate::api::aruna::api::storage::models::v1::Permission::Modify => Some(UserRights::WRITE),
        crate::api::aruna::api::storage::models::v1::Permission::Admin => Some(UserRights::ADMIN),
        crate::api::aruna::api::storage::models::v1::Permission::None => None,
    }
}

/// This helper function maps associated DB user_rights to gRPC permissions
/// this is the reverse of `map_permissions`
///
/// ## Arguments
///
/// * `Option<UserRights>` - Optional user_rights
///
/// ## Returns
///
/// * i32 associated gRPC enum number
//
pub fn map_permissions_rev(right: Option<UserRights>) -> i32 {
    //
    //  Unspecified = 0,
    //  No permissions granted, used for users that are in the
    //  None = 1,
    //  project but have no default permissions
    //
    //  Read only
    //  Read = 2,
    //  Append objects to the collection cannot modify existing objects
    //  Append = 3,
    //  Can Read/Append/Modify objects in the collection
    //  Modify = 4,
    //  that owns the object / Create new collections
    //
    //  Can modify the collections itself and permanently
    //  Admin = 5
    //
    match right {
        Some(t) => match t {
            UserRights::READ => 2,
            UserRights::APPEND => 3,
            UserRights::MODIFY => 4,
            UserRights::WRITE => 4,
            UserRights::ADMIN => 5,
        },
        None => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::aruna::api::storage::models::v1::KeyValue;
    use crate::api::aruna::api::storage::models::v1::Permission;
    use std::any::type_name;

    #[test]
    fn test_option_to_string() {
        let test_id = uuid::Uuid::new_v4();

        let as_option = Some(test_id);

        let result = option_to_string(as_option);

        assert_eq!(as_option.is_some(), result.is_some());
        assert_eq!(test_id.to_string(), result.unwrap())
    }

    #[test]
    fn test_map_permissions_rev() {
        let tests = vec![
            (None, 0),
            (Some(UserRights::READ), 2),
            (Some(UserRights::APPEND), 3),
            (Some(UserRights::MODIFY), 4),
            (Some(UserRights::WRITE), 4),
            (Some(UserRights::ADMIN), 5),
        ];

        for (opt, expected) in tests {
            assert_eq!(map_permissions_rev(opt), expected)
        }
    }

    #[test]
    fn test_map_permissions() {
        for (index, perm) in vec![
            Permission::Unspecified,
            Permission::Read,
            Permission::Append,
            Permission::Modify,
            Permission::Admin,
            Permission::None,
        ]
        .iter()
        .enumerate()
        {
            match index {
                0 => assert_eq!(map_permissions(*perm), None),
                1 => assert_eq!(map_permissions(*perm), Some(UserRights::READ)),
                2 => assert_eq!(map_permissions(*perm), Some(UserRights::APPEND)),
                3 => assert_eq!(map_permissions(*perm), Some(UserRights::WRITE)),
                4 => assert_eq!(map_permissions(*perm), Some(UserRights::ADMIN)),
                5 => assert_eq!(map_permissions(*perm), None),
                _ => panic!("map permissions test index out of bound"),
            }
        }
    }

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
            assert!(x.key == *"label")
        }

        for x in hooks_result {
            assert!(x.key == *"hook")
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
        assert_eq!(
            "https://worker08.computational.bio.uni-giessen.de/workflow",
            db_pairs.get(2).unwrap().value
        );
        assert_eq!("ValidateMe", db_pairs.get(3).unwrap().key);
        assert_eq!("<url-to-validation-server>", db_pairs.get(3).unwrap().value);

        for pair in db_pairs {
            assert_eq!(
                "aruna_server::database::models::object::ObjectKeyValue",
                type_of(pair)
            );
        }
    }

    #[test]
    fn test_convert_from_object_key_value() {
        let object_key_values: Vec<ObjectKeyValue> = vec![
            ObjectKeyValue {
                id: uuid::Uuid::new_v4(),
                object_id: uuid::Uuid::new_v4(),
                key: "label".to_string(),
                value: "bar1".to_string(),
                key_value_type: KeyValueType::LABEL,
            },
            ObjectKeyValue {
                id: uuid::Uuid::new_v4(),
                object_id: uuid::Uuid::new_v4(),
                key: "label".to_string(),
                value: "bar1".to_string(),
                key_value_type: KeyValueType::LABEL,
            },
            ObjectKeyValue {
                id: uuid::Uuid::new_v4(),
                object_id: uuid::Uuid::new_v4(),
                key: "hook".to_string(),
                value: "bar1".to_string(),
                key_value_type: KeyValueType::HOOK,
            },
            ObjectKeyValue {
                id: uuid::Uuid::new_v4(),
                object_id: uuid::Uuid::new_v4(),
                key: "hook".to_string(),
                value: "bar1".to_string(),
                key_value_type: KeyValueType::HOOK,
            },
        ];

        let (labels, hooks) = from_object_key_values(object_key_values);

        assert_eq!(2, labels.len());
        assert_eq!(2, hooks.len());
        for x in labels {
            assert!(x.key == *"label")
        }

        for x in hooks {
            assert!(x.key == *"hook")
        }
    }

    /// Helper method to return the fully qualified type name of an object
    fn type_of<T>(_: T) -> &'static str {
        type_name::<T>()
    }
}
