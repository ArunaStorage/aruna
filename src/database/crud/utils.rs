use std::collections::HashMap;

use chrono::{Datelike, Timelike};
use uuid::Uuid;

use crate::api::aruna::api::storage::models::v1::{KeyValue, LabelOrIdQuery, PageRequest};

use crate::database::models::collection::CollectionKeyValue;
use crate::database::models::enums::{KeyValueType, UserRights};
use crate::database::models::object::ObjectKeyValue;
use crate::database::models::traits::IsKeyValue;
use crate::error::ArunaError;

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

/// This helper function parses a page_request and returns a result with optional pagesize and optional last_uuid
///
///
/// ## Behaviour
///
/// The PageRequest is an optional Parameter for many requests. The following variants can occur:
///
/// - p_request isNone() => Return Ok(Some(default_pagesize), None): No pagerequest means use the default pagesize starting at the beginning
/// - p_request isSome(size = 0) => Return Ok(Some(default_pagesize), None)
/// - p_request isSome(size = -1) => Return Ok(None, None) pagesize == None means unlimited size
/// - last_uuid is Some if it is specified otherwise its the same as above
///
/// ## Arguments
///
/// * `p_request`:`Option<PageRequest>` - PageRequest information
/// * `default_pagesize`:`i64` - pagesize that should be "default"
///
/// ## Returns
///
/// * Result<(Option<i64>, Option<uuid::Uuid>)>: Pagesize, last_uuid, can error when the uuid_parsing fails.
///
pub fn parse_page_request(
    p_request: Option<PageRequest>,
    default_pagesize: i64,
) -> Result<(Option<i64>, Option<uuid::Uuid>), ArunaError> {
    match p_request {
        // If the p_request is some
        Some(p_req) => {
            // if the last_uuid is empty
            if p_req.last_uuid.is_empty() {
                // when the pagesize is 0 == unspecified or < -1 --> use default
                if p_req.page_size == 0 || p_req.page_size < -1 {
                    Ok((Some(default_pagesize), None))
                // When page_size is explicitly -1 == umlimited
                } else if p_req.page_size == -1 {
                    Ok((None, None))
                } else {
                    Ok((Some(p_req.page_size), None))
                }
            // When the last_uuid is not empty
            } else {
                let parsed_uuid = uuid::Uuid::parse_str(&p_req.last_uuid)?;
                // when the pagesize is 0 == unspecified or < -1 --> use default
                if p_req.page_size == 0 || p_req.page_size < -1 {
                    Ok((Some(default_pagesize), Some(parsed_uuid)))
                // When page_size is explicitly -1 == umlimited
                } else if p_req.page_size == -1 {
                    Ok((None, Some(parsed_uuid)))
                } else {
                    Ok((Some(p_req.page_size), Some(parsed_uuid)))
                }
            }
        }

        // if it is None
        None => Ok((Some(default_pagesize), None)),
    }
}

/// Struct that specifies a query
/// Can either be LabelQuery or IdsQuery
#[derive(PartialEq, Debug)]
pub enum ParsedQuery {
    // List with Labels (key and optional(value)) and a bool value that specifies and == true or or behaviour == false
    // default is or
    LabelQuery((Vec<(String, Option<String>)>, bool)),
    // List of uuids to query, this should be always "or"
    IdsQuery(Vec<uuid::Uuid>),
}
/// Function that parses the query to ParsedQuery enum.
///
/// ## Behaviour
///
/// This parses the LabelOrIdQuery to a "ParsedQuery" enum.
/// The enum either contains LabelQuery with a vector of Labels and a boolean for and/or
/// or an enum with a vector of ids that should be queried. Will return None if no labels and ids are specified
/// In this case a list with ALL elements should be returned by the calling DB function
///
/// ## Arguments
///
/// * `grpc_query`:`Option<LabelOrIdQuery>` gRPC LabelOrIdQuery, will be splitted in an enum
///
/// ## Returns
///
/// * Result<Option<ParsedQuery>, ArunaError>: ParsedQuery is an enum with either a list of UUIDs or a list of label (key-value pairs)
///
pub fn parse_query(grpc_query: Option<LabelOrIdQuery>) -> Result<Option<ParsedQuery>, ArunaError> {
    match grpc_query {
        Some(g_query) => {
            if g_query.ids.is_empty() {
                if let Some(filters) = g_query.labels {
                    if filters.labels.is_empty() {
                        return Ok(None);
                    }
                    let mapped_labels = filters
                        .labels
                        .iter()
                        .map(|kv| {
                            (
                                kv.key.clone(),
                                if filters.keys_only {
                                    None
                                } else {
                                    Some(kv.value.clone())
                                },
                            )
                        })
                        .collect::<Vec<_>>();

                    Ok(Some(ParsedQuery::LabelQuery((
                        mapped_labels,
                        filters.and_or_or,
                    ))))
                } else {
                    Ok(None)
                }
            } else {
                if g_query.labels.is_some() {
                    return Err(ArunaError::InvalidRequest(
                        "Either uids, or labelfilter can be specified".to_string(),
                    ));
                }
                let mut parsed_uids = Vec::new();
                for q_id in g_query.ids {
                    parsed_uids.push(uuid::Uuid::parse_str(&q_id)?)
                }
                Ok(Some(ParsedQuery::IdsQuery(parsed_uids)))
            }
        }
        None => Ok(None),
    }
}

/// Generic function that checks if the "all" constraint for queried key_values is fullfilled.
///
/// ## Behaviour
///
/// Because each key_value pair for Objects/ObjectGroups/Collections is its own database entry it is not possible
/// to directly combine multiple key_value constraints via "all". To circumvent this first all db keyvalue pairs that match
/// at least one requested key or value are queried. This list is afterwards processed via this function. This function checks
/// if their are resources that match all "target" key_value pairs, which are returned as list of uuids. If no entries match
/// None is returned.
///
/// ## Arguments
///
/// * `database_key_value`: `Option<Vec<T>>` Vector of Database resources that implement the `IsKeyValue` trait.
/// * `targets`:`Vec<(String, Option<String>)>` Vector of keys with (optional) values that should all match.
///
/// ## Returns
///
/// * Option<Vec<uuid::Uuid>: Vec with all uuids that match.
///
pub fn check_all_for_db_kv<T>(
    database_key_value: Option<Vec<T>>,
    targets: Vec<(String, Option<String>)>,
) -> Option<Vec<uuid::Uuid>>
where
    T: IsKeyValue,
{
    if let Some(ckv) = database_key_value {
        let mut hits = HashMap::new();

        for col_key_value in ckv {
            if !hits.contains_key(col_key_value.get_associated_uuid()) {
                hits.insert(*col_key_value.get_associated_uuid(), 0);
            }

            for (target_key, target_value) in targets.clone() {
                if target_key == col_key_value.get_key().to_string() {
                    if let Some(tkv) = target_value {
                        if col_key_value.get_value() == tkv.to_string() {
                            *hits.get_mut(col_key_value.get_associated_uuid()).unwrap() += 1
                        }
                    } else {
                        *hits.get_mut(col_key_value.get_associated_uuid()).unwrap() += 1;
                    }
                }
            }
        }

        let result = hits
            .iter()
            .filter_map(|(k, v)| if *v == targets.len() { Some(*k) } else { None })
            .collect::<Vec<_>>();
        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::aruna::api::storage::models::v1::KeyValue;
    use crate::api::aruna::api::storage::models::v1::LabelFilter;
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

    #[test]
    fn test_parse_page_request() {
        // Empty PageRequest
        let empty: Option<PageRequest> = None;
        let result = parse_page_request(empty, 25).unwrap();
        assert!(&result.0.is_some()); // Should contain a pagesize
        assert!(&result.1.is_none()); // Should not contain a last_uuid
        assert_eq!(result.0.unwrap(), 25); // Pagesize should be 25

        // Empty non_null request
        let zero_values = Some(PageRequest {
            last_uuid: "".to_string(),
            page_size: 0,
        });
        let result = parse_page_request(zero_values, 30).unwrap();
        assert!(&result.0.is_some()); // Should contain a pagesize
        assert!(&result.1.is_none()); // Should not contain a last_uuid
        assert_eq!(result.0.unwrap(), 30); // Pagesize should be 30

        // Non zero pagesize
        let non_zero_psize = Some(PageRequest {
            last_uuid: "".to_string(),
            page_size: 99,
        });

        let result = parse_page_request(non_zero_psize, 30).unwrap();
        assert!(&result.0.is_some()); // Should contain a pagesize
        assert!(&result.1.is_none()); // Should not contain a last_uuid
        assert_eq!(result.0.unwrap(), 99); // Pagesize should be 99

        // Non zero pagesize and uuid
        let test_uuid = uuid::Uuid::new_v4();
        let non_zero_psize = Some(PageRequest {
            last_uuid: test_uuid.to_string(),
            page_size: 99,
        });

        let result = parse_page_request(non_zero_psize, 30).unwrap();
        assert!(result.0.is_some()); // Should contain a pagesize
        assert!(result.1.is_some()); // Should not contain a last_uuid
        assert_eq!(result.0.unwrap(), 99); // Pagesize should be 99
        assert_eq!(result.1.unwrap(), test_uuid); // Uuid should be equal

        // Invalid uuid
        let non_zero_psize = Some(PageRequest {
            last_uuid: "broken_string12356".to_string(),
            page_size: 99,
        });

        let result = parse_page_request(non_zero_psize, 30);
        assert!(result.is_err()); // Should be err
    }

    #[test]
    fn test_parse_query() {
        // None LabelOrIdQuery
        let empty: Option<LabelOrIdQuery> = None;
        let result = parse_query(empty);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
        // Empty LabelOrIdQuery
        let empty = Some(LabelOrIdQuery {
            labels: None,
            ids: Vec::new(),
        });
        let result = parse_query(empty);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
        // Labels
        let test_labels = vec![
            KeyValue {
                key: "test1".to_string(),
                value: "value1".to_string(),
            },
            KeyValue {
                key: "test2".to_string(),
                value: "value2".to_string(),
            },
            KeyValue {
                key: "test3".to_string(),
                value: "value3".to_string(),
            },
        ];
        let expect_with_values = vec![
            ("test1".to_string(), Some("value1".to_string())),
            ("test2".to_string(), Some("value2".to_string())),
            ("test3".to_string(), Some("value3".to_string())),
        ];

        let expect_without_values: Vec<(String, Option<String>)> = vec![
            ("test1".to_string(), None),
            ("test2".to_string(), None),
            ("test3".to_string(), None),
        ];

        // With labels + and not keys_only
        let labels = Some(LabelOrIdQuery {
            labels: Some(LabelFilter {
                labels: test_labels.clone(),
                and_or_or: true,
                keys_only: false,
            }),
            ids: Vec::new(),
        });

        let result = parse_query(labels).unwrap();
        assert!(result.is_some());
        assert_eq!(
            result.unwrap(),
            ParsedQuery::LabelQuery((expect_with_values.clone(), true))
        );

        // With labels + and keys_only
        let labels = Some(LabelOrIdQuery {
            labels: Some(LabelFilter {
                labels: test_labels.clone(),
                and_or_or: true,
                keys_only: true,
            }),
            ids: Vec::new(),
        });

        let result = parse_query(labels).unwrap();
        assert!(result.is_some());
        assert_eq!(
            result.unwrap(),
            ParsedQuery::LabelQuery((expect_without_values, true))
        );

        // With labels or not keys_only
        let labels = Some(LabelOrIdQuery {
            labels: Some(LabelFilter {
                labels: test_labels.clone(),
                and_or_or: false,
                keys_only: false,
            }),
            ids: Vec::new(),
        });

        let result = parse_query(labels).unwrap();
        assert!(result.is_some());
        assert_eq!(
            result.unwrap(),
            ParsedQuery::LabelQuery((expect_with_values, false))
        );

        // Id section

        let test_ids = vec![
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
        ];
        let test_ids_string = test_ids
            .iter()
            .map(uuid::Uuid::to_string)
            .collect::<Vec<String>>();

        // With ids
        let ids = Some(LabelOrIdQuery {
            labels: None,
            ids: test_ids_string.clone(),
        });

        let result = parse_query(ids).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), ParsedQuery::IdsQuery(test_ids));

        // Errors:

        // Malformed id:

        let bad_test_id_string = vec!["asdasdasdaswd".to_string(), "asdasdasd".to_string()];
        // With ids
        let ids = Some(LabelOrIdQuery {
            labels: None,
            ids: bad_test_id_string,
        });

        let result = parse_query(ids);

        assert!(result.is_err());

        // Ids AND Labels specified

        let broken = Some(LabelOrIdQuery {
            labels: Some(LabelFilter {
                labels: test_labels,
                and_or_or: true,
                keys_only: false,
            }),
            ids: test_ids_string,
        });

        let result = parse_query(broken);

        assert!(result.is_err());
    }

    #[test]
    fn test_check_all_for_db_kv() {
        let test_kvs = vec![
            ("test_key1", "test_value1"),
            ("test_key2", "test_value2"),
            ("test_key3", "test_value3"),
            ("test_key4", "test_value4"),
            ("test_key5", "test_value5"),
            ("test_key6", "test_value6"),
        ];

        let mut coll_key_values = Vec::new();

        let id_hit = uuid::Uuid::new_v4();
        let id_non_hit = uuid::Uuid::new_v4();

        for (index, (k, v)) in test_kvs.iter().enumerate() {
            if index % 3 == 0 {
                coll_key_values.push(CollectionKeyValue {
                    id: uuid::Uuid::new_v4(),
                    collection_id: id_non_hit,
                    key: k.to_string(),
                    value: v.to_string(),
                    key_value_type: KeyValueType::LABEL,
                });
            }

            coll_key_values.push(CollectionKeyValue {
                id: uuid::Uuid::new_v4(),
                collection_id: id_hit,
                key: k.to_string(),
                value: v.to_string(),
                key_value_type: KeyValueType::LABEL,
            });
        }

        let targets_with_values = vec![
            ("test_key1".to_string(), Some("test_value1".to_string())),
            ("test_key2".to_string(), Some("test_value2".to_string())),
            ("test_key3".to_string(), Some("test_value3".to_string())),
        ];

        let hits = check_all_for_db_kv(Some(coll_key_values.clone()), targets_with_values.clone());
        assert!(hits.is_none());

        let hits = check_all_for_db_kv(Some(coll_key_values.clone()), targets_with_values);

        assert_eq!(hits.clone().unwrap().len(), 1);
        assert_eq!(hits.unwrap()[0], id_hit);

        let targets_without_values: Vec<(String, Option<String>)> = vec![
            ("test_key1".to_string(), None),
            ("test_key2".to_string(), None),
            ("test_key3".to_string(), None),
        ];

        let hits = check_all_for_db_kv(Some(coll_key_values.clone()), targets_without_values);

        assert_eq!(hits.clone().unwrap().len(), 1);
        assert_eq!(hits.unwrap()[0], id_hit);

        let targets_both: Vec<(String, Option<String>)> = vec![
            ("test_key1".to_string(), Some("test_value1".to_string())),
            ("test_key4".to_string(), Some("test_value4".to_string())),
        ];

        let hits = check_all_for_db_kv(Some(coll_key_values), targets_both);

        assert_eq!(hits.clone().unwrap().len(), 2);
        assert_eq!(hits.clone().unwrap()[0], id_hit);
        assert_eq!(hits.clone().unwrap()[1], id_non_hit);
    }

    /// Helper method to return the fully qualified type name of an object
    fn type_of<T>(_: T) -> &'static str {
        type_name::<T>()
    }
}
