use aruna_server::api::aruna::api::storage::models::v1::{
    KeyValue,
    collection_overview,
    LabelOrIdQuery,
    LabelFilter,
    PageRequest,
};
use aruna_server::api::aruna::api::storage::services::v1::*;
use aruna_server::database;
use serial_test::serial;
use std::str::FromStr;

#[test]
#[ignore]
#[serial(db)]
fn create_new_collection_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    let creator = uuid::Uuid::parse_str("12345678-1234-1234-1234-111111111111").unwrap();

    let request = CreateNewCollectionRequest {
        name: "new_collection".to_owned(),
        description: "this_is_a_demo_collection".to_owned(),
        project_id: "12345678-1111-1111-1111-111111111111".to_owned(),
        labels: vec![KeyValue {
            key: "test_key".to_owned(),
            value: "test_value".to_owned(),
        }],
        hooks: vec![KeyValue {
            key: "test_key".to_owned(),
            value: "test_value".to_owned(),
        }],
        dataclass: 1,
    };

    let result = db.create_new_collection(request, creator).unwrap();
    let id = uuid::Uuid::from_str(&result.collection_id).unwrap();

    assert!(!id.is_nil())
}

#[test]
#[ignore]
#[serial(db)]
fn get_collection_by_id_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    let creator = uuid::Uuid::parse_str("12345678-1234-1234-1234-111111111111").unwrap();

    let request = CreateNewCollectionRequest {
        name: "new_collection".to_owned(),
        description: "this_is_a_demo_collection".to_owned(),
        project_id: "12345678-1111-1111-1111-111111111111".to_owned(),
        labels: vec![KeyValue {
            key: "label_test_key".to_owned(),
            value: "label_test_value".to_owned(),
        }],
        hooks: vec![KeyValue {
            key: "hook_test_key".to_owned(),
            value: "hook_test_value".to_owned(),
        }],
        dataclass: 2, // PRIVATE
    };

    // Create a new collection
    let result = db.create_new_collection(request, creator).unwrap();

    // Get collection by ID

    let q_col_req = GetCollectionByIdRequest { collection_id: result.collection_id };
    let q_col = db.get_collection_by_id(q_col_req).unwrap();

    // Collection should be some
    assert!(q_col.collection.is_some());
    // Collection should not be public
    assert!(!q_col.collection.clone().unwrap().is_public);
    // Collection should have this description
    assert_eq!(
        q_col.collection.clone().unwrap().description,
        "this_is_a_demo_collection".to_string()
    );
    // Collection should have the following name
    assert_eq!(q_col.collection.clone().unwrap().name, "new_collection".to_string());
    // Collection should not have a version
    assert!(
        q_col.collection.clone().unwrap().version.unwrap() ==
            collection_overview::Version::Latest(true)
    );
    // Collection should not have a version
    assert!(
        // Should be empty vec
        q_col.collection.clone().unwrap().label_ontology.unwrap().required_label_keys.is_empty()
    );
    assert!(q_col.collection.clone().unwrap().labels.len() == 1);
    assert!(q_col.collection.clone().unwrap().hooks.len() == 1);
    assert!(
        q_col.collection.clone().unwrap().hooks[0] ==
            KeyValue {
                key: "hook_test_key".to_owned(),
                value: "hook_test_value".to_owned(),
            }
    );

    assert!(
        q_col.collection.clone().unwrap().labels[0] ==
            KeyValue {
                key: "label_test_key".to_owned(),
                value: "label_test_value".to_owned(),
            }
    );
}

#[test]
#[ignore]
#[serial(db)]
fn get_collections_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    let creator = uuid::Uuid::parse_str("12345678-1234-1234-1234-111111111111").unwrap();

    let request = CreateNewCollectionRequest {
        name: "new_collection_1".to_owned(),
        description: "this_is_a_demo_collection_1".to_owned(),
        project_id: "12345678-1111-1111-1111-111111111111".to_owned(),
        labels: vec![
            KeyValue {
                key: "label_test_key_1".to_owned(),
                value: "label_test_value_1".to_owned(),
            },
            KeyValue {
                key: "common_label".to_owned(),
                value: "common_value".to_owned(),
            }
        ],
        hooks: vec![KeyValue {
            key: "hook_test_key_1".to_owned(),
            value: "hook_test_value_1".to_owned(),
        }],
        dataclass: 2, // PRIVATE
    };

    // Create a new collection
    let result_1 = db.create_new_collection(request, creator).unwrap();

    let request = CreateNewCollectionRequest {
        name: "new_collection_2".to_owned(),
        description: "this_is_a_demo_collection_2".to_owned(),
        project_id: "12345678-1111-1111-1111-111111111111".to_owned(),
        labels: vec![
            KeyValue {
                key: "label_test_key_2".to_owned(),
                value: "label_test_value_2".to_owned(),
            },
            KeyValue {
                key: "common_label".to_owned(),
                value: "common_value".to_owned(),
            }
        ],
        hooks: vec![KeyValue {
            key: "hook_test_key_2".to_owned(),
            value: "hook_test_value_2".to_owned(),
        }],
        dataclass: 2, // PRIVATE
    };

    // Create a new collection
    let result_2 = db.create_new_collection(request, creator).unwrap();
    let res_2_id = result_2.collection_id;

    let request = CreateNewCollectionRequest {
        name: "new_collection_3".to_owned(),
        description: "this_is_a_demo_collection_3".to_owned(),
        project_id: "12345678-1111-1111-1111-111111111111".to_owned(),
        labels: vec![
            KeyValue {
                key: "label_test_key_3_1".to_owned(),
                value: "label_test_value_3_1".to_owned(),
            },
            KeyValue {
                key: "label_test_key_3_2".to_owned(),
                value: "label_test_value_3_2".to_owned(),
            },
            KeyValue {
                key: "common_label".to_owned(),
                value: "common_value".to_owned(),
            }
        ],
        hooks: vec![KeyValue {
            key: "hook_test_key_2".to_owned(),
            value: "hook_test_value_2".to_owned(),
        }],
        dataclass: 2, // PRIVATE
    };

    // Create a new collection
    let result_3 = db.create_new_collection(request, creator).unwrap();

    // Get collections Request

    // 1. ID filter no page
    let q_col_req = GetCollectionsRequest {
        project_id: "12345678-1111-1111-1111-111111111111".to_owned(),
        label_or_id_filter: Some(LabelOrIdQuery { labels: None, ids: vec![res_2_id.clone()] }),
        page_request: None,
    };
    // Expect result_2
    let quest_result = db.get_collections(q_col_req).unwrap();
    assert!(quest_result.collections.clone().unwrap().collection_overviews.len() == 1);
    assert!(quest_result.collections.unwrap().collection_overviews[0].id == res_2_id);

    // 2. Label filter (2)
    let q_col_req = GetCollectionsRequest {
        project_id: "12345678-1111-1111-1111-111111111111".to_owned(),
        label_or_id_filter: Some(LabelOrIdQuery {
            labels: Some(LabelFilter {
                labels: vec![KeyValue {
                    key: "label_test_key_1".to_owned(),
                    value: "label_test_value_1".to_owned(),
                }],
                and_or_or: false,
                keys_only: false,
            }),
            ids: Vec::new(),
        }),
        page_request: None,
    };
    // Expect result_1
    let quest_result = db.get_collections(q_col_req).unwrap();
    assert!(quest_result.collections.clone().unwrap().collection_overviews.len() == 1);
    assert!(quest_result.collections.unwrap().collection_overviews[0].id == result_1.collection_id);

    // 2. Label filter (3)
    let q_col_req = GetCollectionsRequest {
        project_id: "12345678-1111-1111-1111-111111111111".to_owned(),
        label_or_id_filter: Some(LabelOrIdQuery {
            labels: Some(LabelFilter {
                labels: vec![KeyValue {
                    key: "common_label".to_owned(),
                    value: "label_test_value_1".to_owned(),
                }],
                and_or_or: false,
                keys_only: true,
            }),
            ids: Vec::new(),
        }),
        page_request: None,
    };
    // Expect all
    let quest_result = db.get_collections(q_col_req).unwrap();
    assert!(quest_result.collections.clone().unwrap().collection_overviews.len() == 3);

    // 2. Label filter (4)
    let q_col_req = GetCollectionsRequest {
        project_id: "12345678-1111-1111-1111-111111111111".to_owned(),
        label_or_id_filter: Some(LabelOrIdQuery {
            labels: Some(LabelFilter {
                labels: vec![
                    KeyValue {
                        key: "common_label".to_owned(),
                        value: "common_value".to_owned(),
                    },
                    KeyValue {
                        key: "label_test_key_3_1".to_owned(),
                        value: "label_test_value_3_1".to_owned(),
                    }
                ],
                and_or_or: true,
                keys_only: false,
            }),
            ids: Vec::new(),
        }),
        page_request: None,
    };
    // Expect result_3
    let quest_result = db.get_collections(q_col_req).unwrap();
    assert!(quest_result.collections.clone().unwrap().collection_overviews.len() == 1);
    assert!(quest_result.collections.unwrap().collection_overviews[0].id == result_3.collection_id);

    // 2. PageRequest (1)
    let q_col_req = GetCollectionsRequest {
        project_id: "12345678-1111-1111-1111-111111111111".to_owned(),
        label_or_id_filter: Some(LabelOrIdQuery {
            labels: Some(LabelFilter {
                labels: vec![KeyValue {
                    key: "common_label".to_owned(),
                    value: "label_test_value_1".to_owned(),
                }],
                and_or_or: false,
                keys_only: true,
            }),
            ids: Vec::new(),
        }),
        page_request: Some(PageRequest { last_uuid: "".to_string(), page_size: 1 }),
    };
    // Expect all
    let quest_result_1 = db.get_collections(q_col_req).unwrap();
    assert!(quest_result_1.clone().collections.clone().unwrap().collection_overviews.len() == 1);

    // 2. PageRequest (2) -> next
    let q_col_req = GetCollectionsRequest {
        project_id: "12345678-1111-1111-1111-111111111111".to_owned(),
        label_or_id_filter: Some(LabelOrIdQuery {
            labels: Some(LabelFilter {
                labels: vec![KeyValue {
                    key: "common_label".to_owned(),
                    value: "label_test_value_1".to_owned(),
                }],
                and_or_or: false,
                keys_only: true,
            }),
            ids: Vec::new(),
        }),
        page_request: Some(PageRequest {
            last_uuid: quest_result_1
                .clone()
                .collections.unwrap()
                .collection_overviews[0].clone().id,
            page_size: 1,
        }),
    };
    // Expect only one
    let quest_result_2 = db.get_collections(q_col_req).unwrap();
    assert!(quest_result_2.clone().collections.clone().unwrap().collection_overviews.len() == 1);
    assert!(
        quest_result_1.collections.clone().unwrap().collection_overviews[0].id !=
            quest_result_2.collections.clone().unwrap().collection_overviews[0].id
    );

    // INVALID
    let q_col_req = GetCollectionsRequest {
        project_id: "12345678-1111-1111-1111-111111111111".to_owned(),
        label_or_id_filter: Some(LabelOrIdQuery {
            labels: Some(LabelFilter {
                labels: vec![KeyValue {
                    key: "common_label".to_owned(),
                    value: "label_test_value_1".to_owned(),
                }],
                and_or_or: false,
                keys_only: true,
            }),
            ids: vec![res_2_id],
        }),
        page_request: Some(PageRequest {
            last_uuid: quest_result_1
                .clone()
                .collections.unwrap()
                .collection_overviews[0].clone().id,
            page_size: 1,
        }),
    };
    // Expect Error
    let quest_result_3 = db.get_collections(q_col_req);
    assert!(quest_result_3.is_err())
}