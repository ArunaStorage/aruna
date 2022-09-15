use aruna_server::api::aruna::api::storage::internal::v1::Location;
use aruna_server::api::aruna::api::storage::models::v1::{
    collection_overview, KeyValue, LabelFilter, LabelOrIdQuery, PageRequest, Version,
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

    let q_col_req = GetCollectionByIdRequest {
        collection_id: result.collection_id,
    };
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
    assert_eq!(
        q_col.collection.clone().unwrap().name,
        "new_collection".to_string()
    );
    // Collection should not have a version
    assert!(
        q_col.collection.clone().unwrap().version.unwrap()
            == collection_overview::Version::Latest(true)
    );
    // Collection should not have a version
    assert!(
        // Should be empty vec
        q_col
            .collection
            .clone()
            .unwrap()
            .label_ontology
            .unwrap()
            .required_label_keys
            .is_empty()
    );
    assert!(q_col.collection.clone().unwrap().labels.len() == 1);
    assert!(q_col.collection.clone().unwrap().hooks.len() == 1);
    assert!(
        q_col.collection.clone().unwrap().hooks[0]
            == KeyValue {
                key: "hook_test_key".to_owned(),
                value: "hook_test_value".to_owned(),
            }
    );

    assert!(
        q_col.collection.unwrap().labels[0]
            == KeyValue {
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
            },
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
            },
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
            },
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
        label_or_id_filter: Some(LabelOrIdQuery {
            labels: None,
            ids: vec![res_2_id.clone()],
        }),
        page_request: None,
    };
    // Expect result_2
    let quest_result = db.get_collections(q_col_req).unwrap();
    assert!(
        quest_result
            .collections
            .clone()
            .unwrap()
            .collection_overviews
            .len()
            == 1
    );
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
    assert!(
        quest_result
            .collections
            .clone()
            .unwrap()
            .collection_overviews
            .len()
            == 1
    );
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
    assert!(quest_result.collections.unwrap().collection_overviews.len() == 3);

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
                    },
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
    assert!(
        quest_result
            .collections
            .clone()
            .unwrap()
            .collection_overviews
            .len()
            == 1
    );
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
        page_request: Some(PageRequest {
            last_uuid: "".to_string(),
            page_size: 1,
        }),
    };
    // Expect all
    let quest_result_1 = db.get_collections(q_col_req).unwrap();
    assert!(
        quest_result_1
            .collections
            .clone()
            .unwrap()
            .collection_overviews
            .len()
            == 1
    );

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
                .collections
                .unwrap()
                .collection_overviews[0]
                .clone()
                .id,
            page_size: 1,
        }),
    };
    // Expect only one
    let quest_result_2 = db.get_collections(q_col_req).unwrap();
    assert!(
        quest_result_2
            .collections
            .as_ref()
            .unwrap()
            .collection_overviews
            .len()
            == 1
    );
    assert!(
        quest_result_1
            .collections
            .as_ref()
            .unwrap()
            .collection_overviews[0]
            .id
            != quest_result_2.collections.unwrap().collection_overviews[0].id
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
            last_uuid: quest_result_1.collections.unwrap().collection_overviews[0]
                .clone()
                .id,
            page_size: 1,
        }),
    };
    // Expect Error
    let quest_result_3 = db.get_collections(q_col_req);
    assert!(quest_result_3.is_err())
}

#[test]
#[ignore]
#[serial(db)]
fn update_collection_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    let creator = uuid::Uuid::parse_str("12345678-1234-1234-1234-111111111111").unwrap();

    let request = CreateNewCollectionRequest {
        name: "new_collection_update".to_owned(),
        description: "this_is_a_demo_collection_update".to_owned(),
        project_id: "12345678-1111-1111-1111-111111111111".to_owned(),
        labels: vec![KeyValue {
            key: "test_key".to_owned(),
            value: "test_value".to_owned(),
        }],
        hooks: vec![KeyValue {
            key: "test_key".to_owned(),
            value: "test_value".to_owned(),
        }],
        dataclass: 2,
    };

    let result = db.create_new_collection(request, creator).unwrap();
    let col_id = uuid::Uuid::from_str(&result.collection_id).unwrap();
    assert!(!col_id.is_nil());

    let endpoint_uuid = uuid::Uuid::parse_str("12345678-6666-6666-6666-999999999999").unwrap();

    // Add some objects and an objectgroup
    let new_obj_1 = InitializeNewObjectRequest {
        object: Some(StageObject {
            filename: "test_obj_1".to_string(),
            description: "test_obj_1_descr".to_string(),
            collection_id: col_id.to_string(),
            content_len: 5,
            source: None,
            dataclass: 2,
            labels: vec![KeyValue {
                key: "obj_1_key".to_string(),
                value: "obj_1_value".to_string(),
            }],
            hooks: Vec::new(),
        }),
        collection_id: col_id.to_string(),
        preferred_endpoint_id: endpoint_uuid.to_string(),
        multipart: false,
        is_specification: false,
    };
    let obj_1_id = uuid::Uuid::new_v4();

    let _sobj_1 = db
        .create_object(
            &new_obj_1,
            &creator,
            &(Location {
                r#type: 2,
                bucket: "a".to_string(),
                path: "b".to_string(),
            }),
            "uid".to_string(),
            endpoint_uuid,
            obj_1_id,
        )
        .unwrap();
    let f_obj_1_stage = FinishObjectStagingRequest {
        object_id: obj_1_id.to_string(),
        upload_id: "uid".to_string(),
        collection_id: col_id.to_string(),
        hash: None,
        no_upload: false,
        completed_parts: Vec::new(),
        auto_update: true,
    };

    let _res_1 = db.finish_object_staging(&f_obj_1_stage, &creator).unwrap();

    // Add some objects and an objectgroup
    let new_obj_2 = InitializeNewObjectRequest {
        object: Some(StageObject {
            filename: "test_obj_2".to_string(),
            description: "test_obj_2_descr".to_string(),
            collection_id: col_id.to_string(),
            content_len: 10,
            source: None,
            dataclass: 2,
            labels: vec![KeyValue {
                key: "obj_2_key".to_string(),
                value: "obj_2_value".to_string(),
            }],
            hooks: Vec::new(),
        }),
        collection_id: col_id.to_string(),
        preferred_endpoint_id: endpoint_uuid.to_string(),
        multipart: false,
        is_specification: false,
    };

    let obj_2_id = uuid::Uuid::new_v4();

    let _sobj_2 = db
        .create_object(
            &new_obj_2,
            &creator,
            &(Location {
                r#type: 2,
                bucket: "a".to_string(),
                path: "b".to_string(),
            }),
            "uid_2".to_string(),
            endpoint_uuid,
            obj_2_id,
        )
        .unwrap();
    let _f_obj_2_stage = FinishObjectStagingRequest {
        object_id: obj_2_id.to_string(),
        upload_id: "uid".to_string(),
        collection_id: col_id.to_string(),
        hash: None,
        no_upload: false,
        completed_parts: Vec::new(),
        auto_update: true,
    };

    let _res_2 = db.finish_object_staging(&f_obj_1_stage, &creator).unwrap();

    let obj_grp = CreateObjectGroupRequest {
        name: "test_group_1".to_string(),
        description: "test_group_2".to_string(),
        collection_id: col_id.to_string(),
        object_ids: vec![obj_1_id.to_string()],
        meta_object_ids: Vec::new(),
        labels: vec![KeyValue {
            key: "obj_grp_key".to_string(),
            value: "obj_grp_value".to_string(),
        }],
        hooks: Vec::new(),
    };

    let _obj_grp_res = db.create_object_group(&obj_grp, &creator).unwrap();

    let normal_update = UpdateCollectionRequest {
        project_id: "12345678-1111-1111-1111-111111111111".to_owned(),
        collection_id: col_id.to_string(),
        name: "new_name".to_string(),
        description: "new_descrpt".to_string(),
        labels: vec![KeyValue {
            key: "test_key".to_owned(),
            value: "test_value".to_owned(),
        }],
        hooks: vec![KeyValue {
            key: "test_key".to_owned(),
            value: "test_value".to_owned(),
        }],
        label_ontology: Vec::new(),
        dataclass: 2,
        version: None,
    };

    let up_res = db.update_collection(normal_update, creator).unwrap();

    assert_eq!(up_res.collection.unwrap().id, col_id.to_string());

    let pin_update = UpdateCollectionRequest {
        project_id: "12345678-1111-1111-1111-111111111111".to_owned(),
        collection_id: col_id.to_string(),
        name: "new_name".to_string(),
        description: "new_descrpt".to_string(),
        labels: vec![KeyValue {
            key: "test_key_2".to_owned(),
            value: "test_value_2".to_owned(),
        }],
        hooks: vec![KeyValue {
            key: "test_key_2".to_owned(),
            value: "test_value_2".to_owned(),
        }],
        label_ontology: Vec::new(),
        dataclass: 2,
        version: Some(Version {
            major: 1,
            minor: 1,
            patch: 1,
        }),
    };

    let pin_up_res = db.update_collection(pin_update, creator).unwrap();

    assert!(pin_up_res.collection.unwrap().id != col_id.to_string());
}

#[test]
#[ignore]
#[serial(db)]
fn pin_collection_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    let creator = uuid::Uuid::parse_str("12345678-1234-1234-1234-111111111111").unwrap();

    let request = CreateNewCollectionRequest {
        name: "new_collection_update".to_owned(),
        description: "this_is_a_demo_collection_update".to_owned(),
        project_id: "12345678-1111-1111-1111-111111111111".to_owned(),
        labels: vec![KeyValue {
            key: "test_key".to_owned(),
            value: "test_value".to_owned(),
        }],
        hooks: vec![KeyValue {
            key: "test_key".to_owned(),
            value: "test_value".to_owned(),
        }],
        dataclass: 2,
    };

    let result = db.create_new_collection(request, creator).unwrap();
    let col_id = uuid::Uuid::from_str(&result.collection_id).unwrap();
    assert!(!col_id.is_nil());

    let endpoint_uuid = uuid::Uuid::parse_str("12345678-6666-6666-6666-999999999999").unwrap();

    // Add some objects and an objectgroup
    let new_obj_1 = InitializeNewObjectRequest {
        object: Some(StageObject {
            filename: "test_obj_1".to_string(),
            description: "test_obj_1_descr".to_string(),
            collection_id: col_id.to_string(),
            content_len: 5,
            source: None,
            dataclass: 2,
            labels: vec![KeyValue {
                key: "obj_1_key".to_string(),
                value: "obj_1_value".to_string(),
            }],
            hooks: Vec::new(),
        }),
        collection_id: col_id.to_string(),
        preferred_endpoint_id: endpoint_uuid.to_string(),
        multipart: false,
        is_specification: false,
    };
    let obj_1_id = uuid::Uuid::new_v4();

    let _sobj_1 = db
        .create_object(
            &new_obj_1,
            &creator,
            &(Location {
                r#type: 2,
                bucket: "a".to_string(),
                path: "b".to_string(),
            }),
            "uid".to_string(),
            endpoint_uuid,
            obj_1_id,
        )
        .unwrap();
    let f_obj_1_stage = FinishObjectStagingRequest {
        object_id: obj_1_id.to_string(),
        upload_id: "uid".to_string(),
        collection_id: col_id.to_string(),
        hash: None,
        no_upload: false,
        completed_parts: Vec::new(),
        auto_update: true,
    };

    let _res_1 = db.finish_object_staging(&f_obj_1_stage, &creator).unwrap();

    // Add some objects and an objectgroup
    let new_obj_2 = InitializeNewObjectRequest {
        object: Some(StageObject {
            filename: "test_obj_2".to_string(),
            description: "test_obj_2_descr".to_string(),
            collection_id: col_id.to_string(),
            content_len: 10,
            source: None,
            dataclass: 2,
            labels: vec![KeyValue {
                key: "obj_2_key".to_string(),
                value: "obj_2_value".to_string(),
            }],
            hooks: Vec::new(),
        }),
        collection_id: col_id.to_string(),
        preferred_endpoint_id: endpoint_uuid.to_string(),
        multipart: false,
        is_specification: false,
    };

    let obj_2_id = uuid::Uuid::new_v4();

    let _sobj_2 = db
        .create_object(
            &new_obj_2,
            &creator,
            &(Location {
                r#type: 2,
                bucket: "a".to_string(),
                path: "b".to_string(),
            }),
            "uid_2".to_string(),
            endpoint_uuid,
            obj_2_id,
        )
        .unwrap();
    let _f_obj_2_stage = FinishObjectStagingRequest {
        object_id: obj_2_id.to_string(),
        upload_id: "uid".to_string(),
        collection_id: col_id.to_string(),
        hash: None,
        no_upload: false,
        completed_parts: Vec::new(),
        auto_update: true,
    };

    let _res_2 = db.finish_object_staging(&f_obj_1_stage, &creator).unwrap();

    let obj_grp = CreateObjectGroupRequest {
        name: "test_group_1".to_string(),
        description: "test_group_2".to_string(),
        collection_id: col_id.to_string(),
        object_ids: vec![obj_1_id.to_string()],
        meta_object_ids: Vec::new(),
        labels: vec![KeyValue {
            key: "obj_grp_key".to_string(),
            value: "obj_grp_value".to_string(),
        }],
        hooks: Vec::new(),
    };

    let _obj_grp_res = db.create_object_group(&obj_grp, &creator).unwrap();

    let pin_col_req = PinCollectionVersionRequest {
        collection_id: col_id.to_string(),
        version: Some(Version {
            major: 1,
            minor: 3,
            patch: 3,
        }),
    };
    let pin_up_res = db.pin_collection_version(pin_col_req, creator).unwrap();

    assert!(pin_up_res.collection.unwrap().id != col_id.to_string());
}

#[test]
#[ignore]
#[serial(db)]
fn delete_collection_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    let creator = uuid::Uuid::parse_str("12345678-1234-1234-1234-111111111111").unwrap();

    let request = CreateNewCollectionRequest {
        name: "new_collection_update_delete".to_owned(),
        description: "this_is_a_demo_collection_delete".to_owned(),
        project_id: "12345678-1111-1111-1111-111111111111".to_owned(),
        labels: vec![KeyValue {
            key: "delete_test_key".to_owned(),
            value: "delete_test_value".to_owned(),
        }],
        hooks: vec![KeyValue {
            key: "delete_test_key".to_owned(),
            value: "delete_test_value".to_owned(),
        }],
        dataclass: 4,
    };

    let result = db.create_new_collection(request, creator).unwrap();

    let ref_col_request = CreateNewCollectionRequest {
        name: "new_collection_update_delete".to_owned(),
        description: "this_is_a_demo_collection_delete".to_owned(),
        project_id: "12345678-1111-1111-1111-111111111111".to_owned(),
        labels: vec![KeyValue {
            key: "delete_test_key".to_owned(),
            value: "delete_test_value".to_owned(),
        }],
        hooks: vec![KeyValue {
            key: "delete_test_key".to_owned(),
            value: "delete_test_value".to_owned(),
        }],
        dataclass: 4,
    };

    let result_2 = db.create_new_collection(ref_col_request, creator).unwrap();
    let col_id = uuid::Uuid::from_str(&result.collection_id).unwrap();
    assert!(!col_id.is_nil());

    let endpoint_uuid = uuid::Uuid::parse_str("12345678-6666-6666-6666-999999999999").unwrap();

    // Add some objects and an objectgroup
    let new_obj_1 = InitializeNewObjectRequest {
        object: Some(StageObject {
            filename: "test_obj_1_del".to_string(),
            description: "test_obj_1_descr_del".to_string(),
            collection_id: col_id.to_string(),
            content_len: 5,
            source: None,
            dataclass: 2,
            labels: vec![KeyValue {
                key: "obj_1_key".to_string(),
                value: "obj_1_value".to_string(),
            }],
            hooks: Vec::new(),
        }),
        collection_id: col_id.to_string(),
        preferred_endpoint_id: endpoint_uuid.to_string(),
        multipart: false,
        is_specification: false,
    };
    let obj_1_id = uuid::Uuid::new_v4();

    let _sobj_1 = db
        .create_object(
            &new_obj_1,
            &creator,
            &(Location {
                r#type: 2,
                bucket: "a".to_string(),
                path: "b".to_string(),
            }),
            "uid".to_string(),
            endpoint_uuid,
            obj_1_id,
        )
        .unwrap();
    let f_obj_1_stage = FinishObjectStagingRequest {
        object_id: obj_1_id.to_string(),
        upload_id: "uid".to_string(),
        collection_id: col_id.to_string(),
        hash: None,
        no_upload: false,
        completed_parts: Vec::new(),
        auto_update: true,
    };

    let _res_1 = db.finish_object_staging(&f_obj_1_stage, &creator).unwrap();

    let obj_ref_req = CreateObjectReferenceRequest {
        object_id: obj_1_id.to_string(),
        collection_id: result.collection_id,
        target_collection_id: result_2.collection_id,
        writeable: true,
        auto_update: true,
    };
    let _obj_ref = db.create_object_reference(obj_ref_req);
    // Add some objects and an objectgroup
    let new_obj_2 = InitializeNewObjectRequest {
        object: Some(StageObject {
            filename: "test_obj_2".to_string(),
            description: "test_obj_2_descr".to_string(),
            collection_id: col_id.to_string(),
            content_len: 10,
            source: None,
            dataclass: 2,
            labels: vec![KeyValue {
                key: "obj_2_key".to_string(),
                value: "obj_2_value".to_string(),
            }],
            hooks: Vec::new(),
        }),
        collection_id: col_id.to_string(),
        preferred_endpoint_id: endpoint_uuid.to_string(),
        multipart: false,
        is_specification: false,
    };

    let obj_2_id = uuid::Uuid::new_v4();

    let _sobj_2 = db
        .create_object(
            &new_obj_2,
            &creator,
            &(Location {
                r#type: 2,
                bucket: "a".to_string(),
                path: "b".to_string(),
            }),
            "uid_2".to_string(),
            endpoint_uuid,
            obj_2_id,
        )
        .unwrap();
    let _f_obj_2_stage = FinishObjectStagingRequest {
        object_id: obj_2_id.to_string(),
        upload_id: "uid".to_string(),
        collection_id: col_id.to_string(),
        hash: None,
        no_upload: false,
        completed_parts: Vec::new(),
        auto_update: true,
    };

    let _res_2 = db.finish_object_staging(&f_obj_1_stage, &creator).unwrap();

    let obj_grp = CreateObjectGroupRequest {
        name: "test_group_1".to_string(),
        description: "test_group_2".to_string(),
        collection_id: col_id.to_string(),
        object_ids: vec![obj_1_id.to_string()],
        meta_object_ids: Vec::new(),
        labels: vec![KeyValue {
            key: "obj_grp_key".to_string(),
            value: "obj_grp_value".to_string(),
        }],
        hooks: Vec::new(),
    };

    let _obj_grp_res = db.create_object_group(&obj_grp, &creator).unwrap();

    let delete_req_normal = DeleteCollectionRequest {
        collection_id: col_id.to_string(),
        project_id: "12345678-1111-1111-1111-111111111111".to_owned(),
        force: false,
    };

    let res = db.delete_collection(delete_req_normal, creator);
    // This should fail !
    assert!(res.is_err());

    let delete_req_force = DeleteCollectionRequest {
        collection_id: col_id.to_string(),
        project_id: "12345678-1111-1111-1111-111111111111".to_owned(),
        force: true,
    };

    // Should not fail
    let _res = db.delete_collection(delete_req_force, creator).unwrap();
}
