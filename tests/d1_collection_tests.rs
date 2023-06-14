mod common;
use aruna_rust_api::api::storage::models::v1::{
    collection_overview, collection_overview::Version::SemanticVersion, DataClass, Hashalgorithm,
    KeyValue, LabelFilter, LabelOntology, LabelOrIdQuery, PageRequest, Version,
};

use aruna_rust_api::api::storage::services::v1::*;
use aruna_server::database;
use common::functions::{create_collection, TCreateCollection, TCreateObject};
use serial_test::serial;
use std::str::FromStr;

#[test]
#[ignore]
#[serial(db)]
fn create_new_collection_test() {
    let created_project = common::functions::create_project(None);
    // Create collection in project
    create_collection(TCreateCollection {
        project_id: created_project.id,
        ..Default::default()
    });
}

#[test]
#[ignore]
#[serial(db)]
fn get_collection_by_id_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    let created_project = common::functions::create_project(None);

    let request = CreateNewCollectionRequest {
        name: "new-collection".to_owned(),
        description: "this_is_a_demo_collection".to_owned(),
        project_id: common::functions::get_regular_project_ulid().to_string(),
        label_ontology: None,
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
    // Create collection in project
    let result = create_collection(TCreateCollection {
        project_id: created_project.id,
        col_override: Some(request),
        ..Default::default()
    });

    // Get collection by ID
    let q_col_req = GetCollectionByIdRequest {
        collection_id: result.id,
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
        "new-collection".to_string()
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
    let creator = common::functions::get_admin_user_ulid();
    let regular_project_ulid = common::functions::get_regular_project_ulid();

    let request = CreateNewCollectionRequest {
        name: "new-collection-1".to_owned(),
        description: "this_is_a_demo_collection_1".to_owned(),
        project_id: regular_project_ulid.to_string(),
        label_ontology: None,
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
        name: "new-collection-2".to_owned(),
        description: "this_is_a_demo_collection_2".to_owned(),
        project_id: regular_project_ulid.to_string(),
        label_ontology: None,
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
    let res_2_id = result_2.0.collection_id;

    let request = CreateNewCollectionRequest {
        name: "new-collection-3".to_owned(),
        description: "this_is_a_demo_collection_3".to_owned(),
        project_id: regular_project_ulid.to_string(),
        label_ontology: None,
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
        project_id: regular_project_ulid.to_string(),
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
        project_id: regular_project_ulid.to_string(),
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
    assert!(
        quest_result.collections.unwrap().collection_overviews[0].id == result_1.0.collection_id
    );

    // 2. Label filter (3)
    let q_col_req = GetCollectionsRequest {
        project_id: regular_project_ulid.to_string(),
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
        project_id: regular_project_ulid.to_string(),
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
    assert!(
        quest_result.collections.unwrap().collection_overviews[0].id == result_3.0.collection_id
    );

    // 2. PageRequest (1)
    let q_col_req = GetCollectionsRequest {
        project_id: regular_project_ulid.to_string(),
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
        project_id: regular_project_ulid.to_string(),
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
        project_id: regular_project_ulid.to_string(),
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
    let creator = common::functions::get_admin_user_ulid();

    let created_project = common::functions::create_project(None);
    // Create collection in project
    let result = create_collection(TCreateCollection {
        project_id: created_project.id,
        ..Default::default()
    });
    let col_id = diesel_ulid::DieselUlid::from_str(&result.id).unwrap();
    assert!(!col_id.to_string().is_empty());

    // Define mutable request to reuse for the individual updates
    let mut update_request = UpdateCollectionRequest {
        collection_id: result.id.to_string(),
        name: "plain-updated-collection".to_string(),
        description: "Some description.".to_string(),
        labels: vec![],
        hooks: vec![],
        label_ontology: None,
        dataclass: DataClass::Public as i32,
        version: None,
    };

    // Update plain collection name, description and data class
    let updated_collection = db
        .update_collection(update_request.clone(), creator)
        .unwrap()
        .0
        .collection
        .unwrap();

    assert_eq!(updated_collection.id, result.id);
    assert_eq!(updated_collection.name.as_str(), "plain-updated-collection");
    assert_eq!(updated_collection.description.as_str(), "Some description.");
    assert_eq!(updated_collection.labels, vec![]);
    assert_eq!(updated_collection.hooks, vec![]);
    assert_eq!(
        updated_collection.label_ontology,
        Some(LabelOntology {
            required_label_keys: vec![]
        })
    );
    assert!(updated_collection.is_public);

    // Update plain collection labels/hooks
    update_request.labels = vec![KeyValue {
        key: "my_little_label".to_owned(),
        value: "test_value".to_owned(),
    }];
    update_request.hooks = vec![KeyValue {
        key: "my_little_hook".to_owned(),
        value: "test_value".to_owned(),
    }];

    let updated_collection = db
        .update_collection(update_request.clone(), creator)
        .unwrap()
        .0
        .collection
        .unwrap();

    assert_eq!(updated_collection.id, result.id);
    assert_eq!(updated_collection.name.as_str(), "plain-updated-collection");
    assert_eq!(updated_collection.description.as_str(), "Some description.");
    assert_eq!(
        updated_collection.labels,
        vec![KeyValue {
            key: "my_little_label".to_owned(),
            value: "test_value".to_owned(),
        }]
    );
    assert_eq!(
        updated_collection.hooks,
        vec![KeyValue {
            key: "my_little_hook".to_owned(),
            value: "test_value".to_owned(),
        }]
    );
    assert_eq!(
        updated_collection.label_ontology,
        Some(LabelOntology {
            required_label_keys: vec![]
        })
    );
    assert!(updated_collection.is_public);

    // Add some objects
    let _rand_object_01 = common::functions::create_object(&TCreateObject {
        collection_id: updated_collection.id.to_string(),
        ..Default::default()
    });

    let _rand_object_02 = common::functions::create_object(&TCreateObject {
        collection_id: updated_collection.id.to_string(),
        ..Default::default()
    });

    // Update label ontology --> Error, as objects do not have required labels
    update_request.label_ontology = Some(LabelOntology {
        required_label_keys: vec!["dummy_label".to_owned()],
    });

    let error_collection = db.update_collection(update_request.clone(), creator);

    assert!(error_collection.is_err());

    // Try to update collection name --> Error, as name update is only allowed with empty collections
    update_request.label_ontology = None;
    update_request.name = "error-collection".to_string();

    let error_collection = db.update_collection(update_request.clone(), creator);

    assert!(error_collection.is_err());

    //ToDo: Update collection with pin to version
    update_request.name = "archived-collection".to_string();
    update_request.version = Some(Version {
        major: 1,
        minor: 0,
        patch: 0,
    });
    update_request.dataclass = DataClass::Private as i32;

    let archived_collection = db
        .update_collection(update_request, creator)
        .unwrap()
        .0
        .collection
        .unwrap();

    assert_ne!(archived_collection.id, updated_collection.id);
    assert_eq!(archived_collection.name.as_str(), "archived-collection");
    assert_eq!(
        archived_collection.description.as_str(),
        "Some description."
    );
    assert_eq!(
        archived_collection.labels,
        vec![KeyValue {
            key: "my_little_label".to_owned(),
            value: "test_value".to_owned(),
        }]
    );
    assert_eq!(
        archived_collection.hooks,
        vec![KeyValue {
            key: "my_little_hook".to_owned(),
            value: "test_value".to_owned(),
        }]
    );
    assert_eq!(
        archived_collection.label_ontology,
        Some(LabelOntology {
            required_label_keys: vec![]
        })
    );
    assert_eq!(
        archived_collection.version,
        Some(SemanticVersion(Version {
            major: 1,
            minor: 0,
            patch: 0
        }))
    );
    assert!(!archived_collection.is_public)
}

#[test]
#[ignore]
#[serial(db)]
fn pin_collection_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let creator = common::functions::get_admin_user_ulid();

    let request = CreateNewCollectionRequest {
        name: "pin-collection-test-collection-001".to_owned(),
        description: "Collection created in update_collection_test()".to_owned(),
        project_id: common::functions::get_regular_project_ulid().to_string(),
        label_ontology: None,
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
    let col_id = diesel_ulid::DieselUlid::from_str(&result.0.collection_id).unwrap();
    assert!(!col_id.to_string().is_empty());

    let endpoint_uuid = common::functions::get_default_endpoint_ulid();

    // Add some objects and an objectgroup
    let new_obj_1 = InitializeNewObjectRequest {
        object: Some(StageObject {
            filename: "test_obj_1".to_string(),
            sub_path: "".to_string(),
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
        hash: None,
    };
    let obj_1_id = diesel_ulid::DieselUlid::generate();

    let _sobj_1 = db
        .create_object(&new_obj_1, &creator, obj_1_id, &endpoint_uuid)
        .unwrap();
    let f_obj_1_stage = FinishObjectStagingRequest {
        object_id: obj_1_id.to_string(),
        upload_id: "uid".to_string(),
        collection_id: col_id.to_string(),
        hash: None,
        no_upload: true,
        completed_parts: Vec::new(),
        auto_update: true,
    };

    let _res_1 = db.finish_object_staging(&f_obj_1_stage, &creator).unwrap();

    // Add some objects and an objectgroup
    let new_obj_2 = InitializeNewObjectRequest {
        object: Some(StageObject {
            filename: "test_obj_2".to_string(),
            sub_path: "".to_string(),
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
        hash: None,
    };

    let obj_2_id = diesel_ulid::DieselUlid::generate();

    let _sobj_2 = db
        .create_object(&new_obj_2, &creator, obj_2_id, &endpoint_uuid)
        .unwrap();
    let _f_obj_2_stage = FinishObjectStagingRequest {
        object_id: obj_2_id.to_string(),
        upload_id: "uid".to_string(),
        collection_id: col_id.to_string(),
        hash: None,
        no_upload: true,
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

    assert!(pin_up_res.0.collection.unwrap().id != col_id.to_string());
}

#[test]
#[ignore]
#[serial(db)]
fn delete_collection_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let creator = common::functions::get_admin_user_ulid();
    let regular_project_ulid = common::functions::get_regular_project_ulid();

    let request = CreateNewCollectionRequest {
        name: "new-collection-update-delete".to_owned(),
        description: "this_is_a_demo_collection_delete".to_owned(),
        project_id: regular_project_ulid.to_string(),
        label_ontology: None,
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
        name: "new-collection-update-delete".to_owned(),
        description: "this_is_a_demo_collection_delete".to_owned(),
        project_id: regular_project_ulid.to_string(),
        label_ontology: None,
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
    let col_id = diesel_ulid::DieselUlid::from_str(&result.0.collection_id).unwrap();
    assert!(!col_id.to_string().is_empty());

    let endpoint_uuid = common::functions::get_default_endpoint_ulid();

    // Add some objects and an objectgroup
    let new_obj_1 = InitializeNewObjectRequest {
        object: Some(StageObject {
            filename: "test_obj_1_del".to_string(),
            sub_path: "".to_string(),
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
        hash: None,
    };
    let obj_1_id = diesel_ulid::DieselUlid::generate();

    let _sobj_1 = db
        .create_object(&new_obj_1, &creator, obj_1_id, &endpoint_uuid)
        .unwrap();
    let f_obj_1_stage = FinishObjectStagingRequest {
        object_id: obj_1_id.to_string(),
        upload_id: "uid".to_string(),
        collection_id: col_id.to_string(),
        hash: None,
        no_upload: true,
        completed_parts: Vec::new(),
        auto_update: true,
    };

    let _res_1 = db.finish_object_staging(&f_obj_1_stage, &creator).unwrap();

    let obj_ref_req = CreateObjectReferenceRequest {
        object_id: obj_1_id.to_string(),
        collection_id: result.0.collection_id,
        target_collection_id: result_2.0.collection_id,
        writeable: true,
        auto_update: true,
        sub_path: "".to_string(),
    };
    let _obj_ref = db.create_object_reference(obj_ref_req);
    // Add some objects and an objectgroup
    let new_obj_2 = InitializeNewObjectRequest {
        object: Some(StageObject {
            filename: "test_obj_2".to_string(),
            sub_path: "".to_string(),
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
        hash: None,
    };

    let obj_2_id = diesel_ulid::DieselUlid::generate();

    let _sobj_2 = db
        .create_object(&new_obj_2, &creator, obj_2_id, &endpoint_uuid)
        .unwrap();
    let _f_obj_2_stage = FinishObjectStagingRequest {
        object_id: obj_2_id.to_string(),
        upload_id: "uid".to_string(),
        collection_id: col_id.to_string(),
        hash: None,
        no_upload: true,
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

    let res = db.delete_collection(&col_id, creator, false);
    // This should fail !
    assert!(res.is_err());

    // Should not fail
    let _res = db.delete_collection(&col_id, creator, true);
}

#[test]
#[ignore]
#[serial(db)]
pub fn test_materialized_view_refreshs() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let result = db.update_collection_views();
    assert!(result.is_ok());
    let result = db.update_object_group_views();
    assert!(result.is_ok());
}

#[test]
#[ignore]
#[serial(db)]
pub fn test_collection_materialized_views_stats() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let creator = common::functions::get_admin_user_ulid();
    let endpoint_id = common::functions::get_default_endpoint_ulid();

    // Create fresh Project
    let create_project_request = CreateProjectRequest {
        name: "test-collection-materialized-views-stats-project".to_string(),
        description: "Collection created for test_collection_materialized_views_stats()"
            .to_string(),
    };

    let project_ulid = db.create_project(create_project_request, creator).unwrap();
    assert!(!project_ulid.to_string().is_empty());

    // Create Collection
    let create_collection_request = CreateNewCollectionRequest {
        name: "test-collection-materialized-views-stats-collection".to_string(),
        description: "Test collection used in materialized view stats test.".to_string(),
        label_ontology: None,
        project_id: project_ulid.to_string(),
        labels: vec![],
        hooks: vec![],
        dataclass: DataClass::Private as i32,
    };
    let create_collection_response = db
        .create_new_collection(create_collection_request, creator)
        .unwrap();
    let collection_id =
        diesel_ulid::DieselUlid::from_str(&create_collection_response.0.collection_id).unwrap();

    // Create Object
    let new_object_id = diesel_ulid::DieselUlid::generate();
    let upload_id = "".to_string();

    let init_object_request = InitializeNewObjectRequest {
        object: Some(StageObject {
            filename: "File.file".to_string(),
            sub_path: "".to_string(),
            content_len: 1337,
            source: None,
            dataclass: DataClass::Private as i32,
            labels: vec![KeyValue {
                key: "LabelKey".to_string(),
                value: "LabelValue".to_string(),
            }],
            hooks: vec![KeyValue {
                key: "HookKey".to_string(),
                value: "HookValue".to_string(),
            }],
        }),
        collection_id: collection_id.to_string(),
        preferred_endpoint_id: endpoint_id.to_string(),
        multipart: false,
        is_specification: false,
        hash: None,
    };

    let init_object_response = db
        .create_object(&init_object_request, &creator, new_object_id, &endpoint_id)
        .unwrap();

    assert_eq!(&init_object_response.object_id, &new_object_id.to_string());
    assert_eq!(
        &init_object_response.collection_id,
        &collection_id.to_string()
    );
    assert_eq!(&init_object_response.upload_id, &upload_id);

    // Finish object staging
    let finish_hash = aruna_rust_api::api::storage::models::v1::Hash {
        alg: Hashalgorithm::Sha256 as i32,
        hash: "f60b102aa455f085df91ffff53b3c0acd45c10f02782b953759ab10973707a92".to_string(),
    };
    let finish_request = FinishObjectStagingRequest {
        object_id: new_object_id.to_string(),
        upload_id,
        collection_id: collection_id.to_string(),
        hash: Some(finish_hash),
        no_upload: true,
        completed_parts: vec![],
        auto_update: true,
    };

    let _finish_response = db.finish_object_staging(&finish_request, &creator).unwrap();

    // Objects initialized -> refresh views

    let result = db.update_collection_views();
    assert!(result.is_ok());

    // Get collection by ID
    let q_col_req = GetCollectionByIdRequest {
        collection_id: collection_id.to_string(),
    };
    let queried_col = db.get_collection_by_id(q_col_req).unwrap();

    // Acc size should be 1337
    assert_eq!(
        queried_col
            .collection
            .unwrap()
            .stats
            .unwrap()
            .object_stats
            .unwrap()
            .acc_size,
        1337
    )
}
