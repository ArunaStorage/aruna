mod common;

use crate::common::functions::{
    create_collection, create_object, create_project, TCreateCollection, TCreateObject,
};

use aruna_rust_api::api::storage::models::v1::KeyValue;
use aruna_rust_api::api::storage::services::v1::{
    CreateObjectGroupRequest, DeleteObjectGroupRequest, GetObjectGroupByIdRequest,
    GetObjectGroupObjectsRequest, GetObjectGroupsRequest, UpdateObjectGroupRequest,
};
use aruna_server::database;
use aruna_server::error::ArunaError;
use diesel::result::Error;
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use serial_test::serial;

#[test]
#[ignore]
#[serial(db)]
#[allow(clippy::needless_collect)]
fn create_object_group_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let creator = uuid::Uuid::parse_str("12345678-1234-1234-1234-111111111111").unwrap();
    let endpoint_id = uuid::Uuid::parse_str("12345678-6666-6666-6666-999999999999").unwrap();

    // Create random project
    let random_project = create_project(None);

    // Create random collection
    let random_collection = create_collection(TCreateCollection {
        project_id: random_project.id,
        col_override: None,
        ..Default::default()
    });

    // Create 5 random objects
    let mut object_ids = (0..5)
        .map(|_| {
            create_object(
                &(TCreateObject {
                    sub_path: None,
                    creator_id: Some(creator.to_string()),
                    collection_id: random_collection.id.to_string(),
                    default_endpoint_id: Some(endpoint_id.to_string()),
                    num_labels: thread_rng().gen_range(0..4),
                    num_hooks: thread_rng().gen_range(0..4),
                }),
            )
            .id
        })
        .collect::<Vec<_>>();

    // Draw random sample from ids and remove element from vector
    let meta_object = object_ids.choose(&mut thread_rng()).unwrap().clone();
    object_ids.retain(|id| id != &meta_object);

    // Create ObjectGroup
    let object_group_name = "Create-Test-DummyGroup";
    let object_group_description = "Created within the create_object_group_test.";
    let create_request = CreateObjectGroupRequest {
        name: object_group_name.to_string(),
        description: object_group_description.to_string(),
        collection_id: random_collection.id.to_string(),
        object_ids: object_ids.clone(),
        meta_object_ids: vec![meta_object.clone()],
        labels: vec![],
        hooks: vec![],
    };

    let create_response = db.create_object_group(&create_request, &creator).unwrap();
    let object_group = create_response.object_group.unwrap();

    // Validate Object Group Creation
    assert_eq!(object_group.name, object_group_name.to_string());
    assert_eq!(
        object_group.description,
        object_group_description.to_string()
    );
    assert_eq!(object_group.rev_number, 0);
    assert_eq!(object_group.labels, vec!());
    assert_eq!(object_group.hooks, vec!());

    // Get ObjectGroup
    let get_group_request = GetObjectGroupByIdRequest {
        group_id: object_group.id.to_string(),
        collection_id: random_collection.id.to_string(),
    };

    let get_group_response = db.get_object_group_by_id(&get_group_request).unwrap();
    let group_overview = get_group_response.object_group.unwrap();

    // Validate ObjectGroup metadata
    assert_eq!(group_overview.name, object_group_name.to_string());
    assert_eq!(
        group_overview.description,
        object_group_description.to_string()
    );
    assert_eq!(group_overview.rev_number, 0);
    assert_eq!(group_overview.labels, vec!());
    assert_eq!(group_overview.hooks, vec!());

    // Get all Objects in ObjectGroup
    let get_all_request = GetObjectGroupObjectsRequest {
        collection_id: random_collection.id.to_string(),
        group_id: object_group.id.to_string(),
        page_request: None,
        meta_only: false,
    };
    let get_meta_request = GetObjectGroupObjectsRequest {
        collection_id: random_collection.id,
        group_id: object_group.id,
        page_request: None,
        meta_only: true,
    };

    let all_object_group_objects = db.get_object_group_objects(get_all_request).unwrap();
    let meta_object_group_objects = db.get_object_group_objects(get_meta_request).unwrap();

    // Validate objects associated with the ObjectGroup
    assert_eq!(all_object_group_objects.object_group_objects.len(), 5);
    assert_eq!(meta_object_group_objects.object_group_objects.len(), 1);

    let all_object_ids = all_object_group_objects
        .object_group_objects
        .iter()
        .filter(|o| !o.is_metadata)
        .map(|o| o.object.clone().unwrap().id)
        .collect::<Vec<_>>();

    for id in object_ids.clone() {
        assert!(all_object_ids.contains(&id));
    }

    for ogo in meta_object_group_objects.object_group_objects {
        let object = ogo.object.unwrap();
        assert_eq!(object.id, meta_object);
    }
}

#[test]
#[ignore]
#[serial(db)]
#[allow(clippy::needless_collect)]
fn update_object_group_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let creator = uuid::Uuid::parse_str("12345678-1234-1234-1234-111111111111").unwrap();
    let endpoint_id = uuid::Uuid::parse_str("12345678-6666-6666-6666-999999999999").unwrap();

    // Create random project
    let random_project = create_project(None);

    // Create random collection
    let random_collection = create_collection(TCreateCollection {
        project_id: random_project.id,
        col_override: None,
        ..Default::default()
    });

    // Create 5 random objects
    let mut object_ids = (0..5)
        .map(|_| {
            create_object(
                &(TCreateObject {
                    sub_path: None,
                    creator_id: Some(creator.to_string()),
                    collection_id: random_collection.id.to_string(),
                    default_endpoint_id: Some(endpoint_id.to_string()),
                    num_labels: thread_rng().gen_range(0..4),
                    num_hooks: thread_rng().gen_range(0..4),
                }),
            )
            .id
        })
        .collect::<Vec<_>>();

    // Draw random sample from ids and remove element from vector
    let meta_object = object_ids.choose(&mut thread_rng()).unwrap().clone();
    object_ids.retain(|id| id != &meta_object);

    // Create ObjectGroup
    let object_group_name = "Update-Test-DummyGroup";
    let object_group_description = "Created within the update_object_group_test.";
    let create_request = CreateObjectGroupRequest {
        name: object_group_name.to_string(),
        description: object_group_description.to_string(),
        collection_id: random_collection.id.to_string(),
        object_ids: object_ids.clone(),
        meta_object_ids: vec![meta_object.clone()],
        labels: vec![],
        hooks: vec![],
    };

    let create_response = db.create_object_group(&create_request, &creator).unwrap();
    let object_group_rev_0 = create_response.object_group.unwrap();

    // Update ObjectGroup description to generate second revision
    let updated_description = "Updated description within update_object_group_test.";
    let first_update_request = UpdateObjectGroupRequest {
        group_id: object_group_rev_0.id,
        name: object_group_name.to_string(),
        description: updated_description.to_string(),
        collection_id: random_collection.id.to_string(),
        object_ids: object_ids.clone(),
        meta_object_ids: vec![meta_object.clone()],
        labels: vec![],
        hooks: vec![],
    };
    let first_update_response = db
        .update_object_group(&first_update_request, &creator)
        .unwrap();
    let object_group_rev_1 = first_update_response.object_group.unwrap();

    // Validate first ObjectGroup Update
    assert_eq!(object_group_rev_1.name, object_group_name.to_string());
    assert_eq!(
        object_group_rev_1.description,
        updated_description.to_string()
    );
    assert_eq!(object_group_rev_1.rev_number, 1);
    assert_eq!(object_group_rev_1.labels, vec!());
    assert_eq!(object_group_rev_1.hooks, vec!());

    // Update ObjectGroup labels to generate third revision
    let updated_labels = vec![KeyValue {
        key: "isUpdated".to_owned(),
        value: "true".to_owned(),
    }];
    let second_update_request = UpdateObjectGroupRequest {
        group_id: object_group_rev_1.id,
        name: object_group_name.to_string(),
        description: updated_description.to_string(),
        collection_id: random_collection.id.to_string(),
        object_ids: object_ids.clone(),
        meta_object_ids: vec![meta_object.clone()],
        labels: updated_labels.clone(),
        hooks: vec![],
    };
    let second_update_response = db
        .update_object_group(&second_update_request, &creator)
        .unwrap();
    let object_group_rev_2 = second_update_response.object_group.unwrap();

    // Validate second Object Group Update
    assert_eq!(object_group_rev_2.name, object_group_name.to_string());
    assert_eq!(
        object_group_rev_2.description,
        updated_description.to_string()
    );
    assert_eq!(object_group_rev_2.rev_number, 2);
    assert_eq!(object_group_rev_2.labels, updated_labels);
    assert_eq!(object_group_rev_2.hooks, vec!());

    // Update ObjectGroup object ids to generate fourth revision
    let removed_object = object_ids.choose(&mut thread_rng()).unwrap().clone();
    object_ids.retain(|id| id != &removed_object);

    let third_update_request = UpdateObjectGroupRequest {
        group_id: object_group_rev_2.id,
        name: object_group_name.to_string(),
        description: updated_description.to_string(),
        collection_id: random_collection.id.to_string(),
        object_ids: object_ids.clone(),
        meta_object_ids: vec![meta_object.clone()],
        labels: updated_labels.clone(),
        hooks: vec![],
    };
    let third_update_response = db
        .update_object_group(&third_update_request, &creator)
        .unwrap();
    let object_group_rev_3 = third_update_response.object_group.unwrap();

    // Validate third Object Group Update
    assert_eq!(object_group_rev_3.name, object_group_name.to_string());
    assert_eq!(
        object_group_rev_3.description,
        updated_description.to_string()
    );
    assert_eq!(object_group_rev_3.rev_number, 3);
    assert_eq!(object_group_rev_3.labels, updated_labels);
    assert_eq!(object_group_rev_3.hooks, vec!());

    let all_object_group_objects = db
        .get_object_group_objects(GetObjectGroupObjectsRequest {
            collection_id: random_collection.id.to_string(),
            group_id: object_group_rev_3.id.to_string(),
            page_request: None,
            meta_only: false,
        })
        .unwrap();
    let meta_object_group_objects = db
        .get_object_group_objects(GetObjectGroupObjectsRequest {
            collection_id: random_collection.id,
            group_id: object_group_rev_3.id,
            page_request: None,
            meta_only: true,
        })
        .unwrap();

    // Validate objects associated with the ObjectGroup
    assert_eq!(all_object_group_objects.object_group_objects.len(), 4);
    assert_eq!(meta_object_group_objects.object_group_objects.len(), 1);

    let all_object_ids = all_object_group_objects
        .object_group_objects
        .iter()
        .filter(|o| !o.is_metadata)
        .map(|o| o.object.clone().unwrap().id)
        .collect::<Vec<_>>();

    for id in object_ids.clone() {
        assert!(all_object_ids.contains(&id));
    }

    for ogo in meta_object_group_objects.object_group_objects {
        let object = ogo.object.unwrap();
        assert_eq!(object.id, meta_object);
    }
}

#[test]
#[ignore]
#[serial(db)]
fn delete_object_group_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let creator = uuid::Uuid::parse_str("12345678-1234-1234-1234-111111111111").unwrap();
    let endpoint_id = uuid::Uuid::parse_str("12345678-6666-6666-6666-999999999999").unwrap();

    // Create random project
    let random_project = create_project(None);

    // Create random collection
    let random_collection = create_collection(TCreateCollection {
        project_id: random_project.id,
        col_override: None,
        ..Default::default()
    });

    // Create 5 random objects
    let mut object_ids = (0..5)
        .map(|_| {
            create_object(
                &(TCreateObject {
                    sub_path: None,
                    creator_id: Some(creator.to_string()),
                    collection_id: random_collection.id.to_string(),
                    default_endpoint_id: Some(endpoint_id.to_string()),
                    num_labels: thread_rng().gen_range(0..4),
                    num_hooks: thread_rng().gen_range(0..4),
                }),
            )
            .id
        })
        .collect::<Vec<_>>();

    // Draw random sample from ids and remove element from vector
    let meta_object = object_ids.choose(&mut thread_rng()).unwrap().clone();
    object_ids.retain(|id| id != &meta_object);

    // Create ObjectGroup
    let object_group_name = "Update-Test-DummyGroup";
    let object_group_description = "Created within the update_object_group_test.";
    let object_group_label = vec![KeyValue {
        key: "isDeleted".to_owned(),
        value: "false".to_owned(),
    }];
    let object_group_hook = vec![KeyValue {
        key: "deleteMe".to_owned(),
        value: "https://<path-to-aos-instance-api-gateway>/objectgroup/delete".to_owned(),
    }];
    let create_request = CreateObjectGroupRequest {
        name: object_group_name.to_string(),
        description: object_group_description.to_string(),
        collection_id: random_collection.id.to_string(),
        object_ids: object_ids.clone(),
        meta_object_ids: vec![meta_object.clone()],
        labels: object_group_label.clone(),
        hooks: object_group_hook.clone(),
    };

    let create_response = db.create_object_group(&create_request, &creator).unwrap();
    let object_group_rev_0 = create_response.object_group.unwrap();

    // Update ObjectGroup description to generate second revision
    let updated_description = "Updated description within update_object_group_test.";
    let first_update_request = UpdateObjectGroupRequest {
        group_id: object_group_rev_0.id.to_string(),
        name: object_group_name.to_string(),
        description: updated_description.to_string(),
        collection_id: random_collection.id.to_string(),
        object_ids: object_ids.clone(),
        meta_object_ids: vec![meta_object.clone()],
        labels: object_group_label,
        hooks: object_group_hook,
    };
    let first_update_response = db
        .update_object_group(&first_update_request, &creator)
        .unwrap();
    let object_group_rev_1 = first_update_response.object_group.unwrap();

    // Delete ObjectGroup revision 0
    db.delete_object_group(DeleteObjectGroupRequest {
        group_id: object_group_rev_0.id.to_string(),
        collection_id: random_collection.id.to_string(),
    })
    .unwrap();

    // Validate deletion of revision 0
    let get_rev_0_response = db
        .get_object_group_by_id(
            &(GetObjectGroupByIdRequest {
                group_id: object_group_rev_0.id.to_string(),
                collection_id: random_collection.id.to_string(),
            }),
        )
        .unwrap();
    let deleted_object_group_rev_0 = get_rev_0_response.object_group.unwrap();

    assert_eq!(deleted_object_group_rev_0.name, "DELETED");
    assert_eq!(deleted_object_group_rev_0.description, "DELETED");
    assert_eq!(deleted_object_group_rev_0.rev_number, 0);
    assert_eq!(deleted_object_group_rev_0.labels.len(), 0);
    assert_eq!(deleted_object_group_rev_0.hooks.len(), 0);

    let collection_object_groups_response = db
        .get_object_groups(GetObjectGroupsRequest {
            collection_id: random_collection.id.to_string(),
            page_request: None,
            label_id_filter: None,
        })
        .unwrap();
    let collection_object_groups = collection_object_groups_response.object_groups.unwrap();

    assert_eq!(collection_object_groups.object_group_overviews.len(), 1); // Latest revision should still be available

    // Delete ObjectGroup revision 1 and therefore the whole ObjectGroup permanently
    db.delete_object_group(DeleteObjectGroupRequest {
        group_id: object_group_rev_1.id.to_string(),
        collection_id: random_collection.id.to_string(),
    })
    .unwrap();

    // Validate permanent deletion of revision 0
    let get_deleted_rev_0_response = db.get_object_group_by_id(
        &(GetObjectGroupByIdRequest {
            group_id: object_group_rev_0.id,
            collection_id: random_collection.id.to_string(),
        }),
    );

    match get_deleted_rev_0_response {
        Ok(_) => panic!("ObjectGroup revision 0 should have been removed from database."),
        Err(err) => {
            (match &err {
                ArunaError::DieselError(Error::NotFound) => Ok(()),
                _ => Err(err),
            })
            .unwrap();
        }
    }

    // Validate permanent deletion of revision 1
    let get_deleted_rev_1_response = db.get_object_group_by_id(
        &(GetObjectGroupByIdRequest {
            group_id: object_group_rev_1.id,
            collection_id: random_collection.id,
        }),
    );

    match get_deleted_rev_1_response {
        Ok(_) => panic!("ObjectGroup revision 1 should have been removed from database."),
        Err(err) => {
            (match &err {
                ArunaError::DieselError(Error::NotFound) => Ok(()),
                _ => Err(err),
            })
            .unwrap();
        }
    }
}
