mod common;

use crate::common::functions::{get_object, get_object_status_raw, TCreateCollection};
use aruna_rust_api::api::storage::models::v1::{
    DataClass, Hash as DbHash, Hashalgorithm, KeyValue, PageRequest, Version,
};
use aruna_rust_api::api::storage::services::v1::{
    CloneObjectRequest, CreateNewCollectionRequest, CreateObjectReferenceRequest,
    CreateProjectRequest, DeleteObjectRequest, DeleteObjectsRequest, FinishObjectStagingRequest,
    GetLatestObjectRevisionRequest, GetObjectByIdRequest, GetObjectRevisionsRequest,
    GetObjectsRequest, InitializeNewObjectRequest, ObjectWithUrl, PinCollectionVersionRequest,
    StageObject, UpdateObjectRequest,
};
use aruna_server::database;
use aruna_server::database::crud::utils::grpc_to_db_object_status;
use aruna_server::database::models::enums::ObjectStatus;
use common::functions::{
    create_collection, create_object, create_project, TCreateObject, TCreateUpdate,
};
use rand::{thread_rng, Rng};
use serial_test::serial;
use std::hash::{Hash, Hasher};
use std::str::FromStr;

// Wrap struct to implement traits needed for generic comparison
struct MyObjectWithUrl(ObjectWithUrl);

impl Eq for MyObjectWithUrl {}

impl PartialEq for MyObjectWithUrl {
    fn eq(&self, other: &Self) -> bool {
        self.0.object == other.0.object
            && self.0.url == other.0.url
            && self.0.paths == other.0.paths
    }
}

/// Implement hash for MyObjectWithUrl but only include proto object id ...
impl Hash for MyObjectWithUrl {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match &self.0.object {
            None => panic!("Nope."),
            Some(obj) => obj.id.hash(state),
        }
    }
}

#[test]
#[ignore]
#[serial(db)]
fn create_object_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let creator = common::functions::get_admin_user_ulid();
    let endpoint_id = common::functions::get_default_endpoint_ulid();

    // Create Project
    let create_project_request = CreateProjectRequest {
        name: "create-object-test-project-001".to_string(),
        description: "Project created in create_object_test()".to_string(),
    };

    let create_project_response = db.create_project(create_project_request, creator).unwrap();
    let project_id =
        diesel_ulid::DieselUlid::from_str(&create_project_response.project_id).unwrap();

    assert!(!project_id.to_string().is_empty());

    // Create Collection
    let create_collection_request = CreateNewCollectionRequest {
        name: "create-object-test-collection".to_string(),
        description: "Test collection used in create_object_test().".to_string(),
        label_ontology: None,
        project_id: project_id.to_string(),
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
            content_len: 1234,
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
    let finish_hash = DbHash {
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

    let finish_response = db.finish_object_staging(&finish_request, &creator).unwrap();
    let finished_object = finish_response.object.unwrap();
    assert_eq!(finished_object.id, new_object_id.to_string());
    assert!(matches!(
        grpc_to_db_object_status(&finished_object.status),
        ObjectStatus::AVAILABLE
    ));
    assert_eq!(finished_object.data_class, DataClass::Private as i32);
    assert_eq!(finished_object.rev_number, 0);
    assert_eq!(finished_object.filename, "File.file".to_string());
    assert_eq!(finished_object.content_len, 1234);
    //assert!(finished_object.hashes.contains(&finish_hash));
    assert!(finished_object.auto_update);
}

#[test]
#[ignore]
#[serial(db)]
fn update_object_test() {
    // Create random project
    let rand_project = common::functions::create_project(None);

    // Create random collection
    let rand_collection = common::functions::create_collection(TCreateCollection {
        project_id: rand_project.id,
        col_override: None,
        ..Default::default()
    });

    // Create an object
    let object = common::functions::create_object(&TCreateObject {
        collection_id: rand_collection.id.to_string(),
        ..Default::default()
    });

    // Update 1

    let update_1 = common::functions::update_object(&TCreateUpdate {
        original_object: object,
        collection_id: rand_collection.id.to_string(),
        new_name: "SuperName".to_string(),
        ..Default::default()
    });

    // Update Object again
    let update_2 = common::functions::update_object(&TCreateUpdate {
        original_object: update_1,
        collection_id: rand_collection.id,
        new_name: "File.next.update".to_string(),
        content_len: 123456,
        ..Default::default()
    });

    // Validate update
    assert!(matches!(
        grpc_to_db_object_status(&update_2.status),
        ObjectStatus::AVAILABLE
    ));
    assert_eq!(update_2.rev_number, 2);
    assert_eq!(update_2.filename, "File.next.update".to_string());
    assert_eq!(update_2.content_len, 123456);
    assert!(update_2.auto_update);
}

#[test]
#[ignore]
#[serial(db)]
fn update_object_with_reference_test() {
    // Create db connection
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Create random project
    let rand_project = common::functions::create_project(None);

    // Create random collection
    let rand_collection = common::functions::create_collection(TCreateCollection {
        project_id: rand_project.id.clone(),
        col_override: None,
        ..Default::default()
    });

    // Create second random collection
    let rand_collection_2 = common::functions::create_collection(TCreateCollection {
        project_id: rand_project.id,
        col_override: None,
        ..Default::default()
    });

    // Create an object
    let object = common::functions::create_object(&TCreateObject {
        collection_id: rand_collection.id.to_string(),
        ..Default::default()
    });

    // Create auto_updating reference in col 2

    let create_ref = CreateObjectReferenceRequest {
        object_id: object.id.clone(),
        collection_id: rand_collection.id.to_string(),
        target_collection_id: rand_collection_2.id.clone(),
        writeable: true,
        auto_update: true,
        sub_path: "".to_string(),
    };

    let _resp = db.create_object_reference(create_ref).unwrap();

    let update_1 = common::functions::update_object(&TCreateUpdate {
        original_object: object,
        collection_id: rand_collection.id.to_string(),
        new_name: "SuperName".to_string(),
        ..Default::default()
    });

    // Update Object again
    let update_2 = common::functions::update_object(&TCreateUpdate {
        original_object: update_1,
        collection_id: rand_collection.id,
        new_name: "File.next.update".to_string(),
        content_len: 123456,
        ..Default::default()
    });

    // Validate update
    assert!(matches!(
        grpc_to_db_object_status(&update_2.status),
        ObjectStatus::AVAILABLE
    ));
    assert_eq!(update_2.rev_number, 2);
    assert_eq!(update_2.filename, "File.next.update".to_string());
    assert_eq!(update_2.content_len, 123456);
    assert!(update_2.auto_update);

    // Get auto_updated object

    let get_obj = GetObjectsRequest {
        collection_id: rand_collection_2.id,
        page_request: None,
        label_id_filter: None,
        with_url: false,
    };

    let resp = db.get_objects(get_obj).unwrap().unwrap();
    let some_object = resp[0].clone();

    assert_eq!(some_object.object.unwrap().id, update_2.id);
}

#[test]
#[ignore]
#[serial(db)]
fn object_revision_test() {
    // Create db connection
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Create random project
    let rand_project = common::functions::create_project(None);

    // Create random collection
    let rand_collection = common::functions::create_collection(TCreateCollection {
        project_id: rand_project.id,
        col_override: None,
        ..Default::default()
    });

    // Create an object
    let object = common::functions::create_object(&TCreateObject {
        collection_id: rand_collection.id.to_string(),
        ..Default::default()
    });

    // Update 1

    let update_1 = common::functions::update_object(&TCreateUpdate {
        original_object: object.clone(),
        collection_id: rand_collection.id.to_string(),
        new_name: "SuperName".to_string(),
        ..Default::default()
    });

    // Update Object again
    let update_2 = common::functions::update_object(&TCreateUpdate {
        original_object: update_1,
        collection_id: rand_collection.id.to_string(),
        new_name: "File.next.update".to_string(),
        content_len: 123456,
        ..Default::default()
    });

    // Validate update
    assert!(matches!(
        grpc_to_db_object_status(&update_2.status),
        ObjectStatus::AVAILABLE
    ));
    assert_eq!(update_2.rev_number, 2);
    assert_eq!(update_2.filename, "File.next.update".to_string());
    assert_eq!(update_2.content_len, 123456);
    assert!(update_2.auto_update);

    // Test Revisions
    // For now this is easier here,
    // but in the future this should be refactored to a separate function

    let get_latest = GetLatestObjectRevisionRequest {
        collection_id: rand_collection.id,
        object_id: object.id,
        with_url: false,
    };

    let latest = db.get_latest_object_revision(get_latest).unwrap();

    // Test if both updates will point to the "latest"
    assert_eq!(latest.object.unwrap().id, update_2.id);
}

#[test]
#[ignore]
#[serial(db)]
fn object_revisions_test() {
    // Create db connection
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Create random project
    let rand_project = common::functions::create_project(None);

    // Create random collection
    let rand_collection = common::functions::create_collection(TCreateCollection {
        project_id: rand_project.id,
        col_override: None,
        ..Default::default()
    });

    // Create an object
    let object = common::functions::create_object(&TCreateObject {
        collection_id: rand_collection.id.to_string(),
        ..Default::default()
    });

    // Update 1

    let update_1 = common::functions::update_object(&TCreateUpdate {
        original_object: object.clone(),
        collection_id: rand_collection.id.to_string(),
        new_name: "SuperName".to_string(),
        ..Default::default()
    });

    // Update Object again
    let update_2 = common::functions::update_object(&TCreateUpdate {
        original_object: update_1,
        collection_id: rand_collection.id.to_string(),
        new_name: "File.next.update".to_string(),
        content_len: 123456,
        ..Default::default()
    });

    // Validate update
    assert!(matches!(
        grpc_to_db_object_status(&update_2.status),
        ObjectStatus::AVAILABLE
    ));
    assert_eq!(update_2.rev_number, 2);
    assert_eq!(update_2.filename, "File.next.update".to_string());
    assert_eq!(update_2.content_len, 123456);
    assert!(update_2.auto_update);

    // Test Revisions
    // Get all revisions

    let get_all_revs = GetObjectRevisionsRequest {
        collection_id: rand_collection.id.to_string(),
        object_id: object.id,
        page_request: None,
        with_url: false,
    };

    let resp_1 = db.get_object_revisions(get_all_revs).unwrap();

    println!("Revisions: {:#?}", resp_1);

    // This should return the same!
    assert!(resp_1.len() == 3);

    let get_all_revs = GetObjectRevisionsRequest {
        collection_id: rand_collection.id,
        object_id: update_2.id,
        page_request: None,
        with_url: false,
    };

    let resp_2 = db.get_object_revisions(get_all_revs).unwrap();

    println!("Revisions: {:#?}", resp_2);
    assert!(resp_2.len() == 3);

    assert!(common::functions::compare_it(
        resp_1.into_iter().map(MyObjectWithUrl).collect::<Vec<_>>(),
        resp_2.into_iter().map(MyObjectWithUrl).collect::<Vec<_>>()
    ))
}

#[test]
#[ignore]
#[serial(db)]
fn update_object_get_references_test() {
    // Create db connection
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Create random project
    let rand_project = common::functions::create_project(None);

    // Create random collection
    let rand_collection = common::functions::create_collection(TCreateCollection {
        project_id: rand_project.id.clone(),
        col_override: None,
        ..Default::default()
    });

    // Create second random collection
    let rand_collection_2 = common::functions::create_collection(TCreateCollection {
        project_id: rand_project.id,
        col_override: None,
        ..Default::default()
    });

    // Create an object
    let object = common::functions::create_object(&TCreateObject {
        collection_id: rand_collection.id.to_string(),
        ..Default::default()
    });
    let object_rev_0_ulid = diesel_ulid::DieselUlid::from_str(&object.id).unwrap();

    // Create auto_updating reference in col 2
    let create_ref = CreateObjectReferenceRequest {
        object_id: object.id.clone(),
        collection_id: rand_collection.id.clone(),
        target_collection_id: rand_collection_2.id.clone(),
        writeable: true,
        auto_update: true,
        sub_path: "".to_string(),
    };

    let _resp = db.create_object_reference(create_ref).unwrap();

    let update_1 = common::functions::update_object(&TCreateUpdate {
        original_object: object.clone(),
        collection_id: rand_collection.id.to_string(),
        new_name: "SuperName".to_string(),
        ..Default::default()
    });

    // Update Object again
    let update_2 = common::functions::update_object(&TCreateUpdate {
        original_object: update_1,
        collection_id: rand_collection.id.to_string(),
        new_name: "File.next.update".to_string(),
        content_len: 123456,
        ..Default::default()
    });
    let object_rev_2_ulid = diesel_ulid::DieselUlid::from_str(&update_2.id).unwrap();

    // Validate update
    assert!(matches!(
        grpc_to_db_object_status(&update_2.status),
        ObjectStatus::AVAILABLE
    ));
    assert_eq!(update_2.rev_number, 2);
    assert_eq!(update_2.filename, "File.next.update".to_string());
    assert_eq!(update_2.content_len, 123456);
    assert!(update_2.auto_update);

    // Get auto_updated object
    let get_obj = GetObjectsRequest {
        collection_id: rand_collection_2.id,
        page_request: None,
        label_id_filter: None,
        with_url: false,
    };

    let resp = db.get_objects(get_obj).unwrap().unwrap();
    let some_object = resp[0].clone();

    assert_eq!(some_object.object.unwrap().id, update_2.id);

    // Get references test
    let get_refs_resp_1 = db.get_references(&object_rev_2_ulid, true).unwrap();

    println!("Refs: {:#?}", get_refs_resp_1.references);
    assert_eq!(get_refs_resp_1.references.len(), 2);

    let get_refs_resp_2 = db.get_references(&object_rev_0_ulid, true).unwrap();

    println!("Refs: {:#?}", get_refs_resp_2.references);
    assert!(get_refs_resp_2.references.len() == 2);
    assert_eq!(get_refs_resp_1, get_refs_resp_2);
}

#[test]
#[ignore]
#[serial(db)]
fn delete_object_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let creator = common::functions::get_admin_user_ulid();
    let endpoint_id = common::functions::get_default_endpoint_ulid();

    // Create random project
    let random_project = create_project(None);

    // Create random collection
    let random_collection = create_collection(TCreateCollection {
        project_id: random_project.id,
        col_override: None,
        ..Default::default()
    });

    // Create a single object
    let single_id = create_object(
        &(TCreateObject {
            creator_id: Some(creator.to_string()),
            collection_id: random_collection.id.to_string(),
            default_endpoint_id: Some(endpoint_id.to_string()),
            num_labels: thread_rng().gen_range(0..4),
            num_hooks: thread_rng().gen_range(0..4),
            ..Default::default()
        }),
    )
    .id;

    // Simple delete single revision object without revision or force
    let delreq = DeleteObjectRequest {
        object_id: single_id.clone(),
        collection_id: random_collection.clone().id,
        with_revisions: false,
        force: false,
    };

    let resp = db.delete_object(delreq, creator);

    assert!(resp.is_ok());

    let raw_db_object = get_object_status_raw(&single_id);

    // Should delete the object
    assert_eq!(raw_db_object.object_status, ObjectStatus::TRASH);

    //---------------------------------------------

    // New single object
    let single_id = create_object(
        &(TCreateObject {
            creator_id: Some(creator.to_string()),
            collection_id: random_collection.id.to_string(),
            default_endpoint_id: Some(endpoint_id.to_string()),
            num_labels: thread_rng().gen_range(0..4),
            num_hooks: thread_rng().gen_range(0..4),
            ..Default::default()
        }),
    )
    .id;

    // Add revision
    let updatereq = UpdateObjectRequest {
        object_id: single_id.clone(),
        collection_id: random_collection.id.to_string(),
        object: Some(StageObject {
            filename: "Update".to_string(),
            sub_path: "".to_string(),
            content_len: 0,
            source: None,
            dataclass: DataClass::Private as i32,
            labels: Vec::new(),
            hooks: Vec::new(),
        }),
        reupload: false,
        is_specification: false,
        preferred_endpoint_id: "".to_string(),
        multi_part: false,
        hash: None, // Note: Maybe has to be refactored for future hash validation
    };

    let new_id = diesel_ulid::DieselUlid::generate();
    let update_response = db
        .update_object(updatereq, &creator, new_id, &endpoint_id)
        .unwrap();

    let staging_finished = db
        .finish_object_staging(
            &FinishObjectStagingRequest {
                object_id: update_response.object_id,
                upload_id: update_response.staging_id,
                collection_id: update_response.collection_id,
                hash: None,
                no_upload: true,
                completed_parts: vec![],
                auto_update: true,
            },
            &creator,
        )
        .unwrap();

    // Simple delete with revisions / with force
    let mut delreq = DeleteObjectRequest {
        object_id: single_id.clone(),
        collection_id: random_collection.id,
        with_revisions: true,
        force: true,
    };

    //println!("\nAbout to delete all revisions with the revision 0 id of an object.");
    let resp = db.delete_object(delreq.clone(), creator);

    // Should error because single_id is "old" revision
    assert!(resp.is_err());

    delreq.object_id = staging_finished.clone().object.unwrap().id;

    //println!("\nAbout to delete all revisions with the revision 0 id of an object.");
    let _resp = db.delete_object(delreq, creator).unwrap();

    // Revision Should also be deleted
    let raw_db_object = get_object_status_raw(&staging_finished.object.unwrap().id);

    // Should delete the object
    assert_eq!(raw_db_object.object_status, ObjectStatus::TRASH);

    let raw_db_object = get_object_status_raw(&single_id);

    // Should delete the object
    assert_eq!(raw_db_object.object_status, ObjectStatus::TRASH);
}

#[test]
#[ignore]
#[serial(db)]
fn delete_object_references_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let creator = common::functions::get_admin_user_ulid();
    let endpoint_id = common::functions::get_default_endpoint_ulid();

    // Create random project
    let random_project = create_project(None);

    // Create random collection
    let source_collection = create_collection(TCreateCollection {
        project_id: random_project.id.to_string(),
        col_override: None,
        ..Default::default()
    });

    // Create random collection 2
    let target_collection = create_collection(TCreateCollection {
        project_id: random_project.id,
        col_override: None,
        ..Default::default()
    });

    let new_obj = create_object(
        &(TCreateObject {
            creator_id: Some(creator.to_string()),
            collection_id: source_collection.id.to_string(),
            default_endpoint_id: Some(endpoint_id.to_string()),
            ..Default::default()
        }),
    );

    db.create_object_reference(CreateObjectReferenceRequest {
        object_id: new_obj.id.to_string(),
        collection_id: source_collection.id.to_string(),
        target_collection_id: target_collection.id.to_string(),
        writeable: true,
        auto_update: true,
        sub_path: "".to_string(),
    })
    .unwrap();

    // Delete target collection reference
    db.delete_object(
        DeleteObjectRequest {
            object_id: new_obj.id.to_string(),
            collection_id: target_collection.id,
            with_revisions: false,
            force: false,
        },
        creator,
    )
    .unwrap();

    let undeleted = get_object(source_collection.id, new_obj.id);

    assert_ne!(undeleted.filename, "DELETED".to_string())
}

#[test]
#[ignore]
#[serial(db)]
fn get_objects_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let creator = common::functions::get_admin_user_ulid();
    let endpoint_id = common::functions::get_default_endpoint_ulid();

    // Create random project
    let random_project = create_project(None);

    // Create random collection
    let random_collection = create_collection(TCreateCollection {
        project_id: random_project.id,
        col_override: None,
        ..Default::default()
    });

    // Create 5 random objects
    let _object_ids = (0..128)
        .map(|_| {
            create_object(
                &(TCreateObject {
                    creator_id: Some(creator.to_string()),
                    collection_id: random_collection.id.to_string(),
                    default_endpoint_id: Some(endpoint_id.to_string()),
                    num_labels: thread_rng().gen_range(0..4),
                    num_hooks: thread_rng().gen_range(0..4),
                    ..Default::default()
                }),
            )
            .id
        })
        .collect::<Vec<_>>();

    // Get all objects
    let get_request = GetObjectsRequest {
        collection_id: random_collection.id,
        page_request: Some(PageRequest {
            last_uuid: "".to_string(),
            page_size: 64,
        }),
        label_id_filter: None,
        with_url: false,
    };

    let get_optional = db.get_objects(get_request).unwrap();
    let get_response = get_optional.unwrap();

    assert_eq!(get_response.len(), 64);
}

#[test]
#[ignore]
#[serial(db)]
fn get_object_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let creator = common::functions::get_admin_user_ulid();
    let endpoint_id = common::functions::get_default_endpoint_ulid();

    // Create random project
    let random_project = create_project(None);

    // Create random collection
    let random_collection = create_collection(TCreateCollection {
        project_id: random_project.id,
        col_override: None,
        ..Default::default()
    });

    let new_obj = create_object(
        &(TCreateObject {
            creator_id: Some(creator.to_string()),
            collection_id: random_collection.id.to_string(),
            default_endpoint_id: Some(endpoint_id.to_string()),
            num_labels: thread_rng().gen_range(0..4),
            num_hooks: thread_rng().gen_range(0..4),
            ..Default::default()
        }),
    )
    .id;

    // Get all objects
    let get_request = GetObjectByIdRequest {
        collection_id: random_collection.id.to_string(),
        object_id: new_obj.to_string(),
        with_url: false,
    };

    let get_obj = db.get_object(&get_request).unwrap();

    assert!(get_obj.object.is_some());
    assert_eq!(get_obj.object.unwrap().id, new_obj);

    let get_obj_internal = db
        .get_object_by_id(
            &diesel_ulid::DieselUlid::from_str(&new_obj).unwrap(),
            &diesel_ulid::DieselUlid::from_str(&random_collection.id).unwrap(),
        )
        .unwrap()
        .object
        .unwrap();

    assert_eq!(get_obj_internal.id, new_obj);
}

// #[test]
// #[ignore]
// #[serial(db)]
// fn get_object_primary_location_test() {
//     let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
//     let creator = common::functions::get_admin_user_ulid();
//     let endpoint_id = common::functions::get_default_endpoint_ulid();

//     // Create random project
//     let random_project = create_project(None);

//     // Create random collection
//     let random_collection = create_collection(TCreateCollection {
//         project_id: random_project.id,
//         col_override: None,
//         ..Default::default()
//     });

//     let new_obj = create_object(
//         &(TCreateObject {
//             creator_id: Some(creator.to_string()),
//             collection_id: random_collection.id.to_string(),
//             default_endpoint_id: Some(endpoint_id.to_string()),
//             num_labels: thread_rng().gen_range(0..4),
//             num_hooks: thread_rng().gen_range(0..4),
//             ..Default::default()
//         }),
//     )
//     .id;

//     let get_obj_loc = db
//         .get_primary_object_location(&diesel_ulid::DieselUlid::from_str(&new_obj).unwrap())
//         .unwrap();

//     assert_eq!(get_obj_loc.bucket, random_collection.id);
//     assert_eq!(get_obj_loc.path, new_obj);
// }

// #[test]
// #[ignore]
// #[serial(db)]
// fn get_object_primary_location_with_endpoint_test() {
//     let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
//     let creator = common::functions::get_admin_user_ulid();
//     let endpoint_id = common::functions::get_default_endpoint_ulid();

//     // Create random project
//     let random_project = create_project(None);

//     // Create random collection
//     let random_collection = create_collection(TCreateCollection {
//         project_id: random_project.id,
//         col_override: None,
//         ..Default::default()
//     });

//     let new_obj = create_object(
//         &(TCreateObject {
//             creator_id: Some(creator.to_string()),
//             collection_id: random_collection.id.to_string(),
//             default_endpoint_id: Some(endpoint_id.to_string()),
//             num_labels: thread_rng().gen_range(0..4),
//             num_hooks: thread_rng().gen_range(0..4),
//             ..Default::default()
//         }),
//     )
//     .id;

//     let get_obj_loc = db
//         .get_primary_object_location_with_endpoint(&diesel_ulid::DieselUlid::from_str(&new_obj).unwrap())
//         .unwrap();

//     assert_eq!(get_obj_loc.0.bucket, random_collection.id);
//     assert_eq!(get_obj_loc.0.path, new_obj);
//     assert_eq!(get_obj_loc.1.name, "demo_endpoint".to_string());
// }

// #[test]
// #[ignore]
// #[serial(db)]
// fn get_object_locations() {
//     let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
//     let creator = common::functions::get_admin_user_ulid();
//     let endpoint_id = common::functions::get_default_endpoint_ulid();

//     // Create random project
//     let random_project = create_project(None);

//     // Create random collection
//     let random_collection = create_collection(TCreateCollection {
//         project_id: random_project.id,
//         col_override: None,
//         ..Default::default()
//     });

//     let new_obj = create_object(
//         &(TCreateObject {
//             creator_id: Some(creator.to_string()),
//             collection_id: random_collection.id.to_string(),
//             default_endpoint_id: Some(endpoint_id.to_string()),
//             num_labels: thread_rng().gen_range(0..4),
//             num_hooks: thread_rng().gen_range(0..4),
//             ..Default::default()
//         }),
//     )
//     .id;

//     let get_obj_locs = db
//         .get_object_locations(&diesel_ulid::DieselUlid::from_str(&new_obj).unwrap())
//         .unwrap();

//     assert_eq!(get_obj_locs.len(), 1);
//     assert_eq!(get_obj_locs[0].bucket, random_collection.id);
//     assert_eq!(get_obj_locs[0].path, new_obj);
// }

#[test]
#[ignore]
#[serial(db)]
fn clone_object_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let creator = common::functions::get_admin_user_ulid();
    let endpoint_id = common::functions::get_default_endpoint_ulid();

    // Create random project
    let random_project = create_project(None);

    // Create random collection
    let random_collection = create_collection(TCreateCollection {
        project_id: random_project.id.to_string(),
        col_override: None,
        ..Default::default()
    });

    // Create random collection 2
    let random_collection2 = create_collection(TCreateCollection {
        project_id: random_project.id,
        col_override: None,
        ..Default::default()
    });

    let new_obj = create_object(
        &(TCreateObject {
            creator_id: Some(creator.to_string()),
            collection_id: random_collection.id.to_string(),
            default_endpoint_id: Some(endpoint_id.to_string()),
            num_labels: thread_rng().gen_range(0..4),
            num_hooks: thread_rng().gen_range(0..4),
            ..Default::default()
        }),
    );

    // Update Object again
    let update_2 = common::functions::update_object(&TCreateUpdate {
        original_object: new_obj.clone(),
        collection_id: random_collection.id.to_string(),
        new_name: "File.next.update2".to_string(),
        content_len: 123456,
        ..Default::default()
    });

    let clone_req = CloneObjectRequest {
        object_id: update_2.id.to_string(),
        collection_id: random_collection.id,
        target_collection_id: random_collection2.id,
    };

    let resp = db.clone_object(&clone_req, &creator).unwrap();

    let cloned = resp.object.unwrap();

    assert_ne!(cloned.id, update_2.id);
    assert_eq!(cloned.rev_number, 0);
    println!("{:#?}", cloned.id);
    println!("{:#?}", cloned.origin.clone().unwrap().id);
    println!("{:#?}", update_2.id);
    println!("{:#?}", new_obj.id);
    assert_eq!(cloned.origin.unwrap().id, update_2.id);
    assert_eq!(cloned.content_len, update_2.content_len);
    assert_eq!(cloned.filename, update_2.filename);
}

#[test]
#[ignore]
#[serial(db)]
fn delete_multiple_objects_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let creator = common::functions::get_admin_user_ulid();
    let endpoint_id = common::functions::get_default_endpoint_ulid();

    // Create random project
    let random_project = create_project(None);

    // Create random collection
    let source_collection = create_collection(TCreateCollection {
        project_id: random_project.id.to_string(),
        ..Default::default()
    });

    // Create random collection 2
    let target_collection = create_collection(TCreateCollection {
        project_id: random_project.id,
        ..Default::default()
    });

    // Create random object 1
    let rnd_obj_1_rev_0 = create_object(
        &(TCreateObject {
            creator_id: Some(creator.to_string()),
            collection_id: source_collection.id.to_string(),
            default_endpoint_id: Some(endpoint_id.to_string()),
            ..Default::default()
        }),
    );

    // Create static read-only reference in collection 2
    let create_ref_01 = CreateObjectReferenceRequest {
        object_id: rnd_obj_1_rev_0.id.clone(),
        collection_id: source_collection.id.clone(),
        target_collection_id: target_collection.id.clone(),
        writeable: false,
        auto_update: false,
        sub_path: "".to_string(),
    };

    db.create_object_reference(create_ref_01).unwrap();

    // Update first object
    let rnd_obj_1_rev_1 = common::functions::update_object(&TCreateUpdate {
        original_object: rnd_obj_1_rev_0.clone(),
        collection_id: source_collection.id.to_string(),
        new_name: "File.next.update2".to_string(),
        content_len: 123456,
        ..Default::default()
    });

    // Create random object 2
    let rnd_obj_2_rev_0 = create_object(
        &(TCreateObject {
            creator_id: Some(creator.to_string()),
            collection_id: source_collection.id.to_string(),
            default_endpoint_id: Some(endpoint_id.to_string()),
            num_labels: thread_rng().gen_range(0..4),
            num_hooks: thread_rng().gen_range(0..4),
            ..Default::default()
        }),
    );

    // Create random object 3
    let rnd_obj_3_rev_0 = create_object(
        &(TCreateObject {
            creator_id: Some(creator.to_string()),
            collection_id: source_collection.id.to_string(),
            default_endpoint_id: Some(endpoint_id.to_string()),
            num_labels: thread_rng().gen_range(0..4),
            num_hooks: thread_rng().gen_range(0..4),
            ..Default::default()
        }),
    );

    // Create auto_updating reference in col 2
    let create_ref_02 = CreateObjectReferenceRequest {
        object_id: rnd_obj_3_rev_0.id.clone(),
        collection_id: source_collection.id.clone(),
        target_collection_id: target_collection.id.clone(),
        writeable: true,
        auto_update: true,
        sub_path: "".to_string(),
    };

    db.create_object_reference(create_ref_02).unwrap();

    // Delete some of the objects with a single call
    let ids = vec![
        rnd_obj_1_rev_1.id.to_string(),
        rnd_obj_2_rev_0.id.to_string(),
        rnd_obj_3_rev_0.id.to_string(),
    ];

    let del_req = DeleteObjectsRequest {
        object_ids: ids,
        collection_id: source_collection.id.to_string(),
        with_revisions: true,
        force: false,
    };

    db.delete_objects(del_req, creator).unwrap();

    // Check random_collection objects
    let get_obj = GetObjectsRequest {
        collection_id: source_collection.id,
        page_request: None,
        label_id_filter: None,
        with_url: false,
    };
    let resp = db.get_objects(get_obj).unwrap().unwrap();

    // - obj_1_rev_0: moved read-only to collection 2
    // - obj_1_rev_1: revision 1 is available read-only in collection 2
    // - obj_2_rev_0: deleted
    // - obj_3_rev_0: moved writeable to random_collection2
    assert_eq!(resp.len(), 0);

    let obj_1_rev_0_check = common::functions::get_raw_db_object_by_id(&rnd_obj_1_rev_0.id);
    let obj_1_rev_1_check = common::functions::get_raw_db_object_by_id(&rnd_obj_1_rev_1.id);
    let obj_2_rev_0_check = common::functions::get_raw_db_object_by_id(&rnd_obj_2_rev_0.id);
    let obj_3_rev_0_check = common::functions::get_raw_db_object_by_id(&rnd_obj_3_rev_0.id);

    assert_eq!(obj_1_rev_0_check.object_status, ObjectStatus::AVAILABLE); // Read-only available in collection2
    assert_eq!(obj_1_rev_1_check.object_status, ObjectStatus::AVAILABLE); // Revision 1 is still read-only in collection2

    assert_eq!(obj_2_rev_0_check.filename, "DELETED".to_string());
    assert_eq!(obj_2_rev_0_check.content_len, 0);
    assert_eq!(obj_2_rev_0_check.object_status, ObjectStatus::TRASH); // Deleted with last reference

    assert_eq!(obj_3_rev_0_check.object_status, ObjectStatus::AVAILABLE); // Writeable available in collection2

    // Check random_collection2 objects
    let get_obj = GetObjectsRequest {
        collection_id: target_collection.id,
        page_request: None,
        label_id_filter: None,
        with_url: false,
    };

    let resp = db.get_objects(get_obj).unwrap().unwrap();

    // - rnd_obj_1_rev_0: read-only
    // - rnd_obj_3_rev_0: writeable
    assert_eq!(resp.len(), 2);
}

#[test]
#[ignore]
#[serial(db)]
fn delete_object_from_versioned_collection_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let creator = common::functions::get_admin_user_ulid();
    let endpoint_id = common::functions::get_default_endpoint_ulid();

    // Create random project
    let random_project = create_project(None);

    // Create random collection
    let random_collection = create_collection(TCreateCollection {
        project_id: random_project.id,
        col_override: None,
        ..Default::default()
    });

    // Create random object in collection
    create_object(
        &(TCreateObject {
            creator_id: Some(creator.to_string()),
            collection_id: random_collection.id.to_string(),
            default_endpoint_id: Some(endpoint_id.to_string()),
            num_labels: thread_rng().gen_range(0..4),
            num_hooks: thread_rng().gen_range(0..4),
            ..Default::default()
        }),
    );

    // Pin collection to version
    let pin_request = PinCollectionVersionRequest {
        collection_id: random_collection.id,
        version: Some(Version {
            major: 1,
            minor: 0,
            patch: 0,
        }),
    };
    let versioned_collection = db
        .pin_collection_version(pin_request, creator)
        .unwrap()
        .0
        .collection
        .unwrap();

    // Get cloned objects from versioned collection
    let get_request = GetObjectsRequest {
        collection_id: versioned_collection.id.to_string(),
        page_request: None,
        label_id_filter: None,
        with_url: false,
    };
    let collection_objects = db.get_objects(get_request).unwrap().unwrap();

    assert!(!collection_objects.is_empty());

    // Try to delete objects from versioned collection
    let delete_request = DeleteObjectRequest {
        object_id: collection_objects
            .first()
            .unwrap()
            .object
            .as_ref()
            .unwrap()
            .id
            .to_string(),
        collection_id: versioned_collection.id,
        with_revisions: false,
        force: true,
    };
    let response = db.delete_object(delete_request, creator);

    assert!(response.is_err()); // Deletion of objects from versioned collections is forbidden.
}
