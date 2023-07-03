use std::str::FromStr;

use aruna_rust_api::api::storage::services::v1::CreateObjectGroupRequest;
use aruna_server::database::{self, models::enums::Resources};
use serial_test::serial;

use crate::common::functions::{TCreateCollection, TCreateObject};

mod common;

#[test]
#[ignore]
#[serial(db)]
fn create_stream_group_test() {
    // Init database connection
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Create random project ulid
    let project_ulid = diesel_ulid::DieselUlid::generate();

    // Create random stream group ulid and subject
    let stream_group_ulid = diesel_ulid::DieselUlid::generate();
    let stream_group_subject = format!("UPDATES.STORAGE._.{}._", project_ulid);

    // Create stream group in database
    let stream_group = db
        .create_notification_stream_group(
            stream_group_ulid,
            stream_group_subject.to_string(),
            project_ulid,
            Resources::PROJECT,
            false,
        )
        .unwrap();

    assert_eq!(stream_group.id, stream_group_ulid);
    assert_eq!(stream_group.subject, stream_group_subject);

    // Try to insert stream group with same ulid --> Error
    let result = db.create_notification_stream_group(
        stream_group_ulid,
        "".to_string(),
        project_ulid,
        Resources::PROJECT,
        false,
    );

    assert!(result.is_err()); // ULID clash
}

#[ignore]
#[tokio::test]
#[serial(db)]
async fn get_stream_group_test() {
    // Init database connection
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Create random project/collection ulid (no need for actual creation in database)
    let project_ulid = diesel_ulid::DieselUlid::generate();
    let collection_ulid = diesel_ulid::DieselUlid::generate();

    // Create random stream group ulid and subject
    let stream_group_ulid = diesel_ulid::DieselUlid::generate();
    let stream_group_subject =
        format!("UPDATES.STORAGE._.{}._.{}._", project_ulid, collection_ulid);

    // Create stream group in database
    let create_stream_group = db
        .create_notification_stream_group(
            stream_group_ulid,
            stream_group_subject.to_string(),
            collection_ulid,
            Resources::COLLECTION,
            false,
        )
        .unwrap();

    assert_eq!(create_stream_group.id, stream_group_ulid);
    assert_eq!(create_stream_group.subject, stream_group_subject);

    // Try to get stream group with random ulid --> Error
    let result = db.get_notification_stream_group(diesel_ulid::DieselUlid::generate());

    assert!(result.is_err()); // Stream group does not exist.

    // Try to get stream group with existing ulid
    let get_stream_group = db.get_notification_stream_group(stream_group_ulid).unwrap();

    assert_eq!(get_stream_group.id, create_stream_group.id);
    assert_eq!(get_stream_group.subject, create_stream_group.subject);
    assert_eq!(
        get_stream_group.resource_id,
        create_stream_group.resource_id
    );
    assert_eq!(
        get_stream_group.resource_type,
        create_stream_group.resource_type
    );
    assert_eq!(
        get_stream_group.notify_on_sub_resources,
        create_stream_group.notify_on_sub_resources
    );
}

#[ignore]
#[tokio::test]
#[serial(db)]
async fn delete_stream_group_test() {
    // Init database connection
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Create random project/collection ulid (no need for actual creation in database)
    let project_ulid = diesel_ulid::DieselUlid::generate();
    let collection_ulid = diesel_ulid::DieselUlid::generate();

    // Create random stream group ulid and subject
    let stream_group_ulid = diesel_ulid::DieselUlid::generate();
    let stream_group_subject =
        format!("UPDATES.STORAGE._.{}._.{}._", project_ulid, collection_ulid);

    // Create stream group in database
    let create_stream_group = db
        .create_notification_stream_group(
            stream_group_ulid,
            stream_group_subject.to_string(),
            collection_ulid,
            Resources::COLLECTION,
            false,
        )
        .unwrap();

    assert_eq!(create_stream_group.id, stream_group_ulid);
    assert_eq!(create_stream_group.subject, stream_group_subject);

    // Try to delete stream group with random ulid --> Error
    let result = db.delete_notification_stream_group(diesel_ulid::DieselUlid::generate());

    assert!(result.is_ok()); // Stream group does not exist but result is ok as 0 rows where deleted.

    // Try to get stream group with existing ulid
    db.delete_notification_stream_group(stream_group_ulid)
        .unwrap();

    // Try to get delete stream group
    let result = db.get_notification_stream_group(stream_group_ulid);

    assert!(result.is_err()); // Stream group does not exist.
}

#[ignore]
#[tokio::test]
#[serial(db)]
async fn get_shared_revision_test() {
    // Init database connection
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let creator = common::functions::get_admin_user_ulid();
    let endpoint_id = common::functions::get_default_endpoint_ulid();

    // Create random project
    let random_project = common::functions::create_project(None);
    let project_ulid = diesel_ulid::DieselUlid::from_str(&random_project.id).unwrap();

    // Create random collection
    let random_collection = common::functions::create_collection(TCreateCollection {
        project_id: random_project.id,
        col_override: None,
        ..Default::default()
    });
    let collection_ulid = diesel_ulid::DieselUlid::from_str(&random_collection.id).unwrap();

    // Create random object
    let proto_object = common::functions::create_object(
        &(TCreateObject {
            creator_id: Some(creator.to_string()),
            collection_id: random_collection.id.to_string(),
            default_endpoint_id: Some(endpoint_id.to_string()),
            ..Default::default()
        }),
    );

    // Fetch raw db object
    let object = common::functions::get_raw_db_object_by_id(&proto_object.id);

    // Create dummy object group with single object
    let object_group_name = "Dummy_Object_Group";
    let object_group_description = "Created within get_shared_revision_test.";
    let proto_object_group = db
        .create_object_group(
            &CreateObjectGroupRequest {
                name: object_group_name.to_string(),
                description: object_group_description.to_string(),
                collection_id: random_collection.id,
                object_ids: vec![proto_object.id],
                meta_object_ids: vec![],
                labels: vec![],
                hooks: vec![],
            },
            &creator,
        )
        .unwrap()
        .object_group
        .unwrap();

    // Fetch raw db object group
    let object_group = common::functions::get_raw_db_object_group_by_id(&proto_object_group.id);

    // Try to get shared revision of project ulid --> Error
    let result = db.get_resource_shared_revision(project_ulid, Resources::PROJECT);
    assert!(result.is_err());

    // Try to get shared revision of collection ulid --> Error
    let result = db.get_resource_shared_revision(collection_ulid, Resources::COLLECTION);
    assert!(result.is_err());

    // Try to get shared revision of random ulid --> Error
    let result =
        db.get_resource_shared_revision(diesel_ulid::DieselUlid::generate(), Resources::OBJECT);
    assert!(result.is_err());

    // Get shared revision of object
    let object_shared_revision = db
        .get_resource_shared_revision(object.id, Resources::OBJECT)
        .unwrap();
    assert_eq!(object_shared_revision, object.shared_revision_id);

    // Get shared revision of object
    let object_group_shared_revision = db
        .get_resource_shared_revision(object_group.id, Resources::OBJECTGROUP)
        .unwrap();
    assert_eq!(
        object_group_shared_revision,
        object_group.shared_revision_id
    );
}
