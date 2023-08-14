use crate::common::{init_db, test_utils};
use aruna_rust_api::api::storage::services::v2::create_collection_request::Parent as CollectionParent;
use aruna_rust_api::api::storage::services::v2::create_dataset_request::Parent as DatasetParent;
use aruna_rust_api::api::storage::services::v2::create_object_request::Parent as ObjectParent;
use aruna_rust_api::api::storage::services::v2::{
    CreateCollectionRequest, CreateDatasetRequest, CreateObjectRequest, CreateProjectRequest,
};
use aruna_server::database::crud::CrudDb;
use aruna_server::database::enums::{DataClass, ObjectStatus, ObjectType};
use aruna_server::middlelayer::create_request_types::CreateRequest;

#[tokio::test]
async fn create_project() {
    // init
    let db_handler = init_db::init_handler().await;

    // create user
    let user = test_utils::new_user(vec![]);
    user.create(&db_handler.database.get_client().await.unwrap())
        .await
        .unwrap();

    // test requests
    let request = CreateRequest::Project(CreateProjectRequest {
        name: "project".to_string(),
        description: "test".to_string(),
        key_values: vec![],
        external_relations: vec![],
        data_class: 1,
    });
    let proj = db_handler.create_resource(request, user.id).await.unwrap();

    assert_eq!(proj.object.created_by, user.id);
    assert_eq!(proj.object.object_type, ObjectType::PROJECT);
    assert_eq!(proj.object.name, "project".to_string());
    assert_eq!(proj.object.object_status, ObjectStatus::AVAILABLE);
    assert_eq!(proj.object.data_class, DataClass::PUBLIC);
    assert_eq!(proj.object.description, "test".to_string());
    assert_eq!(proj.object.revision_number, 0);
    assert_eq!(proj.object.count, 1);
    assert!(proj.object.dynamic);
    assert!(proj.object.hashes.0 .0.is_empty());
    assert!(proj.object.key_values.0 .0.is_empty());
    assert!(proj.object.external_relations.0 .0.is_empty());
    assert!(proj.object.endpoints.0.is_empty());
    assert!(proj.inbound.0.is_empty());
    assert!(proj.inbound_belongs_to.0.is_empty());
    assert!(proj.outbound.0.is_empty());
    assert!(proj.outbound_belongs_to.0.is_empty());
}
#[tokio::test]
async fn create_collection() {
    // init
    let db_handler = init_db::init_handler().await;
    let client = &db_handler.database.get_client().await.unwrap();

    // create user
    let user = test_utils::new_user(vec![]);
    user.create(client).await.unwrap();

    // create parent
    let parent = CreateRequest::Project(CreateProjectRequest {
        name: "project".to_string(),
        description: "test".to_string(),
        key_values: vec![],
        external_relations: vec![],
        data_class: 1,
    });
    let parent = db_handler.create_resource(parent, user.id).await.unwrap();

    // test requests
    let request = CreateRequest::Collection(CreateCollectionRequest {
        name: "collection".to_string(),
        description: "test".to_string(),
        key_values: vec![],
        external_relations: vec![],
        data_class: 1,
        parent: Some(CollectionParent::ProjectId(parent.object.id.to_string())),
    });
    let coll = db_handler.create_resource(request, user.id).await.unwrap();

    assert_eq!(coll.object.created_by, user.id);
    assert_eq!(coll.object.object_type, ObjectType::COLLECTION);
    assert_eq!(coll.object.name, "collection".to_string());
    assert_eq!(coll.object.object_status, ObjectStatus::AVAILABLE);
    assert_eq!(coll.object.data_class, DataClass::PUBLIC);
    assert_eq!(coll.object.description, "test".to_string());
    assert_eq!(coll.object.revision_number, 0);
    assert_eq!(coll.object.count, 1);
    assert!(coll.object.dynamic);
    assert!(coll.object.hashes.0 .0.is_empty());
    assert!(coll.object.key_values.0 .0.is_empty());
    assert!(coll.object.external_relations.0 .0.is_empty());
    assert!(coll.object.endpoints.0.is_empty());
    assert!(coll.inbound.0.is_empty());
    assert!(coll.inbound_belongs_to.0.get(&parent.object.id).is_some());
    assert!(coll.outbound.0.is_empty());
    assert!(coll.outbound_belongs_to.0.is_empty());
}
#[tokio::test]
async fn create_dataset() {
    // init
    let db_handler = init_db::init_handler().await;
    let client = &db_handler.database.get_client().await.unwrap();

    // create user
    let user = test_utils::new_user(vec![]);
    user.create(client).await.unwrap();
    // create parent
    let parent = CreateRequest::Project(CreateProjectRequest {
        name: "project".to_string(),
        description: "test".to_string(),
        key_values: vec![],
        external_relations: vec![],
        data_class: 1,
    });
    let parent = db_handler.create_resource(parent, user.id).await.unwrap();

    // test requests
    let request = CreateRequest::Dataset(CreateDatasetRequest {
        name: "dataset".to_string(),
        description: "test".to_string(),
        key_values: vec![],
        external_relations: vec![],
        data_class: 1,
        parent: Some(DatasetParent::ProjectId(parent.object.id.to_string())),
    });
    let ds = db_handler.create_resource(request, user.id).await.unwrap();

    assert_eq!(ds.object.created_by, user.id);
    assert_eq!(ds.object.object_type, ObjectType::DATASET);
    assert_eq!(ds.object.name, "dataset".to_string());
    assert_eq!(ds.object.object_status, ObjectStatus::AVAILABLE);
    assert_eq!(ds.object.data_class, DataClass::PUBLIC);
    assert_eq!(ds.object.description, "test".to_string());
    assert_eq!(ds.object.revision_number, 0);
    assert_eq!(ds.object.count, 1);
    assert!(ds.object.dynamic);
    assert!(ds.object.hashes.0 .0.is_empty());
    assert!(ds.object.key_values.0 .0.is_empty());
    assert!(ds.object.external_relations.0 .0.is_empty());
    assert!(ds.object.endpoints.0.is_empty());
    assert!(ds.inbound.0.is_empty());
    assert!(ds.inbound_belongs_to.0.get(&parent.object.id).is_some());
    assert!(ds.outbound.0.is_empty());
    assert!(ds.outbound_belongs_to.0.is_empty());
}
#[tokio::test]
async fn create_object() {
    // init
    let db_handler = init_db::init_handler().await;
    let client = &db_handler.database.get_client().await.unwrap();

    // create user
    let user = test_utils::new_user(vec![]);
    user.create(client).await.unwrap();
    // create parent
    let parent = CreateRequest::Project(CreateProjectRequest {
        name: "project".to_string(),
        description: "test".to_string(),
        key_values: vec![],
        external_relations: vec![],
        data_class: 1,
    });
    let parent = db_handler.create_resource(parent, user.id).await.unwrap();

    // test requests
    let request = CreateRequest::Object(CreateObjectRequest {
        name: "object".to_string(),
        description: "test".to_string(),
        key_values: vec![],
        external_relations: vec![],
        data_class: 1,
        hashes: vec![],
        parent: Some(ObjectParent::ProjectId(parent.object.id.to_string())),
    });
    let obj = db_handler.create_resource(request, user.id).await.unwrap();

    assert_eq!(obj.object.created_by, user.id);
    assert_eq!(obj.object.object_type, ObjectType::OBJECT);
    assert_eq!(obj.object.name, "object".to_string());
    assert_eq!(obj.object.object_status, ObjectStatus::INITIALIZING);
    assert_eq!(obj.object.data_class, DataClass::PUBLIC);
    assert_eq!(obj.object.description, "test".to_string());
    assert_eq!(obj.object.revision_number, 0);
    assert_eq!(obj.object.count, 1);
    assert!(!obj.object.dynamic);
    assert!(obj.object.hashes.0 .0.is_empty());
    assert!(obj.object.key_values.0 .0.is_empty());
    assert!(obj.object.external_relations.0 .0.is_empty());
    assert!(obj.object.endpoints.0.is_empty());
    assert!(obj.inbound.0.is_empty());
    assert!(obj.inbound_belongs_to.0.get(&parent.object.id).is_some());
    assert!(obj.outbound.0.is_empty());
    assert!(obj.outbound_belongs_to.0.is_empty());
}
