use aruna_rust_api::api::storage::internal::v1::Location;
use aruna_rust_api::api::storage::models::v1::{EndpointType, Hash, Hashalgorithm, KeyValue};
use aruna_rust_api::api::storage::services::v1::{
    CreateNewCollectionRequest, CreateProjectRequest, FinishObjectStagingRequest,
    InitializeNewObjectRequest, StageObject,
};
use aruna_server::database;
use aruna_server::database::crud::utils::grpc_to_db_object_status;
use aruna_server::database::models::enums::ObjectStatus;
use serial_test::serial;

#[test]
#[ignore]
#[serial(db)]
fn create_object_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let creator = uuid::Uuid::parse_str("12345678-1234-1234-1234-111111111111").unwrap();
    let endpoint_id = uuid::Uuid::parse_str("12345678-6666-6666-6666-999999999999").unwrap();

    // Create Project
    let create_project_request = CreateProjectRequest {
        name: "Object creation test project".to_string(),
        description: "Test project used in object creation test.".to_string(),
    };

    let create_project_response = db.create_project(create_project_request, creator).unwrap();
    let project_id = uuid::Uuid::parse_str(&create_project_response.project_id).unwrap();

    assert!(!project_id.is_nil());

    // Create Collection
    let create_collection_request = CreateNewCollectionRequest {
        name: "Object creation test project collection".to_string(),
        description: "Test collection used in object creation test.".to_string(),
        project_id: project_id.to_string(),
        labels: vec![],
        hooks: vec![],
        dataclass: 1,
    };
    let create_collection_response = db
        .create_new_collection(create_collection_request, creator)
        .unwrap();
    let collection_id = uuid::Uuid::parse_str(&create_collection_response.collection_id).unwrap();

    // Create Object
    let new_object_id = uuid::Uuid::new_v4();
    let upload_id = uuid::Uuid::new_v4().to_string();

    let location = Location {
        r#type: EndpointType::S3 as i32,
        bucket: collection_id.to_string(),
        path: new_object_id.to_string(),
    };

    let init_object_request = InitializeNewObjectRequest {
        object: Some(StageObject {
            filename: "File.file".to_string(),
            description: "This is a mock file.".to_string(),
            collection_id: collection_id.to_string(),
            content_len: 1234,
            source: None,
            dataclass: 1,
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
    };

    let init_object_response = db
        .create_object(
            &init_object_request,
            &creator,
            &location,
            upload_id.clone(),
            endpoint_id,
            new_object_id,
        )
        .unwrap();

    assert_eq!(&init_object_response.object_id, &new_object_id.to_string());
    assert_eq!(
        &init_object_response.collection_id,
        &collection_id.to_string()
    );
    assert_eq!(&init_object_response.upload_id, &upload_id);

    // Finish object staging
    let finish_hash = Hash {
        alg: Hashalgorithm::Sha256 as i32,
        hash: "f60b102aa455f085df91ffff53b3c0acd45c10f02782b953759ab10973707a92".to_string(),
    };
    let finish_request = FinishObjectStagingRequest {
        object_id: new_object_id.to_string(),
        upload_id,
        collection_id: collection_id.to_string(),
        hash: Some(finish_hash.clone()),
        no_upload: false,
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
    assert_eq!(finished_object.rev_number, 0);
    assert_eq!(finished_object.filename, "File.file".to_string());
    assert_eq!(finished_object.content_len, 1234);
    assert_eq!(finished_object.hash.unwrap(), finish_hash);
    assert!(finished_object.auto_update);
}
