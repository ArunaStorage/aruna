use std::sync::Arc;

use aruna_rust_api::api::storage::models::v1::DataClass;
use aruna_rust_api::api::storage::services::v1::object_service_server::ObjectService;
use aruna_rust_api::api::storage::services::v1::{
    collection_service_server::CollectionService, CreateNewCollectionRequest,
};
use aruna_rust_api::api::storage::services::v1::{
    FinishObjectStagingRequest, GetObjectsRequest, InitializeNewObjectRequest, StageObject,
    UpdateObjectRequest,
};
use aruna_server::{
    config::ArunaServerConfig,
    database::{self},
    server::services::authz::Authz,
    server::services::collection::CollectionServiceImpl,
    server::services::object::ObjectServiceImpl,
};
use serial_test::serial;
mod common;

#[ignore]
#[tokio::test]
#[serial(db)]
async fn get_objects_grpc_test() {
    // Init services
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone()).await);

    let collection_service = CollectionServiceImpl::new(db.clone(), authz.clone()).await;

    // Read config relative to binary
    let config = ArunaServerConfig::new();

    // Initialize instance default data proxy endpoint
    let default_endpoint = db
        .clone()
        .init_default_endpoint(config.config.default_endpoint)
        .unwrap();

    let object_service = ObjectServiceImpl::new(db, authz, default_endpoint).await;

    // Create new collection

    let create_req = common::grpc_helpers::add_token(
        tonic::Request::new(CreateNewCollectionRequest {
            name: "TestCollection".to_string(),
            description: "Test collection".to_string(),
            project_id: "12345678-1111-1111-1111-111111111111".to_string(),
            labels: vec![],
            hooks: vec![],
            label_ontology: None,
            dataclass: 0,
        }),
        common::oidc::ADMINTOKEN,
    );

    let create_collection_response = collection_service
        .create_new_collection(create_req)
        .await
        .unwrap()
        .into_inner();
    let collection_id = create_collection_response.collection_id;

    // Initialize new object

    let create_obj = common::grpc_helpers::add_token(
        tonic::Request::new(InitializeNewObjectRequest {
            object: Some(StageObject {
                filename: "TestObject".to_string(),
                description: "Test object".to_string(),
                collection_id: collection_id.clone(),
                content_len: 0,
                source: None,
                dataclass: DataClass::Private as i32,
                labels: vec![],
                hooks: vec![],
            }),
            collection_id: collection_id.clone(),
            preferred_endpoint_id: "".to_string(),
            multipart: false,
            is_specification: false,
        }),
        common::oidc::ADMINTOKEN,
    );

    let initialize_new_object_response = object_service
        .initialize_new_object(create_obj)
        .await
        .unwrap()
        .into_inner();

    // Finish the object

    let finish_obj = common::grpc_helpers::add_token(
        tonic::Request::new(FinishObjectStagingRequest {
            object_id: initialize_new_object_response.object_id.clone(),
            upload_id: initialize_new_object_response.upload_id.clone(),
            collection_id: collection_id.clone(),
            hash: None,
            no_upload: true,
            completed_parts: vec![],
            auto_update: false,
        }),
        common::oidc::ADMINTOKEN,
    );

    let _finish_object_staging_response = object_service
        .finish_object_staging(finish_obj)
        .await
        .unwrap()
        .into_inner();

    // Update the object

    let update_obj = common::grpc_helpers::add_token(
        tonic::Request::new(UpdateObjectRequest {
            object_id: initialize_new_object_response.object_id.clone(),
            collection_id: collection_id.clone(),
            object: Some(StageObject {
                filename: "UpdatedTestObject".to_string(),
                description: "Updated test object".to_string(),
                collection_id: collection_id.clone(),
                content_len: 0,
                source: None,
                dataclass: DataClass::Private as i32,
                labels: vec![],
                hooks: vec![],
            }),
            reupload: false,
            preferred_endpoint_id: "".to_string(),
            multi_part: false,
            is_specification: false,
            force: false,
        }),
        common::oidc::ADMINTOKEN,
    );

    let _update_object_response = object_service
        .update_object(update_obj)
        .await
        .unwrap()
        .into_inner();

    // Get objects of collection
    // This fails with internal error "Record not found"

    let get_objs = common::grpc_helpers::add_token(
        tonic::Request::new(GetObjectsRequest {
            collection_id: collection_id.clone(),
            page_request: None,
            label_id_filter: None,
            with_url: false,
        }),
        common::oidc::ADMINTOKEN,
    );

    let _get_objects_response = object_service
        .get_objects(get_objs)
        .await
        .unwrap()
        .into_inner();
}
