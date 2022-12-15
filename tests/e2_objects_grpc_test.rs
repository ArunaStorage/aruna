use std::sync::Arc;

use aruna_rust_api::api::storage::models::v1::{DataClass, Hash, Hashalgorithm, Permission};
use aruna_rust_api::api::storage::services::v1::object_service_server::ObjectService;
use aruna_rust_api::api::storage::services::v1::{
    collection_service_server::CollectionService, CreateNewCollectionRequest,
    GetDownloadUrlRequest, GetUploadUrlRequest, InitializeNewObjectResponse,
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

use crate::common::functions::TCreateCollection;

mod common;

#[ignore]
#[tokio::test]
#[serial(db)]
async fn create_objects_grpc_test() {
    // Init db connection
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone()).await);

    // Read config relative to binary
    let config = ArunaServerConfig::new();

    // Initialize instance default data proxy endpoint
    let default_endpoint = db
        .clone()
        .init_default_endpoint(config.config.default_endpoint)
        .unwrap();

    // Init object service
    let object_service = ObjectServiceImpl::new(db, authz, default_endpoint).await;

    // Create random project
    let random_project = common::functions::create_project(None);

    // Fast track user adding with None permission
    let user_id = common::grpc_helpers::get_token_user_id(common::oidc::REGULARTOKEN).await;
    let add_perm = common::grpc_helpers::add_project_permission(
        random_project.id.as_str(),
        user_id.as_str(),
        common::oidc::ADMINTOKEN,
    )
    .await;
    assert_eq!(add_perm.permission, Permission::None as i32);

    // Create random collection
    let random_collection = common::functions::create_collection(TCreateCollection {
        project_id: random_project.id.to_string(),
        num_labels: 0,
        num_hooks: 0,
        col_override: Some(CreateNewCollectionRequest {
            name: "create_objects_grpc_test() Collection".to_string(),
            description: "Lorem Ipsum Dolor".to_string(),
            project_id: random_project.id.to_string(),
            labels: vec![],
            hooks: vec![],
            label_ontology: None,
            dataclass: DataClass::Private as i32,
        }),
        creator_id: Some(user_id.to_string()),
    });

    // Init object in non-existing collection --> Error
    let init_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(InitializeNewObjectRequest {
            object: Some(StageObject {
                filename: "file.test".to_string(),
                description: "Test file with dummy data.".to_string(),
                collection_id: "".to_string(),
                content_len: 1234,
                source: None,
                dataclass: DataClass::Private as i32,
                labels: vec![],
                hooks: vec![],
            }),
            collection_id: "".to_string(),
            preferred_endpoint_id: "".to_string(),
            multipart: false,
            is_specification: false,
        }),
        common::oidc::ADMINTOKEN,
    );
    let init_object_response = object_service
        .initialize_new_object(init_object_request)
        .await;

    assert!(init_object_response.is_err());

    // Init object without staging object --> Error
    let init_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(InitializeNewObjectRequest {
            object: None,
            collection_id: random_collection.id.to_string(),
            preferred_endpoint_id: "".to_string(),
            multipart: false,
            is_specification: false,
        }),
        common::oidc::ADMINTOKEN,
    );
    let init_object_response = object_service
        .initialize_new_object(init_object_request)
        .await;

    assert!(init_object_response.is_err());

    // Init object correctly with different permissions
    for permission in vec![
        Permission::None,
        Permission::Read,
        Permission::Append,
        Permission::Modify,
        Permission::Admin,
    ]
    .iter()
    {
        // Fast track permission edit
        let edit_perm = common::grpc_helpers::edit_project_permission(
            random_project.id.as_str(),
            user_id.as_str(),
            permission,
            common::oidc::ADMINTOKEN,
        )
        .await;
        assert_eq!(edit_perm.permission, *permission as i32);

        // Init object
        let init_object_request = common::grpc_helpers::add_token(
            tonic::Request::new(InitializeNewObjectRequest {
                object: Some(StageObject {
                    filename: "test.file".to_string(),
                    description: format!("Object created with {:#?} permission.", permission)
                        .to_string(),
                    collection_id: random_collection.id.to_string(),
                    content_len: 123456,
                    source: None,
                    dataclass: DataClass::Private as i32,
                    labels: vec![],
                    hooks: vec![],
                }),
                collection_id: random_collection.id.to_string(),
                preferred_endpoint_id: "".to_string(),
                multipart: false,
                is_specification: false,
            }),
            common::oidc::REGULARTOKEN,
        );

        let init_object_response = object_service
            .initialize_new_object(init_object_request)
            .await;

        match *permission {
            Permission::None | Permission::Read => {
                // Request should fail with insufficient permissions
                assert!(init_object_response.is_err());
            }
            Permission::Append | Permission::Modify | Permission::Admin => {
                assert!(init_object_response.is_ok());

                // Destructure response
                let InitializeNewObjectResponse {
                    object_id,
                    upload_id,
                    collection_id,
                } = init_object_response.unwrap().into_inner();

                let get_upload_url_request = common::grpc_helpers::add_token(
                    tonic::Request::new(GetUploadUrlRequest {
                        object_id: object_id.to_string(),
                        upload_id: upload_id.to_string(),
                        collection_id: collection_id.to_string(),
                        multipart: false,
                        part_number: 1,
                    }),
                    common::oidc::REGULARTOKEN,
                );
                let upload_url = object_service
                    .get_upload_url(get_upload_url_request)
                    .await
                    .unwrap()
                    .into_inner()
                    .url
                    .unwrap()
                    .url;

                assert!(!upload_url.is_empty());
                assert!(url::Url::parse(&upload_url).is_ok());

                /* Normally people would upload data at this point but local data proxy upload is scuffed
                // Define file with test data
                let file_path = Path::new("path/to/upload_test.data");
                let file = tokio::fs::File::open(file_path).await.unwrap();
                let client = reqwest::Client::new();
                let stream = FramedRead::new(file, BytesCodec::new());
                let body = Body::wrap_stream(stream);

                // Send the request to the upload url
                let response = client.put(upload_url).body(body).send().await.unwrap();
                assert!(response.status().is_success());
                */

                // Finish object
                let finish_object_request = common::grpc_helpers::add_token(
                    tonic::Request::new(FinishObjectStagingRequest {
                        object_id: object_id.to_string(),
                        upload_id: upload_id.to_string(),
                        collection_id: collection_id.to_string(),
                        hash: Some(Hash {
                            alg: Hashalgorithm::Sha256 as i32,
                            hash:
                                "4ec2d656985e3d823b81cc2cd9b56ec27ab1303cfebaf5f95c37d2fe1661a779"
                                    .to_string(),
                        }),
                        no_upload: false,
                        completed_parts: vec![],
                        auto_update: true,
                    }),
                    common::oidc::REGULARTOKEN,
                );
                let finish_object_response = object_service
                    .finish_object_staging(finish_object_request)
                    .await;
                assert!(finish_object_response.is_ok());

                let finished_object = finish_object_response.unwrap().into_inner().object.unwrap();

                // Validate object object (+ data)
                let get_download_url_request = common::grpc_helpers::add_token(
                    tonic::Request::new(GetDownloadUrlRequest {
                        collection_id: collection_id.to_string(),
                        object_id: finished_object.id.to_string(),
                    }),
                    common::oidc::REGULARTOKEN,
                );
                let download_url = object_service
                    .get_download_url(get_download_url_request)
                    .await
                    .unwrap()
                    .into_inner()
                    .url
                    .unwrap()
                    .url;

                assert!(!download_url.is_empty());
                assert!(url::Url::parse(&download_url).is_ok());

                let response = reqwest::get(download_url).await.unwrap();
                assert!(response.status().is_success());
            }
            _ => panic!("Unspecified permission is not allowed."),
        };
    }
}

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
