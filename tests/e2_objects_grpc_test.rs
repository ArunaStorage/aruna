use std::sync::Arc;

use aruna_rust_api::api::storage::models::v1::{
    DataClass, Hash, Hashalgorithm, KeyValue, Permission,
};
use aruna_rust_api::api::storage::services::v1::object_service_server::ObjectService;
use aruna_rust_api::api::storage::services::v1::{
    collection_service_server::CollectionService, CreateNewCollectionRequest,
    CreateObjectReferenceRequest, GetDownloadUrlRequest, GetLatestObjectRevisionRequest,
    GetObjectByIdRequest, GetReferencesRequest, GetUploadUrlRequest, InitializeNewObjectResponse,
};
use aruna_rust_api::api::storage::services::v1::{
    FinishObjectStagingRequest, GetObjectsRequest, InitializeNewObjectRequest, StageObject,
    UpdateObjectRequest,
};
use aruna_server::database::crud::utils::db_to_grpc_object_status;
use aruna_server::database::models::enums::{ObjectStatus, ReferenceStatus};
use aruna_server::{
    config::ArunaServerConfig,
    database::{self},
    server::services::authz::Authz,
    server::services::collection::CollectionServiceImpl,
    server::services::object::ObjectServiceImpl,
};
use serial_test::serial;

use crate::common::functions::{TCreateCollection, TCreateObject};
use crate::common::grpc_helpers::get_token_user_id;

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
                collection_id: "".to_string(), // Collection Id in StageObject is deprecated
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
                    collection_id: "".to_string(), // Collection Id in StageObject is deprecated
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
                collection_id: "".to_string(), // Collection Id in StageObject is deprecated
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
                collection_id: "".to_string(), // Collection Id in StageObject is deprecated
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

/// The individual steps of this test function contains:
/// 1) Initializing an object
/// 2) Updating the revision 0 staging object
/// 3) Finishing the revision 0 staging object
/// 4) Starting the update on the revision 0 object
/// 5) Updating the revision 1 staging object
/// 6) Finishing the revision 1 staging object
#[ignore]
#[tokio::test]
#[serial(db)]
async fn update_staging_object_grpc_test() {
    // Init database connection
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone()).await);

    // Read test config relative to binary
    let config = ArunaServerConfig::new();

    // Initialize instance default data proxy endpoint
    let default_endpoint = db
        .clone()
        .init_default_endpoint(config.config.default_endpoint)
        .unwrap();

    // Init object service
    let object_service = ObjectServiceImpl::new(db.clone(), authz, default_endpoint).await;

    // Fast track project creation
    let project_id = common::functions::create_project(None).id;

    // Fast track collection creation
    let collection_meta = TCreateCollection {
        project_id: project_id.clone(),
        num_labels: 0,
        num_hooks: 0,
        col_override: None,
        creator_id: None,
    };
    let random_collection = common::functions::create_collection(collection_meta);

    // Initialize object
    let init_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(InitializeNewObjectRequest {
            object: Some(StageObject {
                filename: "original.object".to_string(),
                description: "".to_string(),   // No use.
                collection_id: "".to_string(), // Collection Id in StageObject is deprecated
                content_len: 0,
                source: None,
                dataclass: DataClass::Private as i32,
                labels: vec![],
                hooks: vec![],
            }),
            collection_id: random_collection.id.clone(),
            preferred_endpoint_id: "".to_string(),
            multipart: false,
            is_specification: false,
        }),
        common::oidc::ADMINTOKEN,
    );

    let init_object_response = object_service
        .initialize_new_object(init_object_request)
        .await
        .unwrap()
        .into_inner();

    // Validate object creation
    let get_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(GetObjectByIdRequest {
            collection_id: init_object_response.collection_id.clone(),
            object_id: init_object_response.object_id.clone(),
            with_url: false,
        }),
        common::oidc::ADMINTOKEN,
    );
    let get_object_response = object_service
        .get_object_by_id(get_object_request)
        .await
        .unwrap()
        .into_inner();

    let rev_0_staging_object = get_object_response.object.unwrap().object.unwrap();
    assert_eq!(init_object_response.object_id, rev_0_staging_object.id);

    // Update stage object
    let update_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(UpdateObjectRequest {
            object_id: init_object_response.object_id.clone(),
            collection_id: init_object_response.collection_id.clone(),
            object: Some(StageObject {
                filename: "updated.object".to_string(),
                description: "".to_string(),   // No use.
                collection_id: "".to_string(), // Collection Id in StageObject is deprecated
                content_len: 1234,
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

    let update_object_response = object_service
        .update_object(update_object_request)
        .await
        .unwrap()
        .into_inner();

    // Validate object was updated in-place --> No new object id
    assert_eq!(
        init_object_response.object_id,
        update_object_response.object_id.clone()
    );

    // Validate object was updated in-place
    let get_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(GetObjectByIdRequest {
            collection_id: update_object_response.collection_id.clone(),
            object_id: update_object_response.object_id.clone(),
            with_url: false,
        }),
        common::oidc::ADMINTOKEN,
    );

    let get_object_response = object_service
        .get_object_by_id(get_object_request)
        .await
        .unwrap()
        .into_inner();

    let rev_0_staging_object_updated = get_object_response.object.unwrap().object.unwrap();
    let rev_0_staging_object_updated_ref_status = db
        .clone()
        .get_reference_status(
            &uuid::Uuid::parse_str(&rev_0_staging_object_updated.id).unwrap(),
            &uuid::Uuid::parse_str(&random_collection.id).unwrap(),
        )
        .unwrap();

    // Object uuid should be the same
    assert_eq!(rev_0_staging_object.id, rev_0_staging_object_updated.id);

    // Object status is still INITIALIZING
    assert_eq!(
        rev_0_staging_object.status,
        db_to_grpc_object_status(ObjectStatus::INITIALIZING) as i32
    );
    assert_eq!(
        rev_0_staging_object_updated.status,
        db_to_grpc_object_status(ObjectStatus::INITIALIZING) as i32
    );

    // Reference status is still STAGING
    assert_eq!(
        rev_0_staging_object_updated_ref_status,
        ReferenceStatus::STAGING
    );

    // Filename should differ as it was updated
    assert_ne!(
        rev_0_staging_object.filename,
        rev_0_staging_object_updated.filename
    );
    assert_eq!(rev_0_staging_object.filename, "original.object".to_string());
    assert_eq!(
        rev_0_staging_object_updated.filename,
        "updated.object".to_string()
    );

    // Content length should differ as it was updated
    assert_ne!(
        rev_0_staging_object.content_len,
        rev_0_staging_object_updated.content_len
    );
    assert_eq!(rev_0_staging_object.content_len, 0);
    assert_eq!(rev_0_staging_object_updated.content_len, 1234);

    // Creation timestamp should differ as it was updated
    assert_ne!(
        rev_0_staging_object.created,
        rev_0_staging_object_updated.created
    );

    // Origin should be equal as the same user created/updated
    assert_eq!(
        rev_0_staging_object.origin.unwrap(),
        rev_0_staging_object_updated.origin.unwrap()
    );

    // Revision number should not be increased
    assert_eq!(
        rev_0_staging_object.rev_number,
        rev_0_staging_object_updated.rev_number
    );
    assert_eq!(rev_0_staging_object_updated.rev_number, 0);
    assert_eq!(rev_0_staging_object.rev_number, 0);

    // Hash should be the same as it can only be updated at finish
    assert_eq!(rev_0_staging_object.hash, rev_0_staging_object_updated.hash);

    // Labels/Hooks should be the same as they were not updated
    assert_eq!(
        rev_0_staging_object.labels,
        rev_0_staging_object_updated.labels
    );
    assert_eq!(
        rev_0_staging_object.hooks,
        rev_0_staging_object_updated.hooks
    );

    // Still latest
    assert!(rev_0_staging_object.latest);
    assert!(rev_0_staging_object_updated.latest);

    // Finish object
    let finish_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(FinishObjectStagingRequest {
            object_id: update_object_response.object_id.clone(),
            upload_id: update_object_response.staging_id.clone(),
            collection_id: update_object_response.collection_id.clone(),
            hash: Some(Hash {
                alg: Hashalgorithm::Sha256 as i32,
                hash: "".to_string(), // No upload ¯\_(ツ)_/¯
            }),
            no_upload: true,
            completed_parts: vec![],
            auto_update: true,
        }),
        common::oidc::ADMINTOKEN,
    );
    let finish_object_response = object_service
        .finish_object_staging(finish_object_request)
        .await
        .unwrap()
        .into_inner();

    let rev_0_object = finish_object_response.object.unwrap();

    // Update object --> Add label and increase revision count
    let update_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(UpdateObjectRequest {
            object_id: rev_0_object.id.to_string(),
            collection_id: random_collection.id.to_string(),
            object: Some(StageObject {
                filename: "updated.object".to_string(),
                description: "".to_string(),   // No use.
                collection_id: "".to_string(), // Collection Id in StageObject is deprecated
                content_len: 1234,
                source: None,
                dataclass: DataClass::Private as i32,
                labels: vec![KeyValue {
                    key: "NewKey".to_string(),
                    value: "NewValue".to_string(),
                }],
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
    let update_object_response = object_service
        .update_object(update_object_request)
        .await
        .unwrap()
        .into_inner();

    let finish_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(FinishObjectStagingRequest {
            object_id: update_object_response.object_id.to_string(),
            upload_id: update_object_response.staging_id.to_string(),
            collection_id: update_object_response.collection_id.to_string(),
            hash: Some(Hash {
                alg: Hashalgorithm::Sha256 as i32,
                hash: "".to_string(), // No upload ¯\_(ツ)_/¯
            }),
            no_upload: true,
            completed_parts: vec![],
            auto_update: true,
        }),
        common::oidc::ADMINTOKEN,
    );
    let finish_object_response = object_service
        .finish_object_staging(finish_object_request)
        .await
        .unwrap()
        .into_inner();

    let rev_1_object = finish_object_response.object.unwrap();

    assert_ne!(rev_1_object.id, rev_0_object.id);
    assert_eq!(rev_1_object.filename, "updated.object".to_string());
    assert_eq!(rev_1_object.rev_number, 1);
    assert_eq!(
        rev_1_object.labels,
        vec![KeyValue {
            key: "NewKey".to_string(),
            value: "NewValue".to_string()
        }]
    );

    // Start another update process to update description
    let update_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(UpdateObjectRequest {
            object_id: rev_1_object.id.to_string(),
            collection_id: random_collection.id.to_string(),
            object: Some(StageObject {
                filename: "updated.object".to_string(),
                description: "New description of object".to_string(),
                collection_id: "".to_string(), // Collection Id in StageObject is deprecated
                content_len: 1234,
                source: None,
                dataclass: DataClass::Private as i32,
                labels: vec![KeyValue {
                    key: "NewKey".to_string(),
                    value: "NewValue".to_string(),
                }],
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
    let update_object_response = object_service
        .update_object(update_object_request)
        .await
        .unwrap()
        .into_inner();

    let get_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(GetObjectByIdRequest {
            collection_id: update_object_response.collection_id.clone(),
            object_id: update_object_response.object_id.clone(),
            with_url: false,
        }),
        common::oidc::ADMINTOKEN,
    );

    let get_object_response = object_service
        .get_object_by_id(get_object_request)
        .await
        .unwrap()
        .into_inner();

    let rev_2_staging_object = get_object_response.object.unwrap().object.unwrap();

    assert_ne!(rev_1_object.id, rev_2_staging_object.id);
    assert_eq!(rev_2_staging_object.filename, "updated.object".to_string());
    assert_eq!(rev_2_staging_object.rev_number, 2);

    // Update revision 2 staging object
    //  - Description was not the best idea, so remove it
    //  - Instead change existing label to description
    let update_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(UpdateObjectRequest {
            object_id: rev_2_staging_object.id.to_string(),
            collection_id: random_collection.id.to_string(),
            object: Some(StageObject {
                filename: "updated.object".to_string(),
                description: "".to_string(),   // No use.
                collection_id: "".to_string(), // Collection Id in StageObject is deprecated
                content_len: 1234,
                source: None,
                dataclass: DataClass::Private as i32,
                labels: vec![KeyValue {
                    key: "description".to_string(),
                    value: "My object description".to_string(),
                }],
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
    let update_object_response = object_service
        .update_object(update_object_request)
        .await
        .unwrap()
        .into_inner();

    let get_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(GetObjectByIdRequest {
            collection_id: update_object_response.collection_id.clone(),
            object_id: update_object_response.object_id.clone(),
            with_url: false,
        }),
        common::oidc::ADMINTOKEN,
    );
    let get_object_response = object_service
        .get_object_by_id(get_object_request)
        .await
        .unwrap()
        .into_inner();

    let rev_2_staging_object_updated = get_object_response.object.unwrap().object.unwrap();

    assert_eq!(rev_2_staging_object.id, rev_2_staging_object_updated.id);
    assert_eq!(
        rev_2_staging_object.filename,
        rev_2_staging_object_updated.filename
    );
    assert_eq!(rev_2_staging_object.rev_number, 2);
    assert_eq!(
        rev_2_staging_object_updated.labels,
        vec![KeyValue {
            key: "description".to_string(),
            value: "My object description".to_string(),
        }]
    );
}

/// The individual steps of this test function contains:
/// 1) Creating an object
/// 2) Creating revision 1 of object
/// 3) Try to update revision 0 without force
/// 4) Try to update revision 0 with force
/// 5) Validate revision 2 object
#[ignore]
#[tokio::test]
#[serial(db)]
async fn update_outdated_revision_grpc_test() {
    // Init database connection
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone()).await);

    // Read test config relative to binary
    let config = ArunaServerConfig::new();

    // Initialize instance default data proxy endpoint
    let default_endpoint = db
        .clone()
        .init_default_endpoint(config.config.default_endpoint)
        .unwrap();

    // Init object service
    let object_service = ObjectServiceImpl::new(db.clone(), authz, default_endpoint).await;

    // Fast track project creation
    let project_id = common::functions::create_project(None).id;

    // Fast track collection creation
    let collection_meta = TCreateCollection {
        project_id: project_id.clone(),
        num_labels: 0,
        num_hooks: 0,
        col_override: None,
        creator_id: None,
    };
    let random_collection = common::functions::create_collection(collection_meta);

    // Fast track object creation
    let rev_0_object = common::functions::create_object(&TCreateObject {
        creator_id: None,
        collection_id: random_collection.id.to_string(),
        default_endpoint_id: None,
        num_labels: 0,
        num_hooks: 0,
    });

    // Create revision 1 of object
    let update_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(UpdateObjectRequest {
            object_id: rev_0_object.id.to_string(),
            collection_id: random_collection.id.to_string(),
            object: Some(StageObject {
                filename: "file.name".to_string(),
                description: "".to_string(),   // No use.
                collection_id: "".to_string(), // Collection Id in StageObject is deprecated
                content_len: 123456,
                source: None,
                dataclass: DataClass::Private as i32,
                labels: vec![KeyValue {
                    key: "description".to_string(),
                    value: "Created in update_outdated_revision_grpc_test()".to_string(),
                }],
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
    let update_object_response = object_service
        .update_object(update_object_request)
        .await
        .unwrap()
        .into_inner();

    let finish_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(FinishObjectStagingRequest {
            object_id: update_object_response.object_id.to_string(),
            upload_id: update_object_response.staging_id.to_string(),
            collection_id: update_object_response.collection_id.to_string(),
            hash: Some(Hash {
                alg: Hashalgorithm::Sha256 as i32,
                hash: "".to_string(), // No upload ¯\_(ツ)_/¯
            }),
            no_upload: true,
            completed_parts: vec![],
            auto_update: true,
        }),
        common::oidc::ADMINTOKEN,
    );
    let finish_object_response = object_service
        .finish_object_staging(finish_object_request)
        .await
        .unwrap()
        .into_inner();

    let rev_1_object = finish_object_response.object.unwrap();

    assert_ne!(rev_0_object.id, rev_1_object.id);
    assert_ne!(rev_0_object.filename, rev_1_object.filename);
    assert_ne!(rev_0_object.content_len, rev_1_object.content_len);
    assert_ne!(rev_0_object.labels, rev_1_object.labels);
    assert_eq!(
        rev_1_object.labels,
        vec![KeyValue {
            key: "description".to_string(),
            value: "Created in update_outdated_revision_grpc_test()".to_string(),
        }]
    );
    assert_eq!(rev_1_object.rev_number, 1);

    // Try to update old revision without force --> Error
    let mut inner_update_request = UpdateObjectRequest {
        object_id: rev_0_object.id.to_string(),
        collection_id: random_collection.id.to_string(),
        object: Some(StageObject {
            filename: "file.name".to_string(),
            description: "".to_string(),   // No use.
            collection_id: "".to_string(), // Collection Id in StageObject is deprecated
            content_len: 123456,
            source: None,
            dataclass: DataClass::Private as i32,
            labels: vec![KeyValue {
                key: "description".to_string(),
                value: "Created in update_outdated_revision_grpc_test()".to_string(),
            }],
            hooks: vec![KeyValue {
                key: "my_hook".to_string(),
                value: "service-url".to_string(),
            }],
        }),
        reupload: false,
        preferred_endpoint_id: "".to_string(),
        multi_part: false,
        is_specification: false,
        force: false,
    };
    let update_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(inner_update_request.clone()),
        common::oidc::ADMINTOKEN,
    );
    let update_object_response = object_service.update_object(update_object_request).await;

    assert!(update_object_response.is_err()); // Fails because it is not the latest revision

    // Use the same request with force
    inner_update_request.force = true;

    let update_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(inner_update_request.clone()),
        common::oidc::ADMINTOKEN,
    );
    let update_object_response = object_service
        .update_object(update_object_request)
        .await
        .unwrap()
        .into_inner();

    let finish_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(FinishObjectStagingRequest {
            object_id: update_object_response.object_id.to_string(),
            upload_id: update_object_response.staging_id.to_string(),
            collection_id: update_object_response.collection_id.to_string(),
            hash: Some(Hash {
                alg: Hashalgorithm::Sha256 as i32,
                hash: "".to_string(), // No upload ¯\_(ツ)_/¯
            }),
            no_upload: true,
            completed_parts: vec![],
            auto_update: true,
        }),
        common::oidc::ADMINTOKEN,
    );
    let finish_object_response = object_service
        .finish_object_staging(finish_object_request)
        .await
        .unwrap()
        .into_inner();

    let rev_2_object = finish_object_response.object.unwrap();

    assert_ne!(rev_1_object.id, rev_2_object.id);
    assert_ne!(rev_1_object.hooks, rev_2_object.hooks);
    assert_eq!(
        rev_2_object.hooks,
        vec![KeyValue {
            key: "my_hook".to_string(),
            value: "service-url".to_string(),
        }]
    );
    assert_eq!(rev_2_object.rev_number, 2);
}

/// The individual steps of this test function contains:
/// 1. Creating an object
/// 2. Start first update process on revision 0 object
/// 3. Force second update on revision 0 object
/// 4. Finish first update on revision 0 object
/// 5. Validate consistent object state
#[ignore]
#[tokio::test]
#[serial(db)]
async fn concurrent_update_grpc_test() {
    // Init database connection
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone()).await);

    // Read test config relative to binary
    let config = ArunaServerConfig::new();

    // Initialize instance default data proxy endpoint
    let default_endpoint = db
        .clone()
        .init_default_endpoint(config.config.default_endpoint)
        .unwrap();

    // Init object service
    let object_service = ObjectServiceImpl::new(db.clone(), authz, default_endpoint).await;

    // Fast track project creation
    let project_id = common::functions::create_project(None).id;

    // Fast track collection creation
    let collection_meta = TCreateCollection {
        project_id: project_id.clone(),
        num_labels: 0,
        num_hooks: 0,
        col_override: None,
        creator_id: None,
    };
    let random_collection = common::functions::create_collection(collection_meta);

    // Fast track object creation
    let rev_0_object = common::functions::create_object(&TCreateObject {
        creator_id: None,
        collection_id: random_collection.id.to_string(),
        default_endpoint_id: None,
        num_labels: 0,
        num_hooks: 0,
    });

    assert_eq!(rev_0_object.rev_number, 0);

    // Start normal update process on revision 0 object
    let outdated_update_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(UpdateObjectRequest {
            object_id: rev_0_object.id.to_string(),
            collection_id: random_collection.id.to_string(),
            object: Some(StageObject {
                filename: "file.name".to_string(),
                description: "".to_string(),   // No use.
                collection_id: "".to_string(), // Collection Id in StageObject is deprecated
                content_len: 123456,
                source: None,
                dataclass: DataClass::Private as i32,
                labels: vec![KeyValue {
                    key: "description".to_string(),
                    value: "Normal update in concurrent_update_grpc_test()".to_string(),
                }],
                hooks: vec![KeyValue {
                    key: "my-hook".to_string(),
                    value: "service-url".to_string(),
                }],
            }),
            reupload: false,
            preferred_endpoint_id: "".to_string(),
            multi_part: false,
            is_specification: false,
            force: false,
        }),
        common::oidc::ADMINTOKEN,
    );
    let outdated_update_object_response = object_service
        .update_object(outdated_update_object_request)
        .await
        .unwrap()
        .into_inner();

    // Force update on revision 0 object
    let force_update_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(UpdateObjectRequest {
            object_id: rev_0_object.id.to_string(),
            collection_id: random_collection.id.to_string(),
            object: Some(StageObject {
                filename: "force.update".to_string(),
                description: "".to_string(),   // No use.
                collection_id: "".to_string(), // Collection Id in StageObject is deprecated
                content_len: 654321,
                source: None,
                dataclass: DataClass::Private as i32,
                labels: vec![KeyValue {
                    key: "description".to_string(),
                    value: "Forced update in concurrent_update_grpc_test()".to_string(),
                }],
                hooks: vec![KeyValue {
                    key: "validation-hook".to_string(),
                    value: "service-url".to_string(),
                }],
            }),
            reupload: false,
            preferred_endpoint_id: "".to_string(),
            multi_part: false,
            is_specification: false,
            force: true,
        }),
        common::oidc::ADMINTOKEN,
    );
    let force_update_object_response = object_service
        .update_object(force_update_object_request)
        .await
        .unwrap()
        .into_inner();

    let finish_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(FinishObjectStagingRequest {
            object_id: force_update_object_response.object_id.to_string(),
            upload_id: force_update_object_response.staging_id.to_string(),
            collection_id: force_update_object_response.collection_id.to_string(),
            hash: Some(Hash {
                alg: Hashalgorithm::Sha256 as i32,
                hash: "".to_string(), // No upload ¯\_(ツ)_/¯
            }),
            no_upload: true,
            completed_parts: vec![],
            auto_update: true,
        }),
        common::oidc::ADMINTOKEN,
    );
    let finish_object_response = object_service
        .finish_object_staging(finish_object_request)
        .await
        .unwrap()
        .into_inner();

    let rev_2_object = finish_object_response.object.unwrap();

    assert_eq!(rev_2_object.rev_number, 2); // Should be revision 2 as the started update already blocks revision 1

    // Try to finish update on revision 1 object with auto_update == true
    let finish_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(FinishObjectStagingRequest {
            object_id: outdated_update_object_response.object_id.to_string(),
            upload_id: outdated_update_object_response.staging_id.to_string(),
            collection_id: outdated_update_object_response.collection_id.to_string(),
            hash: Some(Hash {
                alg: Hashalgorithm::Sha256 as i32,
                hash: "".to_string(), // No upload ¯\_(ツ)_/¯
            }),
            no_upload: true,
            completed_parts: vec![],
            auto_update: true,
        }),
        common::oidc::ADMINTOKEN,
    );
    let finish_object_response = object_service
        .finish_object_staging(finish_object_request)
        .await
        .unwrap()
        .into_inner();

    let rev_1_object = finish_object_response.object.unwrap(); // Still revision 1 but revision 2 already exists.

    assert_eq!(rev_1_object.rev_number, 1);

    // Get latest object revision and validate that the forced update
    // is still recognized as the latest revision
    let get_latest_revision_request = common::grpc_helpers::add_token(
        tonic::Request::new(GetLatestObjectRevisionRequest {
            collection_id: random_collection.id.to_string(),
            object_id: rev_0_object.id.to_string(),
        }),
        common::oidc::ADMINTOKEN,
    );
    let get_latest_revision_response = object_service
        .get_latest_object_revision(get_latest_revision_request)
        .await
        .unwrap()
        .into_inner();

    let latest_object = get_latest_revision_response.object.unwrap();

    // Validate that latest object revision is forced concurrent update
    assert_eq!(latest_object.id, rev_2_object.id);
    assert_eq!(latest_object.filename, rev_2_object.filename);
    assert_eq!(latest_object.content_len, rev_2_object.content_len);
    assert_eq!(latest_object.labels, rev_2_object.labels);
    assert_eq!(latest_object.hooks, rev_2_object.hooks);
    assert_eq!(latest_object.rev_number, rev_2_object.rev_number);
}
