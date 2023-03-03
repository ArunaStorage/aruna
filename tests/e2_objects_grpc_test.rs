use rand::Rng;
use std::sync::Arc;
use std::{thread, time};

use aruna_rust_api::api::storage::models::v1::{
    DataClass, Hash, Hashalgorithm, KeyValue, Permission,
};
use aruna_rust_api::api::storage::services::v1::object_service_server::ObjectService;
use aruna_rust_api::api::storage::services::v1::{
    collection_service_server::CollectionService, AddLabelsToObjectRequest, CloneObjectRequest,
    CreateNewCollectionRequest, CreateObjectReferenceRequest, DeleteObjectRequest,
    DeleteObjectsRequest, GetDownloadUrlRequest, GetLatestObjectRevisionRequest,
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

use crate::common::functions::{
    get_object_status_raw, TCreateCollection, TCreateObject, TCreateUpdate,
};
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
    let authz = Arc::new(Authz::new(db.clone(), ArunaServerConfig::default()).await);

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
        creator_id: Some(user_id.to_string()),
        ..Default::default()
    });

    // Init object in non-existing collection --> Error
    let init_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(InitializeNewObjectRequest {
            object: Some(StageObject {
                filename: "file.test".to_string(),
                sub_path: "".to_string(),
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
            hash: None,
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
            hash: None,
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
                    filename: format!("created_with_{:?}.file", permission).to_string(),
                    sub_path: "".to_string(),
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
                hash: None,
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
    let authz = Arc::new(Authz::new(db.clone(), ArunaServerConfig::default()).await);

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
                sub_path: "".to_string(),
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
            hash: None,
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
                sub_path: "".to_string(),
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
            hash: None,
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
    let authz = Arc::new(Authz::new(db.clone(), ArunaServerConfig::default()).await);

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
                sub_path: "".to_string(),
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
            hash: None,
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
                sub_path: "".to_string(),
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
            hash: None,
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
    /* Note: Commented out as in GitHub Actions the timestamps evaluate as equal
    assert_ne!(
        rev_0_staging_object.created,
        rev_0_staging_object_updated.created
    );
    */

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
    // except internal ...

    'outer: for old_label in rev_0_staging_object.labels {
        for new_label in rev_0_staging_object_updated.labels.clone() {
            if old_label == new_label {
                continue 'outer;
            } else if old_label.key == *"app.aruna-storage.org/new_path"
                && new_label.key == *"app.aruna-storage.org/new_path"
            {
                continue 'outer;
            }
        }
        panic!("No corresponding label found for old: {:#?}", old_label)
    }

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
                sub_path: "".to_string(),
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
            hash: None,
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
                sub_path: "".to_string(),
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
            hash: None,
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
                sub_path: "".to_string(),
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
            hash: None,
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
    let authz = Arc::new(Authz::new(db.clone(), ArunaServerConfig::default()).await);

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
        collection_id: random_collection.id.to_string(),
        num_labels: 0,
        num_hooks: 0,
        ..Default::default()
    });

    // Create revision 1 of object
    let update_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(UpdateObjectRequest {
            object_id: rev_0_object.id.to_string(),
            collection_id: random_collection.id.to_string(),
            object: Some(StageObject {
                filename: "file.name".to_string(),
                sub_path: "".to_string(),
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
            hash: None,
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
            sub_path: "".to_string(),
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
        hash: None,
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
    let authz = Arc::new(Authz::new(db.clone(), ArunaServerConfig::default()).await);

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
        collection_id: random_collection.id.to_string(),
        num_labels: 0,
        num_hooks: 0,
        ..Default::default()
    });

    assert_eq!(rev_0_object.rev_number, 0);

    // Start normal update process on revision 0 object
    let outdated_update_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(UpdateObjectRequest {
            object_id: rev_0_object.id.to_string(),
            collection_id: random_collection.id.to_string(),
            object: Some(StageObject {
                filename: "file.name".to_string(),
                sub_path: "".to_string(),
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
            hash: None,
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
                sub_path: "".to_string(),
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
            hash: None,
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
            with_url: false,
        }),
        common::oidc::ADMINTOKEN,
    );
    let get_latest_revision_response = object_service
        .get_latest_object_revision(get_latest_revision_request)
        .await
        .unwrap()
        .into_inner();

    let latest_object = get_latest_revision_response.object.unwrap().object.unwrap();

    // Validate that latest object revision is forced concurrent update
    assert_eq!(latest_object.id, rev_2_object.id);
    assert_eq!(latest_object.filename, rev_2_object.filename);
    assert_eq!(latest_object.content_len, rev_2_object.content_len);
    assert_eq!(latest_object.labels, rev_2_object.labels);
    assert_eq!(latest_object.hooks, rev_2_object.hooks);
    assert_eq!(latest_object.rev_number, rev_2_object.rev_number);
}

/// The individual steps of this test function contains:
/// 1. Create a random object
/// 2. Create static read-only reference of revision 0 in another collection
/// 3. Create static writeable reference of revision 0 in another collection
/// 4. Create auto_update read_only reference in another collection
/// 5. Create auto_update writeable reference in another collection
/// 6. Update object in source collection and validate references
/// 7. Update object in writeable auto_update collection and validate references
/// 8. Try create a reference duplicate in a collection
/// 9. Try update writeable reference on object revision 0
/// 10. Try update read-only references
#[ignore]
#[tokio::test]
#[serial(db)]
async fn object_references_grpc_test() {
    // Init database connection
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone(), ArunaServerConfig::default()).await);

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
    let user_id = get_token_user_id(common::oidc::REGULARTOKEN).await;
    let collection_meta = TCreateCollection {
        project_id: project_id.clone(),
        num_labels: 0,
        num_hooks: 0,
        col_override: None,
        creator_id: Some(user_id.to_string()),
    };
    let source_collection = common::functions::create_collection(collection_meta.clone());
    // Collection with read-only reference on static revision
    let ro_st_collection = common::functions::create_collection(collection_meta.clone());
    // Collection with read-only auto_update reference
    let wr_st_collection = common::functions::create_collection(collection_meta.clone());
    // Collection with writeable reference on static revision
    let ro_au_collection = common::functions::create_collection(collection_meta.clone());
    // Collection with writeable auto_update reference
    let wr_au_collection = common::functions::create_collection(collection_meta);

    // Fast track object creation
    let object_meta = TCreateObject {
        creator_id: Some(user_id),
        collection_id: source_collection.id.to_string(),
        num_labels: 0,
        num_hooks: 0,
        ..Default::default()
    };
    let rev_0_object = common::functions::create_object(&object_meta);

    // Create static read-only revision reference in target collection
    let read_only_revision_reference_request = common::grpc_helpers::add_token(
        tonic::Request::new(CreateObjectReferenceRequest {
            object_id: rev_0_object.id.to_string(),
            collection_id: source_collection.id.to_string(),
            target_collection_id: ro_st_collection.id.to_string(),
            writeable: false,
            auto_update: false,
            sub_path: "".to_string(),
        }),
        common::oidc::ADMINTOKEN,
    );
    let read_only_revision_reference_response = object_service
        .create_object_reference(read_only_revision_reference_request)
        .await;

    assert!(read_only_revision_reference_response.is_ok());

    // Create static writeable revision reference in target collection
    let writeable_revision_reference_request = common::grpc_helpers::add_token(
        tonic::Request::new(CreateObjectReferenceRequest {
            object_id: rev_0_object.id.to_string(),
            collection_id: source_collection.id.to_string(),
            target_collection_id: wr_st_collection.id.to_string(),
            writeable: true,
            auto_update: false,
            sub_path: "".to_string(),
        }),
        common::oidc::ADMINTOKEN,
    );
    let writeable_revision_reference_response = object_service
        .create_object_reference(writeable_revision_reference_request)
        .await;

    assert!(writeable_revision_reference_response.is_ok());

    // Create read-only auto_update reference in target collection
    let read_only_revision_reference_request = common::grpc_helpers::add_token(
        tonic::Request::new(CreateObjectReferenceRequest {
            object_id: rev_0_object.id.to_string(),
            collection_id: source_collection.id.to_string(),
            target_collection_id: ro_au_collection.id.to_string(),
            writeable: false,
            auto_update: true,
            sub_path: "".to_string(),
        }),
        common::oidc::ADMINTOKEN,
    );
    let read_only_revision_reference_response = object_service
        .create_object_reference(read_only_revision_reference_request)
        .await;

    assert!(read_only_revision_reference_response.is_ok());

    // Create writeable auto_update reference in target collection
    let read_only_revision_reference_request = common::grpc_helpers::add_token(
        tonic::Request::new(CreateObjectReferenceRequest {
            object_id: rev_0_object.id.to_string(),
            collection_id: source_collection.id.to_string(),
            target_collection_id: wr_au_collection.id.to_string(),
            writeable: true,
            auto_update: true,
            sub_path: "".to_string(),
        }),
        common::oidc::ADMINTOKEN,
    );
    let read_only_revision_reference_response = object_service
        .create_object_reference(read_only_revision_reference_request)
        .await;

    assert!(read_only_revision_reference_response.is_ok());

    // Validate state of references
    let all_references_request = common::grpc_helpers::add_token(
        tonic::Request::new(GetReferencesRequest {
            collection_id: source_collection.id.to_string(),
            object_id: rev_0_object.id.to_string(),
            with_revisions: true,
        }),
        common::oidc::ADMINTOKEN,
    );
    let all_references = object_service
        .get_references(all_references_request)
        .await
        .unwrap()
        .into_inner()
        .references;
    assert_eq!(all_references.len(), 5);

    for reference in all_references {
        assert_eq!(reference.object_id, rev_0_object.id);
        assert_eq!(reference.revision_number, 0);

        if reference.collection_id == ro_st_collection.id
            || reference.collection_id == ro_au_collection.id
        {
            assert!(!reference.is_writeable);
        } else if reference.collection_id == source_collection.id
            || reference.collection_id == wr_st_collection.id
            || reference.collection_id == wr_au_collection.id
        {
            assert!(reference.is_writeable);
        } else {
            panic!(
                "Reference in collection {} should not exist.",
                reference.collection_id
            )
        }
    }

    // Update object in source collection
    let update_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(UpdateObjectRequest {
            object_id: rev_0_object.id.to_string(),
            collection_id: source_collection.id.to_string(),
            object: Some(StageObject {
                filename: "reference.update.01".to_string(),
                sub_path: "".to_string(),
                content_len: 111222,
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
            hash: None,
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

    // Validate state of references
    let all_references_request = common::grpc_helpers::add_token(
        tonic::Request::new(GetReferencesRequest {
            collection_id: source_collection.id.to_string(),
            object_id: rev_0_object.id.to_string(),
            with_revisions: true,
        }),
        common::oidc::ADMINTOKEN,
    );
    let all_references = object_service
        .get_references(all_references_request)
        .await
        .unwrap()
        .into_inner()
        .references;
    assert_eq!(all_references.len(), 5);

    for reference in all_references {
        if reference.collection_id == ro_st_collection.id {
            assert_eq!(reference.object_id, rev_0_object.id);
            assert_eq!(reference.revision_number, 0);
            assert!(!reference.is_writeable);
        } else if reference.collection_id == ro_au_collection.id {
            assert_eq!(reference.object_id, rev_1_object.id);
            assert_eq!(reference.revision_number, 1);
            assert!(!reference.is_writeable);
        } else if reference.collection_id == wr_st_collection.id {
            assert_eq!(reference.object_id, rev_0_object.id);
            assert_eq!(reference.revision_number, 0);
            assert!(reference.is_writeable);
        } else if reference.collection_id == source_collection.id
            || reference.collection_id == wr_au_collection.id
        {
            assert_eq!(reference.object_id, rev_1_object.id);
            assert_eq!(reference.revision_number, 1);
            assert!(reference.is_writeable);
        } else {
            panic!(
                "Reference in collection {} should not exist.",
                reference.collection_id
            )
        }
    }

    // Update object in writeable auto_update collection --> Revision 2
    let update_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(UpdateObjectRequest {
            object_id: rev_1_object.id.to_string(),
            collection_id: wr_au_collection.id.to_string(),
            object: Some(StageObject {
                filename: "reference.update.02".to_string(),
                sub_path: "".to_string(),
                content_len: 222111,
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
            hash: None,
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

    let rev_2_object = finish_object_response.object.unwrap();

    // Validate state of references
    let all_references_request = common::grpc_helpers::add_token(
        tonic::Request::new(GetReferencesRequest {
            collection_id: source_collection.id.to_string(),
            object_id: rev_0_object.id.to_string(),
            with_revisions: true,
        }),
        common::oidc::ADMINTOKEN,
    );
    let all_references = object_service
        .get_references(all_references_request)
        .await
        .unwrap()
        .into_inner()
        .references;
    assert_eq!(all_references.len(), 5);

    for reference in all_references {
        if reference.collection_id == ro_st_collection.id {
            assert_eq!(reference.object_id, rev_0_object.id);
            assert_eq!(reference.revision_number, 0);
            assert!(!reference.is_writeable);
        } else if reference.collection_id == ro_au_collection.id {
            assert_eq!(reference.object_id, rev_2_object.id);
            assert_eq!(reference.revision_number, 2);
            assert!(!reference.is_writeable);
        } else if reference.collection_id == wr_st_collection.id {
            assert_eq!(reference.object_id, rev_0_object.id);
            assert_eq!(reference.revision_number, 0);
            assert!(reference.is_writeable);
        } else if reference.collection_id == source_collection.id
            || reference.collection_id == wr_au_collection.id
        {
            assert_eq!(reference.object_id, rev_2_object.id);
            assert_eq!(reference.revision_number, 2);
            assert!(reference.is_writeable);
        } else {
            panic!(
                "Reference in collection {} should not exist.",
                reference.collection_id
            )
        }
    }

    // Try to create duplicate reference in collection --> Error (object duplicate in collection)
    let duplicate_reference_request = common::grpc_helpers::add_token(
        tonic::Request::new(CreateObjectReferenceRequest {
            object_id: rev_0_object.id.to_string(),
            collection_id: source_collection.id.to_string(),
            target_collection_id: ro_st_collection.id.to_string(),
            writeable: false,
            auto_update: false,
            sub_path: "".to_string(),
        }),
        common::oidc::ADMINTOKEN,
    );
    let duplicate_reference_response = object_service
        .create_object_reference(duplicate_reference_request)
        .await;

    assert!(duplicate_reference_response.is_err());

    // Try update object in writeable static reference collection --> Error (outdated revision)
    let update_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(UpdateObjectRequest {
            object_id: rev_0_object.id.to_string(),
            collection_id: wr_st_collection.id.to_string(),
            object: Some(StageObject {
                filename: "outdated.reference.update".to_string(),
                sub_path: "".to_string(),
                content_len: 121212,
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
            hash: None,
        }),
        common::oidc::ADMINTOKEN,
    );
    let update_object_response = object_service.update_object(update_object_request).await;

    assert!(update_object_response.is_err());

    // Try update object in read-only auto-update collection --> Error (read-only reference)
    let update_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(UpdateObjectRequest {
            object_id: rev_2_object.id.to_string(),
            collection_id: ro_au_collection.id.to_string(),
            object: Some(StageObject {
                filename: "read-only.reference.update".to_string(),
                sub_path: "".to_string(),
                content_len: 212121,
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
            hash: None,
        }),
        common::oidc::ADMINTOKEN,
    );
    let update_object_response = object_service.update_object(update_object_request).await;

    assert!(update_object_response.is_err());
}

/// The individual steps of this test function contains:
/// 1. Create a random object
/// 2. Create a writeable reference in another collection
/// 3. Create a read-only reference in another collection
/// 4. Add labels with different permissions to object
/// 5. Add labels through writeable reference to object
/// 6. Try add labels through read-only reference to object
/// 7. Update object and try to add labels to outdated revision
#[ignore]
#[tokio::test]
#[serial(db)]
async fn add_labels_to_object_grpc_test() {
    // Init database connection
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone(), ArunaServerConfig::default()).await);

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

    // Fast track adding user to project
    let user_id = get_token_user_id(common::oidc::REGULARTOKEN).await;
    let add_perm = common::grpc_helpers::add_project_permission(
        project_id.as_str(),
        user_id.as_str(),
        common::oidc::ADMINTOKEN,
    )
    .await;
    assert_eq!(add_perm.permission, Permission::None as i32);

    // Fast track collection creation
    let collection_meta = TCreateCollection {
        project_id: project_id.clone(),
        num_labels: 0,
        num_hooks: 0,
        col_override: None,
        creator_id: Some(user_id.clone()),
    };
    let source_collection = common::functions::create_collection(collection_meta.clone());
    // Collection with read-only auto_update reference
    let read_only_collection = common::functions::create_collection(collection_meta.clone());
    // Collection with writeable auto_update reference
    let writeable_collection = common::functions::create_collection(collection_meta);

    // Fast track object creation
    let object_meta = TCreateObject {
        creator_id: Some(user_id.clone()),
        collection_id: source_collection.id.to_string(),
        num_labels: 0,
        num_hooks: 0,
        ..Default::default()
    };
    let rev_0_object = common::functions::create_object(&object_meta);

    // Create read-only object reference in another collection
    let read_only_reference_request = common::grpc_helpers::add_token(
        tonic::Request::new(CreateObjectReferenceRequest {
            object_id: rev_0_object.id.to_string(),
            collection_id: source_collection.id.to_string(),
            target_collection_id: read_only_collection.id.to_string(),
            writeable: false,
            auto_update: true,
            sub_path: "".to_string(),
        }),
        common::oidc::ADMINTOKEN,
    );
    let read_only_reference_response = object_service
        .create_object_reference(read_only_reference_request)
        .await;

    assert!(read_only_reference_response.is_ok());

    // Create writeable object reference in another collection
    let writeable_reference_request = common::grpc_helpers::add_token(
        tonic::Request::new(CreateObjectReferenceRequest {
            object_id: rev_0_object.id.to_string(),
            collection_id: source_collection.id.to_string(),
            target_collection_id: writeable_collection.id.to_string(),
            writeable: true,
            auto_update: true,
            sub_path: "".to_string(),
        }),
        common::oidc::ADMINTOKEN,
    );
    let writeable_reference_response = object_service
        .create_object_reference(writeable_reference_request)
        .await;

    assert!(writeable_reference_response.is_ok());

    //Loop through permissions and add labels to object in source collection
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
            project_id.as_str(),
            user_id.as_str(),
            permission,
            common::oidc::ADMINTOKEN,
        )
        .await;
        assert_eq!(edit_perm.permission, *permission as i32);

        // Add labels to object without creating a new revision
        let add_labels_request = common::grpc_helpers::add_token(
            tonic::Request::new(AddLabelsToObjectRequest {
                collection_id: source_collection.id.to_string(),
                object_id: rev_0_object.id.to_string(),
                labels_to_add: vec![KeyValue {
                    key: "permission".to_string(),
                    value: permission.clone().as_str_name().to_string(),
                }],
            }),
            common::oidc::REGULARTOKEN,
        );
        let add_labels_response = object_service
            .add_labels_to_object(add_labels_request)
            .await;

        match *permission {
            Permission::None | Permission::Read | Permission::Append => {
                // Request should fail with insufficient permissions
                assert!(add_labels_response.is_err());
            }
            Permission::Modify | Permission::Admin => {
                assert!(add_labels_response.is_ok());

                let proto_object = add_labels_response.unwrap().into_inner().object.unwrap();

                assert!(proto_object.labels.contains(&KeyValue {
                    key: "permission".to_string(),
                    value: permission.clone().as_str_name().to_string(),
                }))
            }
            _ => panic!("Unspecified permission is not allowed."),
        }
    }

    // Validate object has exactly two labels from Modify and Admin
    let get_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(GetObjectByIdRequest {
            collection_id: source_collection.id.clone(),
            object_id: rev_0_object.id.clone(),
            with_url: false,
        }),
        common::oidc::ADMINTOKEN,
    );
    let get_object_response = object_service
        .get_object_by_id(get_object_request)
        .await
        .unwrap()
        .into_inner();

    let rev_0_object = get_object_response.object.unwrap().object.unwrap();
    assert_eq!(rev_0_object.labels.len(), 2);
    assert!(rev_0_object.labels.contains(&KeyValue {
        key: "permission".to_string(),
        value: Permission::Modify.as_str_name().to_string(),
    }));
    assert!(rev_0_object.labels.contains(&KeyValue {
        key: "permission".to_string(),
        value: Permission::Admin.as_str_name().to_string(),
    }));

    // Add labels through writeable reference
    let add_labels_request = common::grpc_helpers::add_token(
        tonic::Request::new(AddLabelsToObjectRequest {
            collection_id: writeable_collection.id.to_string(),
            object_id: rev_0_object.id.to_string(),
            labels_to_add: vec![KeyValue {
                key: "added_writeable".to_string(),
                value: "Yes baby.".to_string(),
            }],
        }),
        common::oidc::REGULARTOKEN,
    );
    let add_labels_response = object_service
        .add_labels_to_object(add_labels_request)
        .await
        .unwrap()
        .into_inner();

    let proto_object = add_labels_response.object.unwrap();
    assert!(proto_object.labels.contains(&KeyValue {
        key: "added_writeable".to_string(),
        value: "Yes baby.".to_string(),
    }));

    // Try to add labels through read-only reference
    let add_labels_request = common::grpc_helpers::add_token(
        tonic::Request::new(AddLabelsToObjectRequest {
            collection_id: read_only_collection.id.to_string(),
            object_id: rev_0_object.id.to_string(),
            labels_to_add: vec![KeyValue {
                key: "added_read-only".to_string(),
                value: "Hell no.".to_string(),
            }],
        }),
        common::oidc::REGULARTOKEN,
    );
    let add_labels_response = object_service
        .add_labels_to_object(add_labels_request)
        .await;

    assert!(add_labels_response.is_err());
    println!("{:#?}", add_labels_response);

    // Remove all labels with object update and try to add labels to outdated revision
    let update_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(UpdateObjectRequest {
            object_id: rev_0_object.id.to_string(),
            collection_id: source_collection.id.to_string(),
            object: Some(StageObject {
                filename: "updated.object".to_string(),
                sub_path: "".to_string(),
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
            hash: None,
        }),
        common::oidc::REGULARTOKEN,
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
    assert_eq!(rev_1_object.labels.len(), 0);

    let add_labels_request = common::grpc_helpers::add_token(
        tonic::Request::new(AddLabelsToObjectRequest {
            collection_id: source_collection.id.to_string(),
            object_id: rev_0_object.id.to_string(),
            labels_to_add: vec![KeyValue {
                key: "added_outdated".to_string(),
                value: "Hell no.".to_string(),
            }],
        }),
        common::oidc::REGULARTOKEN,
    );
    let add_labels_response = object_service
        .add_labels_to_object(add_labels_request)
        .await;

    assert!(add_labels_response.is_err());
    println!("{:#?}", add_labels_response);

    let get_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(GetObjectByIdRequest {
            collection_id: source_collection.id.clone(),
            object_id: rev_0_object.id.clone(),
            with_url: false,
        }),
        common::oidc::ADMINTOKEN,
    );
    let get_object_response = object_service
        .get_object_by_id(get_object_request)
        .await
        .unwrap()
        .into_inner();

    let rev_0_object = get_object_response.object.unwrap().object.unwrap();

    assert_eq!(rev_0_object.labels.len(), 3); // Still 3: 2 from permissions and one from reference
    assert_eq!(rev_1_object.labels.len(), 0); // Still 0
}

/// The individual steps of this test function contains:
/// 1. Create a random object
/// 2. Try to clone non-existing object
/// 3. Try to clone object in non-existing collection
/// 4. Try to clone object with different permissions
/// 5. Try to clone object in same collection
/// 6. Update Object and clone revision 1
/// 7. Update cloned object and check OriginType
#[ignore]
#[tokio::test]
#[serial(db)]
async fn clone_object_grpc_test() {
    // Init database connection
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone(), ArunaServerConfig::default()).await);

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

    // Fast track adding user to project
    let user_id = get_token_user_id(common::oidc::REGULARTOKEN).await;
    let add_perm = common::grpc_helpers::add_project_permission(
        project_id.as_str(),
        user_id.as_str(),
        common::oidc::ADMINTOKEN,
    )
    .await;
    assert_eq!(add_perm.permission, Permission::None as i32);

    // Fast track collection creation
    let collection_meta = TCreateCollection {
        project_id: project_id.clone(),
        num_labels: 0,
        num_hooks: 0,
        col_override: None,
        creator_id: Some(user_id.clone()),
    };
    let source_collection = common::functions::create_collection(collection_meta.clone());
    // Collection where the object will be cloned to
    let target_collection = common::functions::create_collection(collection_meta.clone());

    // Fast track object creation
    let object_meta = TCreateObject {
        creator_id: Some(user_id.clone()),
        collection_id: source_collection.id.to_string(),
        num_labels: 0,
        num_hooks: 0,
        ..Default::default()
    };
    let rev_0_object = common::functions::create_object(&object_meta);

    assert_eq!(rev_0_object.clone().origin.unwrap().id, rev_0_object.id); // OriginType::User

    // Sleep 1 second to have differing created_at timestamps with cloned objects
    thread::sleep(time::Duration::from_secs(1));

    // Try to clone non-existing object --> Error
    let clone_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(CloneObjectRequest {
            object_id: uuid::Uuid::new_v4().to_string(), // Random uuid.
            collection_id: source_collection.id.to_string(),
            target_collection_id: target_collection.id.to_string(),
        }),
        common::oidc::ADMINTOKEN,
    );
    let clone_object_response = object_service.clone_object(clone_object_request).await;

    assert!(clone_object_response.is_err());

    // Try to clone object from/in non-existing collection --> Error
    let clone_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(CloneObjectRequest {
            object_id: rev_0_object.id.to_string(),
            collection_id: uuid::Uuid::new_v4().to_string(), // Random uuid.
            target_collection_id: target_collection.id.to_string(),
        }),
        common::oidc::ADMINTOKEN,
    );
    let clone_object_response = object_service.clone_object(clone_object_request).await;

    assert!(clone_object_response.is_err());

    let clone_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(CloneObjectRequest {
            object_id: rev_0_object.id.to_string(),
            collection_id: source_collection.id.to_string(),
            target_collection_id: uuid::Uuid::new_v4().to_string(), // Random uuid.
        }),
        common::oidc::ADMINTOKEN,
    );
    let clone_object_response = object_service.clone_object(clone_object_request).await;

    assert!(clone_object_response.is_err());

    // Loop through permissions and clone object to target collection
    //ToDo: Clone with all permission permutations for both collections?
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
            project_id.as_str(),
            user_id.as_str(),
            permission,
            common::oidc::ADMINTOKEN,
        )
        .await;
        assert_eq!(edit_perm.permission, *permission as i32);

        // Clone object in target collection
        let clone_object_request = common::grpc_helpers::add_token(
            tonic::Request::new(CloneObjectRequest {
                object_id: rev_0_object.id.to_string(),
                collection_id: source_collection.id.to_string(),
                target_collection_id: target_collection.id.to_string(),
            }),
            common::oidc::REGULARTOKEN,
        );
        let clone_object_response = object_service.clone_object(clone_object_request).await;

        match *permission {
            Permission::None | Permission::Read => {
                // Request should fail with insufficient permissions
                assert!(clone_object_response.is_err());
            }
            Permission::Append | Permission::Modify | Permission::Admin => {
                assert!(clone_object_response.is_ok());

                let proto_object = clone_object_response.unwrap().into_inner().object.unwrap();

                // Check attributes from object in clone response
                assert_ne!(rev_0_object.id, proto_object.id);
                assert_ne!(
                    rev_0_object.created.as_ref().unwrap(),
                    proto_object.created.as_ref().unwrap()
                );
                assert_eq!(rev_0_object.id, proto_object.origin.clone().unwrap().id);
                assert_eq!(rev_0_object.filename, proto_object.filename);
                assert_eq!(rev_0_object.content_len, proto_object.content_len);
                assert_eq!(rev_0_object.hash, proto_object.hash);
                assert_eq!(rev_0_object.origin, proto_object.origin); // Both originate from the same object

                assert_eq!(proto_object.rev_number, 0);

                // Get cloned object and check attributes
                let get_object_request = common::grpc_helpers::add_token(
                    tonic::Request::new(GetObjectByIdRequest {
                        collection_id: target_collection.id.to_string(),
                        object_id: proto_object.id.to_string(),
                        with_url: false,
                    }),
                    common::oidc::REGULARTOKEN,
                );
                let get_object_response = object_service
                    .get_object_by_id(get_object_request)
                    .await
                    .unwrap()
                    .into_inner();
                let cloned_object = get_object_response.object.unwrap().object.unwrap();

                assert_ne!(rev_0_object.id, cloned_object.id);
                assert_ne!(
                    rev_0_object.created.as_ref().unwrap(),
                    cloned_object.created.as_ref().unwrap()
                );

                assert_eq!(rev_0_object.id, cloned_object.origin.clone().unwrap().id);
                assert_eq!(rev_0_object.filename, cloned_object.filename);
                assert_eq!(rev_0_object.content_len, cloned_object.content_len);
                assert_eq!(rev_0_object.hash, cloned_object.hash);
                assert_eq!(rev_0_object.origin, cloned_object.origin); // Both originate from the same object

                assert_eq!(cloned_object.rev_number, 0);
                assert_eq!(cloned_object.clone().origin.unwrap().id, rev_0_object.id);
                // OriginType::Objclone
            }
            _ => panic!("Unspecified permission is not allowed."),
        }
    }

    // Try to clone in same collection
    let clone_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(CloneObjectRequest {
            object_id: rev_0_object.id.to_string(),
            collection_id: source_collection.id.to_string(),
            target_collection_id: source_collection.id.to_string(),
        }),
        common::oidc::REGULARTOKEN,
    );
    let clone_object_response = object_service
        .clone_object(clone_object_request)
        .await
        .unwrap()
        .into_inner();

    let proto_object = clone_object_response.object.unwrap();
    assert_ne!(rev_0_object.id, proto_object.id);
    assert_ne!(
        rev_0_object.created.as_ref().unwrap(),
        proto_object.created.as_ref().unwrap()
    );

    assert_eq!(rev_0_object.id, proto_object.origin.clone().unwrap().id);
    assert_eq!(rev_0_object.filename, proto_object.filename);
    assert_eq!(rev_0_object.content_len, proto_object.content_len);
    assert_eq!(rev_0_object.hash, proto_object.hash);
    assert_eq!(rev_0_object.origin, proto_object.origin); // Both originate from the same object

    assert_eq!(proto_object.rev_number, 0);
    assert_eq!(proto_object.clone().origin.unwrap().id, rev_0_object.id); // OriginType::Objclone

    // Fast track update source object
    let rev_1_object = common::functions::update_object(&TCreateUpdate {
        original_object: rev_0_object.clone(),
        collection_id: source_collection.id.to_string(),
        new_name: "updated.object".to_string(),
        content_len: 1234,
        ..Default::default()
    });

    // Sleep 1 second to have differing created_at timestamps with cloned objects
    thread::sleep(time::Duration::from_secs(1));

    assert_eq!(rev_1_object.clone().origin.unwrap().id, rev_0_object.id); // Still OriginType::User

    // Clone revision 1 of source object
    let clone_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(CloneObjectRequest {
            object_id: rev_1_object.id.to_string(),
            collection_id: source_collection.id.to_string(),
            target_collection_id: target_collection.id.to_string(),
        }),
        common::oidc::REGULARTOKEN,
    );
    let clone_object_response = object_service
        .clone_object(clone_object_request)
        .await
        .unwrap()
        .into_inner();
    let clone_rev_0_object = clone_object_response.object.unwrap();

    assert_ne!(rev_1_object.id, clone_rev_0_object.id);
    assert_ne!(
        rev_1_object.created.as_ref().unwrap(),
        clone_rev_0_object.created.as_ref().unwrap()
    );
    assert_ne!(rev_1_object.origin, clone_rev_0_object.origin);

    assert_eq!(
        rev_1_object.id,
        clone_rev_0_object.origin.clone().unwrap().id
    );
    assert_eq!(rev_1_object.filename, clone_rev_0_object.filename);
    assert_eq!(rev_1_object.content_len, clone_rev_0_object.content_len);
    assert_eq!(rev_1_object.hash, clone_rev_0_object.hash);

    assert_eq!(clone_rev_0_object.filename, "updated.object".to_string());
    assert_eq!(clone_rev_0_object.content_len, 1234);
    assert_eq!(clone_rev_0_object.rev_number, 0);
    assert_eq!(
        clone_rev_0_object.clone().origin.unwrap().id,
        rev_1_object.id
    ); // OriginType::Objclone

    // Fast track update cloned object
    let clone_rev_1 = common::functions::update_object(&TCreateUpdate {
        original_object: clone_rev_0_object.clone(),
        collection_id: target_collection.id.to_string(),
        new_name: "clone.update".to_string(),
        content_len: 123456,
        ..Default::default()
    });

    assert_eq!(clone_rev_1.origin.unwrap().id, clone_rev_0_object.id); // Still OriginType::Objclone
}

/// The individual steps of this test function contains:
/// 1. Try to delete non-existing object
/// 2. Try to delete with invalid collection
/// 3. Try to normal delete object with different permissions
/// 4. Try to force delete object with different permissions
/// 5. Delete staging object from initialize and update
#[ignore]
#[tokio::test]
#[serial(db)]
async fn delete_object_grpc_test() {
    // Init database connection
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone(), ArunaServerConfig::default()).await);

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

    // Fast track adding user to project
    let user_id = get_token_user_id(common::oidc::REGULARTOKEN).await;
    let add_perm = common::grpc_helpers::add_project_permission(
        project_id.as_str(),
        user_id.as_str(),
        common::oidc::ADMINTOKEN,
    )
    .await;
    assert_eq!(add_perm.permission, Permission::None as i32);

    // Fast track collection creation
    let collection_meta = TCreateCollection {
        project_id: project_id.clone(),
        num_labels: 0,
        num_hooks: 0,
        col_override: None,
        creator_id: Some(user_id.clone()),
    };
    let random_collection = common::functions::create_collection(collection_meta.clone());

    // Fast track random object creation
    let object_meta = TCreateObject {
        creator_id: Some(user_id.clone()),
        collection_id: random_collection.id.to_string(),
        num_labels: 0,
        num_hooks: 0,
        ..Default::default()
    };
    let random_object = common::functions::create_object(&object_meta);

    // Try to delete non-existing object
    let delete_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(DeleteObjectRequest {
            object_id: "".to_string(),
            collection_id: random_collection.id.to_string(),
            with_revisions: false,
            force: false,
        }),
        common::oidc::ADMINTOKEN,
    );
    let delete_object_response = object_service.delete_object(delete_object_request).await;

    assert!(delete_object_response.is_err());

    // Try to delete object with invalid collection
    let delete_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(DeleteObjectRequest {
            object_id: random_object.id.to_string(),
            collection_id: "".to_string(),
            with_revisions: false,
            force: false,
        }),
        common::oidc::ADMINTOKEN,
    );
    let delete_object_response = object_service.delete_object(delete_object_request).await;

    assert!(delete_object_response.is_err());

    // Try to normal/force delete object with single revision with different permissions.
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
            project_id.as_str(),
            user_id.as_str(),
            permission,
            common::oidc::ADMINTOKEN,
        )
        .await;
        assert_eq!(edit_perm.permission, *permission as i32);

        // Create random object
        let object_meta = TCreateObject {
            creator_id: Some(user_id.clone()),
            collection_id: random_collection.id.to_string(),
            num_labels: 0,
            num_hooks: 0,
            ..Default::default()
        };
        let random_object = common::functions::create_object(&object_meta);

        // Try to normally delete object without force
        let inner_delete_object_request = DeleteObjectRequest {
            object_id: random_object.id.to_string(),
            collection_id: random_collection.id.to_string(),
            with_revisions: false,
            force: false,
        };

        let delete_object_request = common::grpc_helpers::add_token(
            tonic::Request::new(inner_delete_object_request.clone()),
            common::oidc::REGULARTOKEN,
        );
        let delete_object_response = object_service.delete_object(delete_object_request).await;

        match *permission {
            Permission::None | Permission::Read => {
                // Request should fail with insufficient permissions and/or not using force to delete last undeleted revision
                assert!(delete_object_response.is_err());
            }
            Permission::Append | Permission::Modify | Permission::Admin => {
                // Request should fail with insufficient permissions and/or not using force to delete last undeleted revision
                assert!(delete_object_response.is_ok());

                // Validate that object still exists
                let get_object_request = common::grpc_helpers::add_token(
                    tonic::Request::new(GetObjectByIdRequest {
                        collection_id: random_collection.id.to_string(),
                        object_id: random_object.id.to_string(),
                        with_url: false,
                    }),
                    common::oidc::ADMINTOKEN,
                );
                let get_object_response = object_service.get_object_by_id(get_object_request).await;

                assert!(get_object_response.is_err());

                let trashed_object =
                    common::functions::get_raw_db_object_by_id(&random_object.id.to_string());
                assert_eq!(trashed_object.id.to_string(), random_object.id);
                assert_eq!(trashed_object.object_status, ObjectStatus::TRASH);

                let all_references_request = common::grpc_helpers::add_token(
                    tonic::Request::new(GetReferencesRequest {
                        collection_id: random_collection.id.to_string(),
                        object_id: random_object.id.to_string(),
                        with_revisions: true,
                    }),
                    common::oidc::ADMINTOKEN,
                );
                assert_eq!(
                    object_service
                        .get_references(all_references_request)
                        .await
                        .unwrap()
                        .into_inner()
                        .references
                        .into_iter()
                        .filter(|reference| reference.is_writeable)
                        .count(),
                    0
                ); // At least one writeable reference exist. Could also be less than 0 but that is very improbable...
            }
            _ => panic!("Unspecified permission is not allowed."),
        }
    }

    // Initialize and delete staging object
    let init_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(InitializeNewObjectRequest {
            object: Some(StageObject {
                filename: "stage.object".to_string(),
                sub_path: "".to_string(),
                content_len: 0,
                source: None,
                dataclass: DataClass::Private as i32,
                labels: vec![],
                hooks: vec![],
            }),
            collection_id: random_collection.id.to_string(),
            preferred_endpoint_id: "".to_string(),
            multipart: false,
            is_specification: false,
            hash: None,
        }),
        common::oidc::REGULARTOKEN,
    );
    let init_object_response = object_service
        .initialize_new_object(init_object_request)
        .await;

    assert!(init_object_response.is_ok());

    let staging_object_id = init_object_response.unwrap().into_inner().object_id;

    let delete_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(DeleteObjectRequest {
            object_id: staging_object_id.to_string(),
            collection_id: random_collection.id.to_string(),
            with_revisions: false,
            force: false,
        }),
        common::oidc::REGULARTOKEN,
    );
    let delete_object_response = object_service.delete_object(delete_object_request).await;

    assert!(delete_object_response.is_ok());

    let object_references = common::functions::get_object_references(
        random_collection.id.to_string(),
        staging_object_id.to_string(),
        false,
    );

    assert!(object_references.is_empty());

    let staging_object = common::functions::get_raw_db_object_by_id(&staging_object_id);
    assert_eq!(staging_object.id.to_string(), staging_object_id);
    assert_eq!(staging_object.object_status, ObjectStatus::TRASH);
    assert!(staging_object.revision_number < 0);

    // Try to get deleted initialize staging object --> Does not exist anymore
    let get_deleted_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(GetObjectByIdRequest {
            collection_id: random_collection.id.to_string(),
            object_id: staging_object_id.to_string(),
            with_url: false,
        }),
        common::oidc::REGULARTOKEN,
    );
    let get_deleted_object_response = object_service
        .get_object_by_id(get_deleted_object_request)
        .await;
    assert!(get_deleted_object_response.is_err());

    let trashed_object = common::functions::get_raw_db_object_by_id(&staging_object_id);
    assert_eq!(trashed_object.id.to_string(), staging_object_id);
    assert_eq!(trashed_object.object_status, ObjectStatus::TRASH);

    // Update object and delete staging object
    let random_object = common::functions::create_object(&TCreateObject {
        creator_id: Some(user_id.clone()),
        collection_id: random_collection.id.to_string(),
        num_labels: 0,
        num_hooks: 0,
        ..Default::default()
    });

    let update_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(UpdateObjectRequest {
            object_id: random_object.id.to_string(),
            collection_id: random_collection.id.to_string(),
            object: Some(StageObject {
                filename: "stage.update".to_string(),
                sub_path: "".to_string(),
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
            hash: None,
        }),
        common::oidc::REGULARTOKEN,
    );
    let update_object_response = object_service.update_object(update_object_request).await;

    assert!(update_object_response.is_ok());

    let staging_object_id = update_object_response.unwrap().into_inner().object_id;

    let delete_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(DeleteObjectRequest {
            object_id: staging_object_id.to_string(),
            collection_id: random_collection.id.to_string(),
            with_revisions: false,
            force: false,
        }),
        common::oidc::REGULARTOKEN,
    );
    let delete_object_response = object_service.delete_object(delete_object_request).await;

    assert!(delete_object_response.is_ok());

    let object_references = common::functions::get_object_references(
        random_collection.id.to_string(),
        staging_object_id.to_string(),
        false,
    );

    assert!(object_references.is_empty());

    let staging_object = common::functions::get_raw_db_object_by_id(&staging_object_id.to_string());

    assert_eq!(staging_object.id.to_string(), staging_object_id);
    assert_eq!(staging_object.object_status, ObjectStatus::TRASH);
    assert!(staging_object.revision_number < 0);

    // Update object again and check if revision number is 1
    let updated_object = common::functions::update_object(&TCreateUpdate {
        original_object: random_object.clone(),
        collection_id: random_collection.id.to_string(),
        new_name: "rev1.object".to_string(),
        content_len: 1234,
        ..Default::default()
    });

    assert_ne!(updated_object.id, staging_object_id);
    assert_eq!(updated_object.rev_number, 1);
}

/// The individual steps of this test function contains:
/// 1. Try to delete object revisions in random order
/// 2. Try to delete object with read-only references
/// 3. Try to delete object with writeable references
/// 4. Try to force delete object with revisions with mixed references
#[ignore]
#[tokio::test]
#[serial(db)]
async fn delete_object_revisions_grpc_test() {
    // Init database connection
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone(), ArunaServerConfig::default()).await);

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

    // Fast track adding user to project
    let user_id = get_token_user_id(common::oidc::REGULARTOKEN).await;
    let add_perm = common::grpc_helpers::add_project_permission(
        project_id.as_str(),
        user_id.as_str(),
        common::oidc::ADMINTOKEN,
    )
    .await;
    assert_eq!(add_perm.permission, Permission::None as i32);

    // Fast track collection creation
    let collection_meta = TCreateCollection {
        project_id: project_id.clone(),
        num_labels: 0,
        num_hooks: 0,
        col_override: None,
        creator_id: Some(user_id.clone()),
    };
    let random_collection = common::functions::create_collection(collection_meta.clone());

    // Fast track random object creation
    let object_meta = TCreateObject {
        creator_id: Some(user_id.clone()),
        collection_id: random_collection.id.to_string(),
        num_labels: 0,
        num_hooks: 0,
        ..Default::default()
    };
    let rev_0_object = common::functions::create_object(&object_meta);

    // Fast track object update to create revisions
    let mut update_meta = TCreateUpdate {
        original_object: rev_0_object.clone(),
        collection_id: random_collection.id.to_string(),
        new_name: "rev1.object".to_string(),
        content_len: 1234,
        ..Default::default()
    };
    let rev_1_object = common::functions::update_object(&update_meta);

    update_meta.original_object = rev_1_object.clone();
    update_meta.new_name = "rev2.object".to_string();
    update_meta.content_len = 123456;

    let rev_2_object = common::functions::update_object(&update_meta);

    let mut inner_delete_request = DeleteObjectRequest {
        object_id: rev_2_object.id.to_string(),
        collection_id: random_collection.id.to_string(),
        with_revisions: false,
        force: false,
    };

    // Delete object revisions in "random" order 2,0,1
    let delete_rev_2_request = common::grpc_helpers::add_token(
        tonic::Request::new(inner_delete_request.clone()),
        common::oidc::ADMINTOKEN,
    );
    let delete_rev_2_response = object_service.delete_object(delete_rev_2_request).await;

    assert!(delete_rev_2_response.is_ok());

    let rev_2_deleted = common::functions::get_raw_db_object_by_id(&rev_2_object.id.to_string());
    assert_eq!(rev_2_deleted.id.to_string(), rev_2_object.id);
    assert_eq!(rev_2_deleted.object_status, ObjectStatus::TRASH); // Validate deletion

    // Validate that latest reference should now point to revision 1
    let object_references = common::functions::get_object_references(
        random_collection.id.to_string(),
        rev_2_object.id.to_string(),
        true,
    );
    assert_eq!(object_references.len(), 0); // No references should exist

    // Check status of all 3 objects
    let mut raw_db_object = get_object_status_raw(&rev_0_object.id);
    assert_eq!(raw_db_object.object_status, ObjectStatus::TRASH);
    raw_db_object = get_object_status_raw(&rev_1_object.id);
    assert_eq!(raw_db_object.object_status, ObjectStatus::TRASH);
    raw_db_object = get_object_status_raw(&rev_2_object.id);
    assert_eq!(raw_db_object.object_status, ObjectStatus::TRASH);

    // Create object with multiple revisions and delete them in one request
    let random_object = common::functions::create_object(&object_meta);

    let mut update_meta = TCreateUpdate {
        original_object: random_object.clone(),
        collection_id: random_collection.id.to_string(),
        new_name: "random_update.01".to_string(),
        content_len: 123,
        ..Default::default()
    };
    let random_object_rev_1 = common::functions::update_object(&update_meta);

    update_meta.original_object = random_object_rev_1.clone();
    update_meta.new_name = "random_update.02".to_string();
    let random_object_rev_2 = common::functions::update_object(&update_meta);

    update_meta.original_object = random_object_rev_2.clone();
    update_meta.new_name = "random_update.03".to_string();
    let random_object_rev_3 = common::functions::update_object(&update_meta);

    inner_delete_request.object_id = random_object_rev_3.id.to_string();
    inner_delete_request.force = true;
    inner_delete_request.with_revisions = true;

    let delete_revisions_request = common::grpc_helpers::add_token(
        tonic::Request::new(inner_delete_request),
        common::oidc::ADMINTOKEN,
    );
    let delete_revisions_response = object_service.delete_object(delete_revisions_request).await;

    assert!(delete_revisions_response.is_ok());

    // Validate deletion of all revisions
    //   - Get all references of object and check that there are none left
    //   - Get object and check status
    for trashed_object in vec![
        random_object,
        random_object_rev_1,
        random_object_rev_2,
        random_object_rev_3,
    ] {
        let get_references_request = common::grpc_helpers::add_token(
            tonic::Request::new(GetReferencesRequest {
                collection_id: random_collection.id.to_string(),
                object_id: trashed_object.id.to_string(),
                with_revisions: false,
            }),
            common::oidc::ADMINTOKEN,
        );
        let get_references_response = object_service.get_references(get_references_request).await;

        assert!(get_references_response.is_ok());
        assert!(get_references_response
            .unwrap()
            .into_inner()
            .references
            .is_empty());

        let get_object_request = common::grpc_helpers::add_token(
            tonic::Request::new(GetObjectByIdRequest {
                collection_id: random_collection.id.to_string(),
                object_id: trashed_object.id.to_string(),
                with_url: false,
            }),
            common::oidc::ADMINTOKEN,
        );
        let get_object_response = object_service.get_object_by_id(get_object_request).await;
        assert!(get_object_response.is_err());

        let trashed_db_object =
            common::functions::get_raw_db_object_by_id(&trashed_object.id.to_string());
        assert_eq!(trashed_db_object.id.to_string(), trashed_object.id);
        assert_eq!(trashed_db_object.object_status, ObjectStatus::TRASH);
    }
}

/// The individual steps of this test function contains:
/// 1. Delete multiple individual objects with different permissions
/// 2. Delete multiple individual objects with their revisions with different permissions
/// 3. Delete all revisions individually of same object in random order
#[ignore]
#[tokio::test]
#[serial(db)]
async fn delete_multiple_objects_grpc_test() {
    // Init database connection
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone(), ArunaServerConfig::default()).await);

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

    // Fast track adding user to project
    let user_id = get_token_user_id(common::oidc::REGULARTOKEN).await;
    let add_perm = common::grpc_helpers::add_project_permission(
        project_id.as_str(),
        user_id.as_str(),
        common::oidc::ADMINTOKEN,
    )
    .await;
    assert_eq!(add_perm.permission, Permission::None as i32);

    // Fast track collection creation
    let collection_meta = TCreateCollection {
        project_id: project_id.clone(),
        num_labels: 0,
        num_hooks: 0,
        col_override: None,
        creator_id: Some(user_id.clone()),
    };
    let random_collection = common::functions::create_collection(collection_meta.clone());

    // Create multiple objects without revisions and delete them all at once with different permissions
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
            project_id.as_str(),
            user_id.as_str(),
            permission,
            common::oidc::ADMINTOKEN,
        )
        .await;
        assert_eq!(edit_perm.permission, *permission as i32);

        // Create objects
        let mut object_ids = Vec::new();
        for _ in 1..3 {
            object_ids.push(
                common::functions::create_object(&TCreateObject {
                    creator_id: Some(user_id.clone()),
                    collection_id: random_collection.id.to_string(),
                    num_labels: 0,
                    num_hooks: 0,
                    ..Default::default()
                })
                .id,
            );
        }

        // Try delete objects without force
        let inner_delete_request = DeleteObjectsRequest {
            object_ids: object_ids.clone(),
            collection_id: random_collection.id.to_string(),
            with_revisions: false,
            force: false,
        };
        let delete_multiple_objects_request = common::grpc_helpers::add_token(
            tonic::Request::new(inner_delete_request.clone()),
            common::oidc::REGULARTOKEN,
        );
        let delete_multiple_objects_response = object_service
            .delete_objects(delete_multiple_objects_request)
            .await;

        match *permission {
            Permission::None | Permission::Read => {
                // Request should fail with insufficient permissions
                assert!(delete_multiple_objects_response.is_err());
            }
            Permission::Append | Permission::Modify | Permission::Admin => {
                assert!(delete_multiple_objects_response.is_ok());

                // Validate deletion:
                //   - Get all references of object and check that there are none left
                //   - Get objects of collection and check status
                for object_uuid in object_ids {
                    let get_references_request = common::grpc_helpers::add_token(
                        tonic::Request::new(GetReferencesRequest {
                            collection_id: random_collection.id.to_string(),
                            object_id: object_uuid.to_string(),
                            with_revisions: false,
                        }),
                        common::oidc::ADMINTOKEN,
                    );
                    let get_references_response =
                        object_service.get_references(get_references_request).await;

                    assert!(get_references_response.is_ok());
                    assert!(get_references_response
                        .unwrap()
                        .into_inner()
                        .references
                        .is_empty());

                    let get_object_request = common::grpc_helpers::add_token(
                        tonic::Request::new(GetObjectByIdRequest {
                            collection_id: random_collection.id.to_string(),
                            object_id: object_uuid.to_string(),
                            with_url: false,
                        }),
                        common::oidc::ADMINTOKEN,
                    );
                    let get_object_response =
                        object_service.get_object_by_id(get_object_request).await;
                    assert!(get_object_response.is_err());

                    let trashed_object = common::functions::get_raw_db_object_by_id(&object_uuid);
                    assert_eq!(trashed_object.object_status, ObjectStatus::TRASH)
                }
            }
            _ => panic!("Unspecified permission is not allowed."),
        };
    }

    // Create multiple objects with revisions and delete them all at once with different permissions
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
            project_id.as_str(),
            user_id.as_str(),
            permission,
            common::oidc::ADMINTOKEN,
        )
        .await;
        assert_eq!(edit_perm.permission, *permission as i32);

        // Create multiple objects
        let mut rng = rand::thread_rng();
        let mut object_ids = Vec::new();
        for _ in 1..3 {
            let mut source_object = common::functions::create_object(&TCreateObject {
                creator_id: Some(user_id.clone()),
                collection_id: random_collection.id.to_string(),
                ..Default::default()
            });

            for update_num in 1..=rng.gen_range(1..3) {
                // 3 revisions for each object
                let updated_object = common::functions::update_object(&TCreateUpdate {
                    original_object: source_object.clone(),
                    collection_id: random_collection.id.to_string(),
                    new_name: format!("update.{}.{}", source_object.id, update_num).to_string(),
                    content_len: rng.gen_range(1234..123456),
                    ..Default::default()
                });

                source_object = updated_object;
            }

            object_ids.push(source_object.id);
        }

        // Try delete objects without force
        let inner_delete_request = DeleteObjectsRequest {
            object_ids: object_ids.clone(),
            collection_id: random_collection.id.to_string(),
            with_revisions: true,
            force: false,
        };
        let delete_multiple_objects_request = common::grpc_helpers::add_token(
            tonic::Request::new(inner_delete_request.clone()),
            common::oidc::REGULARTOKEN,
        );
        let delete_multiple_objects_response = object_service
            .delete_objects(delete_multiple_objects_request)
            .await;

        match *permission {
            Permission::None | Permission::Read => {
                // Request should fail with insufficient permissions
                assert!(delete_multiple_objects_response.is_err());
            }
            Permission::Admin | Permission::Append | Permission::Modify => {
                assert!(delete_multiple_objects_response.is_ok());

                // Validate deletion:
                //   - Get all references of object and check that there are none left
                //   - Get objects of collection and check status
                for object_uuid in object_ids {
                    let get_references_request = common::grpc_helpers::add_token(
                        tonic::Request::new(GetReferencesRequest {
                            collection_id: random_collection.id.to_string(),
                            object_id: object_uuid.to_string(),
                            with_revisions: true,
                        }),
                        common::oidc::ADMINTOKEN,
                    );
                    let get_references_response =
                        object_service.get_references(get_references_request).await;

                    assert!(get_references_response.is_ok());
                    assert!(get_references_response
                        .unwrap()
                        .into_inner()
                        .references
                        .is_empty());

                    let get_object_request = common::grpc_helpers::add_token(
                        tonic::Request::new(GetObjectByIdRequest {
                            collection_id: random_collection.id.to_string(),
                            object_id: object_uuid.to_string(),
                            with_url: false,
                        }),
                        common::oidc::ADMINTOKEN,
                    );
                    let get_object_response =
                        object_service.get_object_by_id(get_object_request).await;
                    assert!(get_object_response.is_err());

                    let trashed_object = common::functions::get_raw_db_object_by_id(&object_uuid);
                    assert_eq!(trashed_object.object_status, ObjectStatus::TRASH)
                }
            }
            _ => panic!("Unspecified permission is not allowed."),
        };
    }

    // Create single object with revisions and delete all of them in incorrect order
    let random_object = common::functions::create_object(&TCreateObject {
        creator_id: Some(user_id.clone()),
        collection_id: random_collection.id.to_string(),
        ..Default::default()
    });

    let mut update_meta = TCreateUpdate {
        original_object: random_object.clone(),
        collection_id: random_collection.id.to_string(),
        new_name: "random_update.01".to_string(),
        content_len: 123,
        ..Default::default()
    };
    let random_object_rev_1 = common::functions::update_object(&update_meta);

    update_meta.original_object = random_object_rev_1.clone();
    update_meta.new_name = "random_update.02".to_string();
    let random_object_rev_2 = common::functions::update_object(&update_meta);

    update_meta.original_object = random_object_rev_2.clone();
    update_meta.new_name = "random_update.03".to_string();
    let random_object_rev_3 = common::functions::update_object(&update_meta);

    let object_ids = vec![
        random_object_rev_1.id,
        random_object_rev_3.id.clone(),
        random_object.id,
        random_object_rev_2.id,
    ];
    let mut inner_delete_request = DeleteObjectsRequest {
        object_ids: object_ids.clone(),
        collection_id: random_collection.id.to_string(),
        with_revisions: false,
        force: false,
    };
    let delete_multiple_objects_request = common::grpc_helpers::add_token(
        tonic::Request::new(inner_delete_request.clone()),
        common::oidc::REGULARTOKEN,
    );
    let delete_multiple_objects_response = object_service
        .delete_objects(delete_multiple_objects_request)
        .await;

    assert!(delete_multiple_objects_response.is_err());

    inner_delete_request.force = true;

    let delete_multiple_objects_request = common::grpc_helpers::add_token(
        tonic::Request::new(inner_delete_request.clone()),
        common::oidc::REGULARTOKEN,
    );
    let delete_multiple_objects_response = object_service
        .delete_objects(delete_multiple_objects_request)
        .await;

    assert!(delete_multiple_objects_response.is_err());

    let object_ids = vec![random_object_rev_3.id.clone()];
    inner_delete_request = DeleteObjectsRequest {
        object_ids: object_ids.clone(),
        collection_id: random_collection.id.to_string(),
        with_revisions: false,
        force: false,
    };
    let delete_multiple_objects_request = common::grpc_helpers::add_token(
        tonic::Request::new(inner_delete_request.clone()),
        common::oidc::REGULARTOKEN,
    );
    let delete_multiple_objects_response = object_service
        .delete_objects(delete_multiple_objects_request)
        .await;
    delete_multiple_objects_response.unwrap();

    // Validate deletion:
    //   - Get all references of object and check that there are none left
    //   - Get objects of collection and check status
    for object_uuid in object_ids {
        let get_references_request = common::grpc_helpers::add_token(
            tonic::Request::new(GetReferencesRequest {
                collection_id: random_collection.id.to_string(),
                object_id: object_uuid.to_string(),
                with_revisions: false,
            }),
            common::oidc::ADMINTOKEN,
        );
        let get_references_response = object_service.get_references(get_references_request).await;

        assert!(get_references_response.is_ok());
        assert!(get_references_response
            .unwrap()
            .into_inner()
            .references
            .is_empty());

        let deleted_object = common::functions::get_raw_db_object_by_id(&object_uuid);

        assert_eq!(deleted_object.object_status, ObjectStatus::TRASH);

        let get_object_request = common::grpc_helpers::add_token(
            tonic::Request::new(GetObjectByIdRequest {
                collection_id: random_collection.id.to_string(),
                object_id: object_uuid,
                with_url: false,
            }),
            common::oidc::ADMINTOKEN,
        );
        let get_object_response = object_service.get_object_by_id(get_object_request).await;
        assert!(get_object_response.is_err());
    }
}
