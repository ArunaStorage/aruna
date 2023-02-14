use crate::common::functions::{TCreateCollection, TCreateObject};
use crate::common::grpc_helpers::get_token_user_id;

use aruna_rust_api::api::storage::models::v1::{DataClass, Hash, Hashalgorithm, Permission};
use aruna_rust_api::api::storage::services::v1::object_service_server::ObjectService;
use aruna_rust_api::api::storage::services::v1::{
    CreateObjectPathRequest, FinishObjectStagingRequest, GetObjectByIdRequest,
    GetObjectPathsRequest, InitializeNewObjectRequest, StageObject,
};
use aruna_server::config::ArunaServerConfig;
use aruna_server::database;
use aruna_server::server::services::authz::Authz;
use aruna_server::server::services::object::ObjectServiceImpl;

use serial_test::serial;
use std::sync::Arc;

mod common;

/// The individual steps of this test function contains:
/// 1) Creating an object with the default subpath
/// 2) Creating an object with a specific subpath
#[ignore]
#[tokio::test]
#[serial(db)]
async fn create_object_with_path_grpc_test() {
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

    // Create random object with default subpath
    let object_meta = TCreateObject {
        creator_id: Some(user_id.clone()),
        collection_id: random_collection.id.to_string(),
        num_labels: 0,
        num_hooks: 0,
        ..Default::default()
    };
    let random_object = common::functions::create_object(&object_meta);

    // Get object and validate empty default path
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

    let default_object_with_url = get_object_response.object.unwrap();
    let default_object = default_object_with_url.object.unwrap();

    assert_eq!(default_object.id, random_object.id);
    assert_eq!(default_object_with_url.paths.len(), 1); // Only empty default path
    assert!(default_object_with_url.paths.contains(&"".to_string()));
    assert!(default_object_with_url.url.is_empty());

    // Create object with custom sub_path
    let init_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(InitializeNewObjectRequest {
            object: Some(StageObject {
                filename: "test.file".to_string(),
                sub_path: "/some/sub/path".to_string(),
                content_len: 3210,
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
        common::oidc::ADMINTOKEN,
    );

    let init_object_response = object_service
        .initialize_new_object(init_object_request)
        .await
        .unwrap()
        .into_inner();

    // Finish object
    let finish_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(FinishObjectStagingRequest {
            object_id: init_object_response.object_id.to_string(),
            upload_id: init_object_response.upload_id.to_string(),
            collection_id: init_object_response.collection_id.to_string(),
            hash: Some(Hash {
                alg: Hashalgorithm::Sha256 as i32,
                hash: "4ec2d656985e3d823b81cc2cd9b56ec27ab1303cfebaf5f95c37d2fe1661a779"
                    .to_string(),
            }),
            no_upload: false,
            completed_parts: vec![],
            auto_update: true,
        }),
        common::oidc::ADMINTOKEN,
    );

    let finish_object_response = object_service
        .finish_object_staging(finish_object_request)
        .await;

    assert!(finish_object_response.is_ok());

    // Get object and validate path creation
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

    let custom_object_with_url = get_object_response.object.unwrap();
    let custom_object = default_object_with_url.object.unwrap();

    assert_eq!(custom_object.id, init_object_response.object_id);
    assert_eq!(custom_object_with_url.paths.len(), 1); // Only empty default path
    assert!(custom_object_with_url
        .paths
        .contains(&"/some/sub/path".to_string()));
    assert!(custom_object_with_url.url.is_empty());
}

/// The individual steps of this test function contains:
/// 1) Creating an object with the default subpath
/// 2) Try creating a duplicate default path for the same object
/// 3) Creating an additional path for the same object
#[ignore]
#[tokio::test]
#[serial(db)]
async fn create_additional_object_path_grpc_test() {
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
    let random_project = common::functions::create_project(None);

    // Fast track adding user to project
    let user_id = get_token_user_id(common::oidc::REGULARTOKEN).await;
    let add_perm = common::grpc_helpers::add_project_permission(
        random_project.id.as_str(),
        user_id.as_str(),
        common::oidc::ADMINTOKEN,
    )
    .await;
    assert_eq!(add_perm.permission, Permission::None as i32);

    // Fast track collection creation
    let collection_meta = TCreateCollection {
        project_id: random_project.id.to_string(),
        num_labels: 0,
        num_hooks: 0,
        col_override: None,
        creator_id: Some(user_id.clone()),
    };
    let random_collection = common::functions::create_collection(collection_meta.clone());

    // Create random object with default subpath
    let object_meta = TCreateObject {
        creator_id: Some(user_id.clone()),
        collection_id: random_collection.id.to_string(),
        num_labels: 0,
        num_hooks: 0,
        ..Default::default()
    };
    let random_object = common::functions::create_object(&object_meta);

    // Try create duplicate empty path for object
    let create_path_request = common::grpc_helpers::add_token(
        tonic::Request::new(CreateObjectPathRequest {
            collection_id: random_collection.id.to_string(),
            object_id: random_object.id.to_string(),
            sub_path: "".to_string(),
        }),
        common::oidc::ADMINTOKEN,
    );
    let create_path_response = object_service.create_object_path(create_path_request).await;

    assert!(create_path_response.is_err()); // Duplicate paths are not allowed.

    // Try to create additional paths with different permissions
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
            random_project.as_str(),
            user_id.as_str(),
            permission,
            common::oidc::ADMINTOKEN,
        )
        .await;
        assert_eq!(edit_perm.permission, *permission as i32);

        // Create additional custom path for object
        let create_path_request = common::grpc_helpers::add_token(
            tonic::Request::new(CreateObjectPathRequest {
                collection_id: random_collection.id.to_string(),
                object_id: random_object.id.to_string(),
                sub_path: format!("/{permission}/").to_string(),
            }),
            common::oidc::REGULARTOKEN,
        );
        let create_path_response = object_service.create_object_path(create_path_request).await;

        // Check if request succeeded for specific permission
        match *permission {
            Permission::None | Permission::Read | Permission::Append => {
                assert!(create_path_response.is_err())
            }
            Permission::Modify | Permission::Admin => {
                assert!(create_path_response.is_ok());

                // Validate path creation/existence
                let fq_path = format!(
                    "/{}/{}/{:?}/{}",
                    random_project.name,
                    random_collection.name,
                    *permission,
                    random_object.filename
                )
                .to_string();
                let response_path = create_path_response.unwrap().into_inner().path;

                assert_eq!(response_path, fq_path);

                // Get object and validate empty default path
                let get_object_request = common::grpc_helpers::add_token(
                    tonic::Request::new(GetObjectByIdRequest {
                        collection_id: random_collection.id.to_string(),
                        object_id: random_object.id.to_string(),
                        with_url: false,
                    }),
                    common::oidc::ADMINTOKEN,
                );
                let get_object_response = object_service
                    .get_object_by_id(get_object_request)
                    .await
                    .unwrap()
                    .into_inner();

                let object_with_url = get_object_response.object.unwrap();

                assert!(object_with_url.paths.contains(&fq_path))
            }
            _ => panic!("Unspecified permission is not allowed."),
        }
    }

    let get_paths_request = common::grpc_helpers::add_token(
        tonic::Request::new(GetObjectPathsRequest {
            collection_id: random_collection.id.to_string(),
            include_inactive: true,
        }),
        common::oidc::ADMINTOKEN,
    );
    let get_paths_response = object_service
        .get_object_paths(get_paths_request)
        .await
        .unwrap()
        .into_inner();

    assert_eq!(get_paths_response.object_paths.len(), 3) // Default, Modify/Write and Admin
}

/// The individual steps of this test function contains:
/// 1) Creating an object with the default subpath
/// 2) Creating some additional paths for the same object
/// 3) Get each path individually
#[ignore]
#[tokio::test]
#[serial(db)]
async fn get_object_path_grpc_test() {
    todo!()
}

/// The individual steps of this test function contains:
/// 1) Creating an object with the default subpath
/// 2) Creating some additional paths for the same object
/// 3) Get all active object paths
/// 4) Get all object paths including the inactive
#[ignore]
#[tokio::test]
#[serial(db)]
async fn get_object_paths_grpc_test() {
    todo!()
}

/// The individual steps of this test function contains:
/// 1) Creating an object with the default subpath
/// 2) Creating some additional paths for the same object
/// 3) Modify visibility of some paths
#[ignore]
#[tokio::test]
#[serial(db)]
async fn set_object_path_visibility_grpc_test() {
    todo!()
}

/// The individual steps of this test function contains:
/// 1) Creating an object with the default subpath
/// 2) Creating some additional paths for the same object
/// 3) Get object via each path individually
#[ignore]
#[tokio::test]
#[serial(db)]
async fn get_object_by_path_grpc_test() {
    todo!()
}
