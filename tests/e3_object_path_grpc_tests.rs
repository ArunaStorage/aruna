use crate::common::functions::{TCreateCollection, TCreateObject, TCreateUpdate};
use crate::common::grpc_helpers::get_token_user_id;

use aruna_rust_api::api::storage::models::v1::{DataClass, Hash, Hashalgorithm, Permission};
use aruna_rust_api::api::storage::services::v1::object_service_server::ObjectService;
use aruna_rust_api::api::storage::services::v1::{
    CreateObjectPathRequest, CreateObjectReferenceRequest, DeleteObjectRequest,
    FinishObjectStagingRequest, GetObjectByIdRequest, GetObjectPathRequest, GetObjectPathsRequest,
    GetObjectsByPathRequest, InitializeNewObjectRequest, Path, SetObjectPathVisibilityRequest,
    StageObject,
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
        creator_id: Some(user_id.clone()),
        ..Default::default()
    };
    let random_collection = common::functions::create_collection(collection_meta.clone());

    // Reusable static path part
    let static_path_part = format!(
        "s3://latest.{}.{}",
        random_collection.name, random_project.name
    );

    // Create random object with default subpath
    let object_meta = TCreateObject {
        creator_id: Some(user_id.clone()),
        collection_id: random_collection.id.to_string(),
        num_labels: 0,
        num_hooks: 0,
        ..Default::default()
    };
    let random_object = common::functions::create_object(&object_meta);
    let random_object_default_path =
        format!("{static_path_part}/{}", random_object.filename).to_string();

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

    let default_object_with_url = get_object_response.object.unwrap();
    let default_object = default_object_with_url.object.unwrap();

    assert_eq!(default_object.id, random_object.id);
    assert_eq!(default_object_with_url.paths.len(), 1); // Only empty default path
    assert!(default_object_with_url
        .paths
        .contains(&random_object_default_path));
    assert!(default_object_with_url.url.is_empty());

    // Create object with custom sub_path
    let init_object_request = common::grpc_helpers::add_token(
        tonic::Request::new(InitializeNewObjectRequest {
            object: Some(StageObject {
                filename: "test.file".to_string(),
                sub_path: "/some/path".to_string(),
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

    let random_object = finish_object_response.unwrap().into_inner().object.unwrap();
    let random_object_custom_path =
        format!("{static_path_part}/some/path/{}", random_object.filename).to_string();

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
    let custom_object = custom_object_with_url.object.unwrap();

    assert_eq!(custom_object.id, init_object_response.object_id);
    assert_eq!(custom_object_with_url.paths.len(), 1); // Only custom path
    assert!(custom_object_with_url
        .paths
        .contains(&random_object_custom_path));
    assert!(custom_object_with_url.url.is_empty());
}

/// The individual steps of this test function contains:
/// 1) Creating an object with the default subpath
/// 2) Try creating a duplicate default path for the same object
/// 3) Try creating additional paths for the same object with different permissions
#[ignore]
#[tokio::test]
#[serial(db)]
async fn create_additional_object_path_grpc_test() {
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
        creator_id: Some(user_id.clone()),
        ..Default::default()
    };
    let random_collection = common::functions::create_collection(collection_meta.clone());

    // Reusable static path part
    let static_path_part = format!(
        "s3://latest.{}.{}",
        random_collection.name, random_project.name
    );

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

    assert!(create_path_response.is_ok()); // Duplicate paths are ignored on creation.

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
            random_project.id.as_str(),
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
                sub_path: format!("/{:?}/", permission).to_lowercase(),
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
                // Validate path creation/existence
                let permission_name = format!("{:?}", *permission).to_lowercase();
                let fq_path = format!(
                    "{}/{}/{}",
                    static_path_part, permission_name, random_object.filename
                )
                .to_string();
                let response_path = create_path_response
                    .unwrap()
                    .into_inner()
                    .path
                    .unwrap()
                    .path;

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
/// 2) Create reference of object in another collection with default subpath
/// 3) Create reference of object in another collection with custom subpath
/// 4) Update object and static reference older revision with duplicate subpath
#[ignore]
#[tokio::test]
#[serial(db)]
async fn create_object_path_with_reference_grpc_test() {
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
        creator_id: Some(user_id.clone()),
        ..Default::default()
    };
    let source_collection = common::functions::create_collection(collection_meta.clone());
    let target_collection = common::functions::create_collection(collection_meta.clone());

    // Reusable static path parts
    let static_path_part = format!(
        "s3://latest.{}.{}",
        target_collection.name, random_project.name
    );

    // Create random object with default subpath
    let object_meta = TCreateObject {
        creator_id: Some(user_id.clone()),
        collection_id: source_collection.id.to_string(),
        ..Default::default()
    };
    let random_object = common::functions::create_object(&object_meta);

    // Create object reference with default subpath in target collection
    let mut inner_create_reference_request = CreateObjectReferenceRequest {
        object_id: random_object.id.to_string(),
        collection_id: source_collection.id.to_string(),
        target_collection_id: target_collection.id.to_string(),
        writeable: true,
        auto_update: true,
        sub_path: "".to_string(),
    };
    let default_path_reference_request = common::grpc_helpers::add_token(
        tonic::Request::new(inner_create_reference_request.clone()),
        common::oidc::ADMINTOKEN,
    );
    object_service
        .create_object_reference(default_path_reference_request)
        .await
        .unwrap();

    // Get paths of target collection to validate default path of object
    let get_paths_request = common::grpc_helpers::add_token(
        tonic::Request::new(GetObjectPathsRequest {
            collection_id: target_collection.id.to_string(),
            include_inactive: true,
        }),
        common::oidc::ADMINTOKEN,
    );
    let target_collection_paths = object_service
        .get_object_paths(get_paths_request)
        .await
        .unwrap()
        .into_inner()
        .object_paths;

    assert_eq!(target_collection_paths.len(), 1); // Only default subpath of object
    assert_eq!(
        target_collection_paths.first().unwrap().path,
        format!("{static_path_part}/{}", random_object.filename)
    );

    // Update object and create static reference on revision 0 in same collection with default subpath
    common::functions::update_object(&TCreateUpdate {
        original_object: random_object.clone(),
        collection_id: source_collection.id.to_string(),
        ..Default::default()
    });

    inner_create_reference_request.object_id = random_object.id.to_string();
    inner_create_reference_request.auto_update = false;

    let default_path_reference_request = common::grpc_helpers::add_token(
        tonic::Request::new(inner_create_reference_request.clone()),
        common::oidc::ADMINTOKEN,
    );
    object_service
        .create_object_reference(default_path_reference_request)
        .await
        .unwrap();

    // Get paths of target collection to validate default path of object
    let get_paths_request = common::grpc_helpers::add_token(
        tonic::Request::new(GetObjectPathsRequest {
            collection_id: target_collection.id.to_string(),
            include_inactive: true,
        }),
        common::oidc::ADMINTOKEN,
    );
    let target_collection_paths = object_service
        .get_object_paths(get_paths_request)
        .await
        .unwrap()
        .into_inner()
        .object_paths;

    // Target collection should now have two references but still only the default path for the object
    assert_eq!(target_collection_paths.len(), 1); // Only default subpath of object
    assert_eq!(
        target_collection_paths.first().unwrap().path,
        format!("{static_path_part}/{}", random_object.filename)
    );

    // Remove object from target collection which includes removal of all its paths
    let delete_reference_request = common::grpc_helpers::add_token(
        tonic::Request::new(DeleteObjectRequest {
            object_id: random_object.id.to_string(),
            collection_id: target_collection.id.to_string(),
            with_revisions: false,
            force: false,
        }),
        common::oidc::ADMINTOKEN,
    );
    object_service
        .delete_object(delete_reference_request)
        .await
        .unwrap();

    // Create object reference with custom subpath in target collection
    inner_create_reference_request.sub_path = "custom/".to_string();
    let custom_path_reference_request = common::grpc_helpers::add_token(
        tonic::Request::new(inner_create_reference_request),
        common::oidc::ADMINTOKEN,
    );
    object_service
        .create_object_reference(custom_path_reference_request)
        .await
        .unwrap();

    // Get paths of target collection to validate default path of object
    let get_paths_request = common::grpc_helpers::add_token(
        tonic::Request::new(GetObjectPathsRequest {
            collection_id: target_collection.id.to_string(),
            include_inactive: true,
        }),
        common::oidc::ADMINTOKEN,
    );
    let target_collection_paths = object_service
        .get_object_paths(get_paths_request)
        .await
        .unwrap()
        .into_inner()
        .object_paths;

    assert_eq!(target_collection_paths.len(), 1); // Only custom subpath of object
    assert_eq!(
        target_collection_paths.first().unwrap().path,
        format!("{static_path_part}/custom/{}", random_object.filename)
    );
}

/// The individual steps of this test function contains:
/// 1) Creating an object with the default subpath
/// 2) Try creating some additional paths with varying invalid formats/characters
/// 3) Try creating some additional paths with varying valid formats/characters
/// 4) Get all paths of object and validate the successfully created
#[ignore]
#[tokio::test]
#[serial(db)]
async fn get_object_path_grpc_test() {
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

    // Reusable static path part
    let static_path_part = format!(
        "s3://latest.{}.{}",
        random_collection.name, random_project.name
    );

    // Create random object with default subpath
    let object_meta = TCreateObject {
        creator_id: Some(user_id.clone()),
        collection_id: random_collection.id.to_string(),
        ..Default::default()
    };
    let random_object = common::functions::create_object(&object_meta);

    let mut inner_create_path_request = CreateObjectPathRequest {
        collection_id: random_collection.id.to_string(),
        object_id: random_object.id.to_string(),
        sub_path: "".to_string(),
    };

    // Requests with invalid paths
    for invalid_path in vec![
        "/".to_string(),             // Empty path parts are not allowed
        "//".to_string(),            // Empty path parts are not allowed
        "path//".to_string(),        // Empty path parts are not allowed
        "/path//".to_string(),       // Empty path parts are not allowed
        "//path/".to_string(),       // Empty path parts are not allowed
        "//path//".to_string(),      // Empty path parts are not allowed
        "path//path".to_string(),    // Empty path parts are not allowed
        "LOUD/PATH/".to_string(),    // Only ^(/?[a-z0-9~\-]+)*/?$ allowed; no upper case characters
        "$%&/path/".to_string(),     // Only ^(/?[a-z0-9~\-]+)*/?$ allowed; no special characters
        "custom\\path/".to_string(), // Only ^(/?[a-z0-9~\-]+)*/?$ allowed; no backslashes
        "some path".to_string(),     // Only ^(/?[a-z0-9~\-]+)*/?$ allowed; no whitespaces
        "some|path".to_string(),     // Only ^(/?[a-z0-9~\-]+)*/?$ allowed; no pipes
        "some.path".to_string(),     // Only ^(/?[a-z0-9~\-]+)*/?$ allowed; no points
        "some,path".to_string(),     // Only ^(/?[a-z0-9~\-]+)*/?$ allowed; no commas
    ]
    .iter()
    {
        inner_create_path_request.sub_path = invalid_path.to_string();

        let create_path_request = common::grpc_helpers::add_token(
            tonic::Request::new(inner_create_path_request.clone()),
            common::oidc::ADMINTOKEN,
        );

        let create_path_response = object_service.create_object_path(create_path_request).await;

        if create_path_response.is_ok() {
            // Hint in the terminal output which custom path failed the test.
            println!("Wrongfully creation of path succeeded: {}", invalid_path);
        }

        assert!(create_path_response.is_err());
    }

    // Vector to save created paths for easier validation (already includes object default path)
    let mut fq_valid_paths = vec![format!("{static_path_part}/{}", random_object.filename)];

    // Requests with valid paths in different formats
    for valid_path in vec![
        "".to_string(), // Duplicate of existing default path are ignored
        "single-part".to_string(),
        "multi/part".to_string(),
        "/slash/front".to_string(),
        "slash/back/".to_string(),
        "/slash/both/".to_string(),
        "/my-custom/path/".to_string(),
        "my-custom-path/".to_string(),
        "my-custom-path/final".to_string(),
        "my-custom-path/final-really".to_string(),
        "my-custom-path/final-really/ver4".to_string(),
    ]
    .iter()
    {
        inner_create_path_request.sub_path = valid_path.to_string();
        let create_path_request = common::grpc_helpers::add_token(
            tonic::Request::new(inner_create_path_request.clone()),
            common::oidc::ADMINTOKEN,
        );
        let created_path = object_service
            .create_object_path(create_path_request)
            .await
            .unwrap()
            .into_inner()
            .path
            .unwrap();

        let fq_path = if valid_path.is_empty() {
            format!("{static_path_part}/{}", random_object.filename).to_string()
        } else if valid_path.starts_with('/') {
            if valid_path.ends_with('/') {
                format!("{static_path_part}{valid_path}{}", random_object.filename).to_string()
            } else {
                format!("{static_path_part}{valid_path}/{}", random_object.filename).to_string()
            }
        } else if valid_path.ends_with('/') {
            format!("{static_path_part}/{valid_path}{}", random_object.filename).to_string()
        } else {
            format!("{static_path_part}/{valid_path}/{}", random_object.filename).to_string()
        };

        assert_eq!(created_path.path, fq_path);

        fq_valid_paths.push(fq_path);
    }

    // Get all paths of object and validate the successfully created
    let get_object_path_request = common::grpc_helpers::add_token(
        tonic::Request::new(GetObjectPathRequest {
            collection_id: random_collection.id.to_string(),
            object_id: random_object.id.to_string(),
            include_inactive: true,
        }),
        common::oidc::ADMINTOKEN,
    );
    let get_object_path_response = object_service
        .get_object_path(get_object_path_request)
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        get_object_path_response.object_paths.len(),
        fq_valid_paths.len() - 1
    ); // 11 created and default ignored

    for proto_path in get_object_path_response.object_paths {
        assert!(fq_valid_paths.contains(&proto_path.path));
    }
}

/// The individual steps of this test function contains:
/// 1) Creating multiple objects with the default subpath and a custom subpath
/// 2) Get active/all object paths for validation
/// 3) Modify the visibility of some paths
/// 4) Get active/all object paths for validation
/// 5) Update objects and create subpaths for latest revisions
/// 6) Get active/all object paths for validation
#[ignore]
#[tokio::test]
#[serial(db)]
async fn get_object_paths_grpc_test() {
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
        creator_id: Some(user_id.clone()),
        ..Default::default()
    };
    let random_collection = common::functions::create_collection(collection_meta.clone());

    // Reusable static path part
    let static_path_part = format!(
        "s3://latest.{}.{}",
        random_collection.name, random_project.name
    );

    // Vector to save all created objects/paths for easier validation
    let mut created_objects = Vec::new();
    let mut created_paths = Vec::new();

    // Create multiple objects with a default and a custom path
    for _ in 0..3 {
        let random_object = common::functions::create_object(&TCreateObject {
            collection_id: random_collection.id.to_string(),
            creator_id: Some(user_id.clone()),
            ..Default::default()
        });

        // Add default path to vector
        created_paths.push(format!("{static_path_part}/{}", random_object.filename).to_string());

        // Create custom path
        let create_path_request = common::grpc_helpers::add_token(
            tonic::Request::new(CreateObjectPathRequest {
                collection_id: random_collection.id.to_string(),
                object_id: random_object.id.to_string(),
                sub_path: "custom/".to_string(),
            }),
            common::oidc::ADMINTOKEN,
        );

        created_paths.push(
            object_service
                .create_object_path(create_path_request)
                .await
                .unwrap()
                .into_inner()
                .path
                .unwrap()
                .path,
        );

        // Save object_id for later updates
        created_objects.push(random_object.id);
    }

    // Reusable inner request
    let mut inner_get_paths_request = GetObjectPathsRequest {
        collection_id: random_collection.id.to_string(),
        include_inactive: false,
    };

    // Get active/all paths for validation once for different permissions
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

        // Get only active paths of collection
        inner_get_paths_request.include_inactive = false;
        let get_active_paths_request = common::grpc_helpers::add_token(
            tonic::Request::new(inner_get_paths_request.clone()),
            common::oidc::REGULARTOKEN,
        );
        let get_active_paths_response = object_service
            .get_object_paths(get_active_paths_request)
            .await;

        // Get all paths of collection
        inner_get_paths_request.include_inactive = true;
        let get_all_paths_request = common::grpc_helpers::add_token(
            tonic::Request::new(inner_get_paths_request.clone()),
            common::oidc::REGULARTOKEN,
        );
        let get_all_paths_response = object_service.get_object_paths(get_all_paths_request).await;

        match *permission {
            Permission::None => {
                // Request should fail with insufficient permissions
                assert!(get_active_paths_response.is_err());
                assert!(get_all_paths_response.is_err())
            }
            Permission::Read | Permission::Append | Permission::Modify | Permission::Admin => {
                assert!(get_active_paths_response.is_ok());
                assert!(get_all_paths_response.is_ok());

                let active_paths = get_active_paths_response.unwrap().into_inner().object_paths;
                assert_eq!(active_paths.len(), 6); // 3 objects with default and custom path

                for active_path in active_paths {
                    assert!(created_paths.contains(&active_path.path)) // Created and received paths are equal
                }

                let all_paths = get_all_paths_response.unwrap().into_inner().object_paths;
                assert_eq!(all_paths.len(), 6); // 3 objects with default and custom path

                for path in all_paths {
                    assert!(created_paths.contains(&path.path)) // Created and received paths are equal
                }
            }
            _ => panic!("Unspecified permission is not allowed."),
        }
    }

    // Modify visibility of default paths
    let modified_paths = created_paths
        .clone()
        .into_iter()
        .filter(|path| !path.contains("custom/"))
        .collect::<Vec<_>>();

    for default_path in &modified_paths {
        let set_visibility_request = common::grpc_helpers::add_token(
            tonic::Request::new(SetObjectPathVisibilityRequest {
                collection_id: random_collection.id.to_string(),
                path: default_path.to_string(),
                visibility: false,
            }),
            common::oidc::ADMINTOKEN,
        );

        let set_visibility_response = object_service
            .set_object_path_visibility(set_visibility_request)
            .await;

        assert!(set_visibility_response.is_ok());
    }

    // Get active paths od collection for validation
    inner_get_paths_request.include_inactive = false;
    let get_active_paths_request = common::grpc_helpers::add_token(
        tonic::Request::new(inner_get_paths_request.clone()),
        common::oidc::ADMINTOKEN,
    );
    let active_paths = object_service
        .get_object_paths(get_active_paths_request)
        .await
        .unwrap()
        .into_inner()
        .object_paths;

    assert_eq!(active_paths.len(), 3); // Only three custom paths are still active
    for active_path in &active_paths {
        assert!(active_path.visibility);
        assert!(!modified_paths.contains(&active_path.path));
    }

    // Get all paths of collection for validation
    inner_get_paths_request.include_inactive = true;
    let get_active_paths_request = common::grpc_helpers::add_token(
        tonic::Request::new(inner_get_paths_request.clone()),
        common::oidc::ADMINTOKEN,
    );
    let all_paths = object_service
        .get_object_paths(get_active_paths_request)
        .await
        .unwrap()
        .into_inner()
        .object_paths;

    assert_eq!(all_paths.len(), 6); // 3 default paths and 3 custom paths
    for path in all_paths {
        if modified_paths.contains(&path.path) {
            assert!(!path.visibility);
        } else if created_paths.contains(&path.path) {
            assert!(path.visibility);
        } else {
            panic!("Path should not exist in this collection: {}", path.path);
        }
    }

    // Update objects and create custom paths again
    for object_id in created_objects {
        let source_object =
            common::functions::get_object(random_collection.id.to_string(), object_id.to_string());

        let updated_object = common::functions::update_object(&TCreateUpdate {
            original_object: source_object.clone(),
            collection_id: random_collection.id.to_string(),
            new_name: source_object.filename.to_string(),
            new_sub_path: Some("revision/".to_string()),
            ..Default::default()
        });

        // Save newly created revision sub paths
        created_paths
            .push(format!("{static_path_part}/revision/{}", updated_object.filename).to_string());
    }

    // Get active paths od collection for validation
    inner_get_paths_request.include_inactive = false;
    let get_active_paths_request = common::grpc_helpers::add_token(
        tonic::Request::new(inner_get_paths_request.clone()),
        common::oidc::ADMINTOKEN,
    );
    let active_paths = object_service
        .get_object_paths(get_active_paths_request)
        .await
        .unwrap()
        .into_inner()
        .object_paths;

    assert_eq!(active_paths.len(), 6); // 2 custom paths active per object
    for active_path in &active_paths {
        assert!(active_path.visibility);
        assert!(!modified_paths.contains(&active_path.path));
    }

    // Get all paths of collection for validation
    inner_get_paths_request.include_inactive = true;
    let get_all_paths_request = common::grpc_helpers::add_token(
        tonic::Request::new(inner_get_paths_request.clone()),
        common::oidc::ADMINTOKEN,
    );
    let all_paths = object_service
        .get_object_paths(get_all_paths_request)
        .await
        .unwrap()
        .into_inner()
        .object_paths;

    assert_eq!(all_paths.len(), 9); // 3 default paths and 6 custom paths
    for path in all_paths {
        if modified_paths.contains(&path.path) {
            assert!(!path.visibility);
        } else if created_paths.contains(&path.path) {
            assert!(path.visibility);
        } else {
            panic!("Path should not exist in this collection: {}", path.path);
        }
    }
}

/// The individual steps of this test function contains:
/// 1) Creating an object with the default subpath
/// 2) Creating some additional paths for the same object
/// 3) Modify visibility of some paths
/// 4) Get all paths of object
/// 5) Get all active paths of object
#[ignore]
#[tokio::test]
#[serial(db)]
async fn set_object_path_visibility_grpc_test() {
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

    // Reusable static path part
    let static_path_part = format!(
        "s3://latest.{}.{}",
        random_collection.name, random_project.name,
    );

    // Create random object with default subpath
    let object_meta = TCreateObject {
        creator_id: Some(user_id.clone()),
        collection_id: random_collection.id.to_string(),
        ..Default::default()
    };
    let random_object = common::functions::create_object(&object_meta);

    let mut inner_create_path_request = CreateObjectPathRequest {
        collection_id: random_collection.id.to_string(),
        object_id: random_object.id.to_string(),
        sub_path: "".to_string(),
    };

    for valid_path in vec![
        "path-01".to_string(),
        "path-02".to_string(),
        "path-03".to_string(),
        "path-04".to_string(),
        "path-05".to_string(),
    ]
    .iter()
    {
        inner_create_path_request.sub_path = valid_path.to_string();
        let create_path_request = common::grpc_helpers::add_token(
            tonic::Request::new(inner_create_path_request.clone()),
            common::oidc::ADMINTOKEN,
        );
        let create_path_response = object_service
            .create_object_path(create_path_request)
            .await
            .unwrap()
            .into_inner();

        let fq_path =
            format!("{static_path_part}/{valid_path}/{}", random_object.filename).to_string();
        assert_eq!(create_path_response.path.unwrap().path, fq_path);
    }

    // Set visibility of some paths to inactive
    let mut inner_set_visibility_request = SetObjectPathVisibilityRequest {
        collection_id: random_collection.id.to_string(),
        path: "".to_string(),
        visibility: false,
    };

    for set_visibility_path in vec![
        "path-02".to_string(),
        "path-03".to_string(),
        "path-04".to_string(),
    ]
    .iter()
    {
        let fq_path = format!(
            "{static_path_part}/{set_visibility_path}/{}",
            random_object.filename
        )
        .to_string();

        inner_set_visibility_request.path = fq_path;

        let set_visibility_request = common::grpc_helpers::add_token(
            tonic::Request::new(inner_set_visibility_request.clone()),
            common::oidc::ADMINTOKEN,
        );

        let set_visibility_response = object_service
            .set_object_path_visibility(set_visibility_request)
            .await
            .unwrap()
            .into_inner();

        let inactive_path = set_visibility_response.path.unwrap();
        let fq_path = format!(
            "{static_path_part}/{set_visibility_path}/{}",
            random_object.filename
        );

        assert_eq!(inactive_path.path, fq_path);
        assert!(!inactive_path.visibility);
    }

    // Set visibility of one path_03 to active again
    let fq_path = format!("{static_path_part}/path-03/{}", random_object.filename);
    inner_set_visibility_request.path = fq_path.to_string();
    inner_set_visibility_request.visibility = true;

    let set_visibility_request = common::grpc_helpers::add_token(
        tonic::Request::new(inner_set_visibility_request.clone()),
        common::oidc::ADMINTOKEN,
    );

    let set_visibility_response = object_service
        .set_object_path_visibility(set_visibility_request)
        .await
        .unwrap()
        .into_inner();

    let reactivated_path = set_visibility_response.path.unwrap();

    assert_eq!(reactivated_path.path, fq_path);
    assert!(reactivated_path.visibility);

    // Get all active paths of object
    let mut inner_get_paths_request = GetObjectPathsRequest {
        collection_id: random_collection.id.to_string(),
        include_inactive: false,
    };

    let get_paths_request = common::grpc_helpers::add_token(
        tonic::Request::new(inner_get_paths_request.clone()),
        common::oidc::ADMINTOKEN,
    );

    let get_paths_response = object_service
        .get_object_paths(get_paths_request)
        .await
        .unwrap()
        .into_inner();

    let active_paths = get_paths_response.object_paths;
    assert_eq!(active_paths.len(), 4); // Contains only the three active paths + the default

    for active_path in vec![
        "path-01".to_string(),
        "path-03".to_string(),
        "path-05".to_string(),
    ]
    .iter()
    {
        let proto_path = Path {
            path: format!(
                "{static_path_part}/{active_path}/{}",
                random_object.filename
            )
            .to_string(),
            visibility: true,
        };

        assert!(active_paths.contains(&proto_path));
    }

    // Get all paths of object and validate visibility
    inner_get_paths_request.include_inactive = true;
    let get_paths_request = common::grpc_helpers::add_token(
        tonic::Request::new(inner_get_paths_request.clone()),
        common::oidc::ADMINTOKEN,
    );

    let get_paths_response = object_service
        .get_object_paths(get_paths_request)
        .await
        .unwrap()
        .into_inner();

    let all_paths = get_paths_response.object_paths;
    for path in vec!["path-01", "path-02", "path-03", "path-04", "path-05"].iter() {
        let mut proto_path = Path {
            path: format!("{static_path_part}/{path}/{}", random_object.filename).to_string(),
            visibility: true,
        };

        proto_path.visibility = match *path {
            "path-01" | "path-03" | "path-05" => true,
            "path-02" | "path-04" => false,
            _ => panic!("Received sub path which should not exist."),
        };

        assert!(all_paths.contains(&proto_path));
    }
}

/// The individual steps of this test function contains:
/// 1) Creating an object with the default subpath and another custom path
/// 2) Get object via each path individually for different permissions
/// 3) Update the object to create another revision
/// 4) Create another custom sub path for the latest revision
/// 5) Get object via each path individually with/without revisions
#[ignore]
#[tokio::test]
#[serial(db)]
async fn get_object_by_path_grpc_test() {
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

    // Reusable static path part
    let static_path_part = format!(
        "s3://latest.{}.{}",
        random_collection.name, random_project.name,
    );

    // Create random object with default subpath
    let object_meta = TCreateObject {
        creator_id: Some(user_id.clone()),
        collection_id: random_collection.id.to_string(),
        ..Default::default()
    };
    let mut rev_0_object = common::functions::create_object(&object_meta);
    let rev_0_default_path = format!("{static_path_part}/{}", rev_0_object.filename).to_string();

    // Create additional custom sub path with the revision 0 object
    let create_path_request = common::grpc_helpers::add_token(
        tonic::Request::new(CreateObjectPathRequest {
            collection_id: random_collection.id.to_string(),
            object_id: rev_0_object.id.to_string(),
            sub_path: "rev-0/custom/".to_string(),
        }),
        common::oidc::ADMINTOKEN,
    );
    let rev_0_custom_path = object_service
        .create_object_path(create_path_request)
        .await
        .unwrap()
        .into_inner()
        .path
        .unwrap();

    // Get object through the default path and the additional custom path for different permissions
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

        // Get object through available paths
        for object_path in vec![
            rev_0_default_path.to_string(),
            rev_0_custom_path.path.to_string(),
        ]
        .iter()
        {
            let get_object_by_path_request = common::grpc_helpers::add_token(
                tonic::Request::new(GetObjectsByPathRequest {
                    collection_id: random_collection.id.to_string(),
                    path: object_path.to_string(),
                    with_revisions: false, // Also only one revision exists.
                }),
                common::oidc::REGULARTOKEN,
            );

            let get_object_by_path_response = object_service
                .get_objects_by_path(get_object_by_path_request)
                .await;

            match *permission {
                Permission::None => {
                    // Request should fail with insufficient permissions
                    assert!(get_object_by_path_response.is_err());
                }
                Permission::Read | Permission::Append | Permission::Modify | Permission::Admin => {
                    // Extract object from response
                    let proto_object =
                        get_object_by_path_response.unwrap().into_inner().object[0].clone();

                    assert_eq!(proto_object, rev_0_object);
                }
                _ => panic!("Unspecified permission is not allowed."),
            }
        }
    }

    // Update object and create another custom path with latest revision
    let rev_1_object = common::functions::update_object(&TCreateUpdate {
        original_object: rev_0_object.clone(),
        collection_id: random_collection.id.to_string(),
        new_name: rev_0_object.filename.to_string(), // Same filename, no custom sub path. Maybe just updated data.
        ..Default::default()
    });
    rev_0_object.latest = false; // Revision 1 is now latest

    // Create additional custom sub path with the revision 1 object
    let create_path_request = common::grpc_helpers::add_token(
        tonic::Request::new(CreateObjectPathRequest {
            collection_id: random_collection.id.to_string(),
            object_id: rev_1_object.id.to_string(),
            sub_path: "rev-1/custom/".to_string(),
        }),
        common::oidc::ADMINTOKEN,
    );
    let rev_1_custom_path = object_service
        .create_object_path(create_path_request)
        .await
        .unwrap()
        .into_inner()
        .path
        .unwrap();

    // Get latest object through available paths
    for object_path in vec![
        rev_0_default_path.to_string(),
        rev_0_custom_path.path.to_string(),
        rev_1_custom_path.path.to_string(),
    ]
    .iter()
    {
        let get_object_by_path_request = common::grpc_helpers::add_token(
            tonic::Request::new(GetObjectsByPathRequest {
                collection_id: random_collection.id.to_string(),
                path: object_path.to_string(),
                with_revisions: false,
            }),
            common::oidc::REGULARTOKEN,
        );

        // Extract object from response
        let proto_object = object_service
            .get_objects_by_path(get_object_by_path_request)
            .await
            .unwrap()
            .into_inner()
            .object[0]
            .clone();

        assert_eq!(proto_object, rev_1_object); // Objects should be equal and always latest revision
    }

    // Get all object revisions through available paths
    for object_path in vec![
        rev_0_default_path.to_string(),
        rev_0_custom_path.path.to_string(),
        rev_1_custom_path.path.to_string(),
    ]
    .iter()
    {
        let get_object_by_path_request = common::grpc_helpers::add_token(
            tonic::Request::new(GetObjectsByPathRequest {
                collection_id: random_collection.id.to_string(),
                path: object_path.to_string(),
                with_revisions: true,
            }),
            common::oidc::REGULARTOKEN,
        );

        // Extract objects from response
        let proto_objects = object_service
            .get_objects_by_path(get_object_by_path_request)
            .await
            .unwrap()
            .into_inner()
            .object;

        println!("{:#?}", proto_objects);

        assert_eq!(proto_objects.len(), 2); // Only contain revision 0 and 1
                                            //assert!(proto_objects.contains(&rev_0_object));
        assert!(proto_objects.contains(&rev_1_object));
    }
}
