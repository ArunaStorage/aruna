extern crate core;

use crate::common::functions::{TCreateCollection, TCreateObject};
use aruna_rust_api::api::storage::models::v1::collection_overview::Version::SemanticVersion;
use aruna_rust_api::api::storage::models::v1::{
    CollectionOverview, DataClass, KeyValue, LabelFilter, LabelOntology, LabelOrIdQuery,
    Permission, ProjectPermission, Version,
};
use aruna_rust_api::api::storage::services::v1::collection_service_server::CollectionService;
use aruna_rust_api::api::storage::services::v1::project_service_server::ProjectService;
use aruna_rust_api::api::storage::services::v1::user_service_server::UserService;
use aruna_rust_api::api::storage::services::v1::{
    AddUserToProjectRequest, CreateNewCollectionRequest, DeleteCollectionRequest,
    GetCollectionByIdRequest, GetCollectionsRequest, GetUserRequest, PinCollectionVersionRequest,
    UpdateCollectionRequest,
};
use aruna_server::{
    database::{self},
    server::services::authz::Authz,
    server::services::collection::CollectionServiceImpl,
    server::services::project::ProjectServiceImpl,
    server::services::user::UserServiceImpl,
};
use serial_test::serial;
use std::sync::Arc;

mod common;

#[ignore]
#[tokio::test]
#[serial(db)]
async fn create_collection_grpc_test() {
    // Init project service
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone()).await);
    let user_service = UserServiceImpl::new(db.clone(), authz.clone()).await;
    let project_service = ProjectServiceImpl::new(db.clone(), authz.clone()).await;
    let collection_service = CollectionServiceImpl::new(db, authz).await;

    // Fast track project creation
    let random_project = common::functions::create_project(None);

    // Create gRPC request to fetch user associated with token
    let get_user_request = common::grpc_helpers::add_token(
        tonic::Request::new(GetUserRequest {
            user_id: "".to_string(),
        }),
        common::oidc::REGULARTOKEN,
    );
    let get_user_response = user_service
        .get_user(get_user_request)
        .await
        .unwrap()
        .into_inner();
    let user_id = get_user_response.user.unwrap().id;

    // Add user with None permission to project
    let add_user_request = common::grpc_helpers::add_token(
        tonic::Request::new(AddUserToProjectRequest {
            project_id: random_project.id.to_string(),
            user_permission: Some(ProjectPermission {
                user_id: user_id.to_string(),
                project_id: random_project.id.to_string(),
                permission: Permission::None as i32,
                service_account: false,
            }),
        }),
        common::oidc::ADMINTOKEN,
    );
    let add_user_response = project_service.add_user_to_project(add_user_request).await;
    assert!(add_user_response.is_ok());

    // Create gRPC request for collection creation
    let mut create_collection_request = CreateNewCollectionRequest {
        name: "Test Collection".to_string(),
        description: "".to_string(),
        project_id: random_project.id.to_string(),
        labels: vec![KeyValue {
            key: "station_id".to_string(),
            value: "123456".to_string(),
        }],
        hooks: vec![KeyValue {
            key: "some_service".to_string(),
            value: "some_service_endpoint".to_string(),
        }],
        label_ontology: Some(LabelOntology {
            required_label_keys: vec!["sensor_id".to_string()],
        }),
        dataclass: DataClass::Private as i32,
    };

    // Loop all permissions and validate error/success
    for permission in vec![
        Permission::None,
        Permission::Read,
        Permission::Append,
        Permission::Modify,
        Permission::Admin,
    ]
    .iter()
    {
        // Update collection description
        create_collection_request.description = format!(
            "This collection was created with {}",
            Permission::as_str_name(permission)
        )
        .to_string();

        // Fast track permission edit
        assert!(common::functions::update_project_permission(
            random_project.id.as_str(),
            user_id.as_str(),
            *permission
        ));

        // Create gRPC request for collection creation
        let create_collection_grpc_request = common::grpc_helpers::add_token(
            tonic::Request::new(create_collection_request.clone()),
            common::oidc::REGULARTOKEN,
        );

        // Request with insufficient permissions should fail
        let create_collection_response = collection_service
            .create_new_collection(create_collection_grpc_request)
            .await;

        match *permission {
            Permission::None | Permission::Read | Permission::Append => {
                // Creation should fail with insufficient permissions
                assert!(create_collection_response.is_err());
            }
            Permission::Modify | Permission::Admin => {
                // Validate correct collection creation
                assert!(create_collection_response.is_ok());
                let collection_id = create_collection_response
                    .unwrap()
                    .into_inner()
                    .collection_id;

                let get_collection_request = common::grpc_helpers::add_token(
                    tonic::Request::new(GetCollectionByIdRequest {
                        collection_id: collection_id.to_string(),
                    }),
                    common::oidc::REGULARTOKEN,
                );
                let get_collection_response = collection_service
                    .get_collection_by_id(get_collection_request)
                    .await
                    .unwrap()
                    .into_inner();
                let collection = get_collection_response.collection.unwrap();

                assert_eq!(collection.id, collection_id);
                assert_eq!(collection.name, create_collection_request.name);
                assert_eq!(
                    collection.description,
                    create_collection_request.description
                );
                assert_eq!(collection.labels, create_collection_request.labels);
                assert_eq!(collection.hooks, create_collection_request.hooks);
                assert_eq!(
                    &collection.label_ontology.unwrap(),
                    create_collection_request.label_ontology.as_ref().unwrap()
                );
                assert!(!collection.is_public) // Not public.
            }
            _ => panic!("Unspecified permission is not allowed."),
        }
    }
}

#[ignore]
#[tokio::test]
#[serial(db)]
async fn get_collection_grpc_test() {
    // Init project service
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone()).await);
    let collection_service = CollectionServiceImpl::new(db, authz).await;

    // Fast track project creation
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

    // Fast track collection creation
    let random_collection = common::functions::create_collection(TCreateCollection {
        project_id: random_project.id.to_string(),
        num_labels: 0,
        num_hooks: 0,
        col_override: None,
        creator_id: Some(user_id.clone()),
    });

    // Get collection information with varying permissions
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

        // Create gRPC request to fetch collection information
        let get_collection_grpc_request = common::grpc_helpers::add_token(
            tonic::Request::new(GetCollectionByIdRequest {
                collection_id: random_collection.id.to_string(),
            }),
            common::oidc::REGULARTOKEN,
        );
        let get_collection_response = collection_service
            .get_collection_by_id(get_collection_grpc_request)
            .await;

        match *permission {
            Permission::None => {
                assert!(get_collection_response.is_err());
            }
            Permission::Read | Permission::Append | Permission::Modify | Permission::Admin => {
                assert!(get_collection_response.is_ok());

                // Validate fetched collection information
                let collection = get_collection_response
                    .unwrap()
                    .into_inner()
                    .collection
                    .unwrap();

                assert_eq!(collection.id, random_collection.id);
                assert_eq!(collection.name, random_collection.name);
                assert_eq!(collection.description, random_collection.description);
                assert_eq!(collection.labels, random_collection.labels);
                assert_eq!(collection.hooks, random_collection.hooks);
                assert_eq!(collection.label_ontology, random_collection.label_ontology);
                assert!(collection.is_public)
            }
            _ => panic!("Unspecified permission is not allowed."),
        }
    }
}

#[ignore]
#[tokio::test]
#[serial(db)]
async fn get_collections_grpc_test() {
    // Init project service
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone()).await);
    let collection_service = CollectionServiceImpl::new(db, authz).await;

    // Fast track project creation
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

    // Create multiple collections with labels for filtering
    let mut collection_ids: Vec<String> = Vec::new();

    // Loop with label vectors
    for (index, labels) in vec![
        vec![
            KeyValue {
                key: "station_id".to_string(),
                value: "12345".to_string(),
            },
            KeyValue {
                key: "deprecated".to_string(),
                value: "".to_string(),
            },
        ],
        vec![KeyValue {
            key: "station_id".to_string(),
            value: "12345".to_string(),
        }],
        vec![
            KeyValue {
                key: "station_id".to_string(),
                value: "23456".to_string(),
            },
            KeyValue {
                key: "deprecated".to_string(),
                value: "".to_string(),
            },
        ],
        vec![KeyValue {
            key: "station_id".to_string(),
            value: "23456".to_string(),
        }],
        vec![KeyValue {
            key: "station_id".to_string(),
            value: "34567".to_string(),
        }],
    ]
    .iter()
    .enumerate()
    {
        let create_collection_request = common::grpc_helpers::add_token(
            tonic::Request::new(CreateNewCollectionRequest {
                name: format!("Test Collection 00{}", index).to_string(),
                description: "Created for get_collections_grpc_test().".to_string(),
                project_id: random_project.id.to_string(),
                labels: labels.clone(),
                hooks: vec![],
                label_ontology: None,
                dataclass: DataClass::Private as i32,
            }),
            common::oidc::ADMINTOKEN,
        );

        let response = collection_service
            .create_new_collection(create_collection_request)
            .await
            .unwrap()
            .into_inner();

        collection_ids.push(response.collection_id);
    }

    // Request to get all collections
    let get_all_collections_request = common::grpc_helpers::add_token(
        tonic::Request::new(GetCollectionsRequest {
            project_id: random_project.id.to_string(),
            label_or_id_filter: None,
            page_request: None,
        }),
        common::oidc::ADMINTOKEN,
    );

    let get_all_collections_response = collection_service
        .get_collections(get_all_collections_request)
        .await
        .unwrap()
        .into_inner();
    let collections = get_all_collections_response.collections.unwrap();

    assert_eq!(collections.collection_overviews.len(), 5);

    // Request to filter collections by ID
    let id_filter_collections_request = common::grpc_helpers::add_token(
        tonic::Request::new(GetCollectionsRequest {
            project_id: random_project.id.to_string(),
            label_or_id_filter: Some(LabelOrIdQuery {
                labels: None,
                ids: collection_ids[0..3].to_vec(),
            }),
            page_request: None,
        }),
        common::oidc::ADMINTOKEN,
    );

    let id_filter_collections_response = collection_service
        .get_collections(id_filter_collections_request)
        .await
        .unwrap()
        .into_inner();
    let collections = id_filter_collections_response.collections.unwrap();

    assert_eq!(collections.collection_overviews.len(), 3);

    // Request to filter collections by label key+value
    let label_filter_collections_request = common::grpc_helpers::add_token(
        tonic::Request::new(GetCollectionsRequest {
            project_id: random_project.id.to_string(),
            label_or_id_filter: Some(LabelOrIdQuery {
                labels: Some(LabelFilter {
                    labels: vec![KeyValue {
                        key: "station_id".to_string(),
                        value: "12345".to_string(),
                    }],
                    and_or_or: false,
                    keys_only: false,
                }),
                ids: vec![],
            }),
            page_request: None,
        }),
        common::oidc::ADMINTOKEN,
    );

    let label_filter_collections_response = collection_service
        .get_collections(label_filter_collections_request)
        .await
        .unwrap()
        .into_inner();
    let collections = label_filter_collections_response.collections.unwrap();

    assert_eq!(collections.collection_overviews.len(), 2);

    // Request to filter collections by label key only
    let label_filter_collections_request = common::grpc_helpers::add_token(
        tonic::Request::new(GetCollectionsRequest {
            project_id: random_project.id.to_string(),
            label_or_id_filter: Some(LabelOrIdQuery {
                labels: Some(LabelFilter {
                    labels: vec![KeyValue {
                        key: "deprecated".to_string(),
                        value: "".to_string(),
                    }],
                    and_or_or: false,
                    keys_only: true,
                }),
                ids: vec![],
            }),
            page_request: None,
        }),
        common::oidc::ADMINTOKEN,
    );

    let label_filter_collections_response = collection_service
        .get_collections(label_filter_collections_request)
        .await
        .unwrap()
        .into_inner();
    let collections = label_filter_collections_response.collections.unwrap();

    assert_eq!(collections.collection_overviews.len(), 2);

    // Loop over permissions to test authorization
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

        // Request to get all collections
        let get_all_collections_request = common::grpc_helpers::add_token(
            tonic::Request::new(GetCollectionsRequest {
                project_id: random_project.id.to_string(),
                label_or_id_filter: None,
                page_request: None,
            }),
            common::oidc::REGULARTOKEN,
        );

        // Try to get unfiltered collections of project
        let get_collections_response = collection_service
            .get_collections(get_all_collections_request)
            .await;

        match *permission {
            Permission::None => {
                assert!(get_collections_response.is_err());
            }
            Permission::Read | Permission::Append | Permission::Modify | Permission::Admin => {
                assert!(get_collections_response.is_ok());

                // Validate fetched collection information
                let collections = get_collections_response
                    .unwrap()
                    .into_inner()
                    .collections
                    .unwrap();

                assert_eq!(collections.collection_overviews.len(), 5);
            }
            _ => panic!("Unspecified permission is not allowed."),
        }
    }

    // Get single collection and validate fetched fields
    let get_single_collections_request = common::grpc_helpers::add_token(
        tonic::Request::new(GetCollectionsRequest {
            project_id: random_project.id.to_string(),
            label_or_id_filter: Some(LabelOrIdQuery {
                labels: None,
                ids: collection_ids[0..1].to_vec(),
            }),
            page_request: None,
        }),
        common::oidc::ADMINTOKEN,
    );

    let get_single_collections_response = collection_service
        .get_collections(get_single_collections_request)
        .await
        .unwrap()
        .into_inner();
    let collections = get_single_collections_response
        .collections
        .unwrap()
        .collection_overviews;

    assert_eq!(collections.len(), 1);
    assert_eq!(collections[0].name, "Test Collection 000".to_string());
    assert_eq!(
        collections[0].description,
        "Created for get_collections_grpc_test().".to_string()
    );
    assert_eq!(collections[0].labels.len(), 2);
    assert!(collections[0].labels.contains(&KeyValue {
        key: "station_id".to_string(),
        value: "12345".to_string(),
    }));
    assert!(collections[0].labels.contains(&KeyValue {
        key: "deprecated".to_string(),
        value: "".to_string(),
    }));
    assert_eq!(collections[0].hooks, vec![]);
    assert_eq!(
        collections[0].label_ontology,
        Some(LabelOntology {
            required_label_keys: vec![]
        })
    );
    assert!(!collections[0].is_public)
}

#[ignore]
#[tokio::test]
#[serial(db)]
async fn update_collection_grpc_test() {
    // Init project service
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone()).await);
    let collection_service = CollectionServiceImpl::new(db, authz).await;

    // Fast track project creation
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

    // Fast track collection creation
    let random_collection = common::functions::create_collection(TCreateCollection {
        project_id: random_project.id.to_string(),
        num_labels: 0,
        num_hooks: 0,
        col_override: None,
        creator_id: Some(user_id.clone()),
    });

    // Update collection information with varying permissions
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

        // Create gRPC request to update collection
        let update_collection_grpc_request = common::grpc_helpers::add_token(
            tonic::Request::new(UpdateCollectionRequest {
                collection_id: random_collection.id.to_string(),
                name: "Test Collection".to_string(),
                description: format!(
                    "Collection updated with permission {}",
                    permission.as_str_name().to_string()
                ),
                labels: vec![KeyValue {
                    key: "permission".to_string(),
                    value: permission.as_str_name().to_string(),
                }],
                hooks: vec![],
                label_ontology: None,
                dataclass: DataClass::Private as i32,
                version: None,
            }),
            common::oidc::REGULARTOKEN,
        );
        let update_collection_response = collection_service
            .update_collection(update_collection_grpc_request)
            .await;

        match *permission {
            Permission::None | Permission::Read | Permission::Append => {
                // Request should fail with insufficient permissions
                assert!(update_collection_response.is_err());
            }
            Permission::Modify | Permission::Admin => {
                assert!(update_collection_response.is_ok());

                // Validate fetched collection information
                let collection = update_collection_response
                    .unwrap()
                    .into_inner()
                    .collection
                    .unwrap();

                assert_eq!(collection.id, random_collection.id);
                assert_eq!(collection.name, "Test Collection".to_string());
                assert_eq!(
                    collection.description,
                    format!(
                        "Collection updated with permission {}",
                        permission.as_str_name().to_string()
                    )
                );
                assert_eq!(
                    collection.labels,
                    vec![KeyValue {
                        key: "permission".to_string(),
                        value: permission.as_str_name().to_string()
                    }]
                );
                assert_eq!(collection.hooks, vec![]);
                assert_eq!(
                    collection.label_ontology,
                    Some(LabelOntology {
                        required_label_keys: vec![]
                    })
                );
                assert!(collection.is_public)
            }
            _ => panic!("Unspecified permission is not allowed."),
        }
    }

    // Update collection with version --> pinned collection
    let pin_update_collection_request = common::grpc_helpers::add_token(
        tonic::Request::new(UpdateCollectionRequest {
            collection_id: random_collection.id.to_string(),
            name: "Test Collection".to_string(),
            description: "Collection updated with version 1.2.3.".to_string(),
            labels: vec![KeyValue {
                key: "is_versioned".to_string(),
                value: "true".to_string(),
            }],
            hooks: vec![],
            label_ontology: None,
            dataclass: DataClass::Public as i32,
            version: Some(Version {
                major: 1,
                minor: 2,
                patch: 3,
            }),
        }),
        common::oidc::REGULARTOKEN, // Project Admin at this point
    );
    let pin_update_collection_response = collection_service
        .update_collection(pin_update_collection_request)
        .await
        .unwrap()
        .into_inner();

    let versioned_collection = pin_update_collection_response.collection.unwrap();

    assert_ne!(versioned_collection.id, random_collection.id); // Versioned collection is deep clone with individual id
    assert_eq!(versioned_collection.name, "Test Collection".to_string());
    assert_eq!(
        versioned_collection.description,
        "Collection updated with version 1.2.3.".to_string()
    );
    assert_eq!(
        versioned_collection.labels,
        vec![KeyValue {
            key: "is_versioned".to_string(),
            value: "true".to_string(),
        }]
    );
    assert_eq!(versioned_collection.hooks, vec![]);
    assert_eq!(
        versioned_collection.version,
        Some(SemanticVersion(Version {
            major: 1,
            minor: 2,
            patch: 3
        }))
    );
    assert!(versioned_collection.is_public);

    // Update versioned/pinned collection without version --> Error
    let update_versioned_collection_request = common::grpc_helpers::add_token(
        tonic::Request::new(UpdateCollectionRequest {
            collection_id: versioned_collection.id.to_string(),
            name: "Test Collection".to_string(),
            description: "Versioned collection updated without version.".to_string(),
            labels: vec![KeyValue {
                key: "is_versioned".to_string(),
                value: "false".to_string(),
            }],
            hooks: vec![],
            label_ontology: None,
            dataclass: DataClass::Private as i32,
            version: None,
        }),
        common::oidc::REGULARTOKEN, // Project Admin at this point
    );
    let update_versioned_collection_response = collection_service
        .update_collection(update_versioned_collection_request)
        .await;

    assert!(update_versioned_collection_response.is_err());

    // Update versioned/pinned collection with lesser version --> Error
    let update_versioned_collection_request = common::grpc_helpers::add_token(
        tonic::Request::new(UpdateCollectionRequest {
            collection_id: versioned_collection.id.to_string(),
            name: "Test Collection".to_string(),
            description: "Versioned collection updated without version.".to_string(),
            labels: vec![KeyValue {
                key: "is_versioned".to_string(),
                value: "true".to_string(),
            }],
            hooks: vec![],
            label_ontology: None,
            dataclass: DataClass::Private as i32,
            version: Some(Version {
                major: 1,
                minor: 0,
                patch: 0,
            }),
        }),
        common::oidc::REGULARTOKEN, // Project Admin at this point
    );
    let update_versioned_collection_response = collection_service
        .update_collection(update_versioned_collection_request)
        .await;

    assert!(update_versioned_collection_response.is_err());
}

#[ignore]
#[tokio::test]
#[serial(db)]
async fn pin_collection_grpc_test() {
    // Init project service
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone()).await);
    let collection_service = CollectionServiceImpl::new(db, authz).await;

    // Fast track project creation
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

    // Create and get collection
    let create_collection_request = common::grpc_helpers::add_token(
        tonic::Request::new(CreateNewCollectionRequest {
            name: "pin_collection_grpc_test()".to_string(),
            description: "Some description.".to_string(),
            project_id: random_project.id.to_string(),
            labels: vec![],
            hooks: vec![],
            label_ontology: None,
            dataclass: DataClass::Private as i32,
        }),
        common::oidc::ADMINTOKEN,
    );
    let create_collection_response = collection_service
        .create_new_collection(create_collection_request)
        .await
        .unwrap()
        .into_inner();

    let get_collection_request = common::grpc_helpers::add_token(
        tonic::Request::new(GetCollectionByIdRequest {
            collection_id: create_collection_response.collection_id.to_string(),
        }),
        common::oidc::ADMINTOKEN,
    );

    let get_collection_response = collection_service
        .get_collection_by_id(get_collection_request)
        .await
        .unwrap()
        .into_inner();
    let random_collection = get_collection_response.collection.unwrap();

    // Try to pin collection without collection_id --> Error
    let pin_collection_grpc_request = common::grpc_helpers::add_token(
        tonic::Request::new(PinCollectionVersionRequest {
            collection_id: "".to_string(),
            version: Some(Version {
                major: 1,
                minor: 0,
                patch: 0,
            }),
        }),
        common::oidc::REGULARTOKEN,
    );
    let pin_collection_response = collection_service
        .pin_collection_version(pin_collection_grpc_request)
        .await;

    assert!(pin_collection_response.is_err());

    // Try to pin collection without version --> Error
    let pin_collection_grpc_request = common::grpc_helpers::add_token(
        tonic::Request::new(PinCollectionVersionRequest {
            collection_id: random_collection.id.to_string(),
            version: None,
        }),
        common::oidc::REGULARTOKEN,
    );
    let pin_collection_response = collection_service
        .pin_collection_version(pin_collection_grpc_request)
        .await;

    assert!(pin_collection_response.is_err());

    let mut versioned_collection_option: Option<CollectionOverview> = None;
    // Pin collection with varying permissions
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

        // Create gRPC request to pin collection
        let pin_collection_grpc_request = common::grpc_helpers::add_token(
            tonic::Request::new(PinCollectionVersionRequest {
                collection_id: random_collection.id.to_string(),
                version: Some(Version {
                    major: 3,
                    minor: 2,
                    patch: 1,
                }),
            }),
            common::oidc::REGULARTOKEN,
        );
        let pin_collection_response = collection_service
            .pin_collection_version(pin_collection_grpc_request)
            .await;

        match *permission {
            Permission::None | Permission::Read | Permission::Append => {
                // Request should fail with insufficient permissions
                assert!(pin_collection_response.is_err());
            }
            Permission::Modify | Permission::Admin => {
                assert!(pin_collection_response.is_ok());

                // Validate collection information returned from pin
                let collection = pin_collection_response
                    .unwrap()
                    .into_inner()
                    .collection
                    .unwrap();

                assert_ne!(collection.id, random_collection.id);
                assert_eq!(collection.name, random_collection.name);
                assert_eq!(collection.description, random_collection.description);
                assert_eq!(collection.labels, random_collection.labels);
                assert_eq!(collection.hooks, random_collection.hooks);
                assert_eq!(collection.label_ontology, random_collection.label_ontology);
                assert!(!collection.is_public);

                versioned_collection_option = Some(collection);
            }
            _ => panic!("Unspecified permission is not allowed."),
        }
    }

    // Check if pinned version is available after permission loop
    if let Some(versioned_collection) = versioned_collection_option {
        // Try to pin versioned collection without version --> Error
        let pin_collection_grpc_request = common::grpc_helpers::add_token(
            tonic::Request::new(PinCollectionVersionRequest {
                collection_id: versioned_collection.id.to_string(),
                version: None,
            }),
            common::oidc::REGULARTOKEN, // At this point has project ADMIN permissions
        );
        let pin_collection_response = collection_service
            .pin_collection_version(pin_collection_grpc_request)
            .await;

        assert!(pin_collection_response.is_err());

        // Try to pin versioned collection with lesser version --> Error
        let pin_collection_grpc_request = common::grpc_helpers::add_token(
            tonic::Request::new(PinCollectionVersionRequest {
                collection_id: versioned_collection.id.to_string(),
                version: Some(Version {
                    major: 1,
                    minor: 2,
                    patch: 3,
                }),
            }),
            common::oidc::REGULARTOKEN, // At this point has project ADMIN permissions
        );
        let pin_collection_response = collection_service
            .pin_collection_version(pin_collection_grpc_request)
            .await;

        assert!(pin_collection_response.is_err());

        // Try to pin versioned collection with same version --> Error
        let pin_collection_grpc_request = common::grpc_helpers::add_token(
            tonic::Request::new(PinCollectionVersionRequest {
                collection_id: versioned_collection.id.to_string(),
                version: Some(Version {
                    major: 1,
                    minor: 2,
                    patch: 3,
                }),
            }),
            common::oidc::REGULARTOKEN, // At this point has project ADMIN permissions
        );
        let pin_collection_response = collection_service
            .pin_collection_version(pin_collection_grpc_request)
            .await;

        assert!(pin_collection_response.is_err());

        // Try to pin versioned collection with greater version --> Success
        let pin_collection_grpc_request = common::grpc_helpers::add_token(
            tonic::Request::new(PinCollectionVersionRequest {
                collection_id: versioned_collection.id.to_string(),
                version: Some(Version {
                    major: 4,
                    minor: 0,
                    patch: 0,
                }),
            }),
            common::oidc::REGULARTOKEN, // At this point has project ADMIN permissions
        );
        let pin_collection_response = collection_service
            .pin_collection_version(pin_collection_grpc_request)
            .await;

        assert!(pin_collection_response.is_ok());

        let ultra_versioned_collection = pin_collection_response
            .unwrap()
            .into_inner()
            .collection
            .unwrap();

        assert_ne!(ultra_versioned_collection.id, versioned_collection.id); // Again created a clone of the collection
    } else {
        panic!("No versioned collection available after collection pin.")
    }
}

#[ignore]
#[tokio::test]
#[serial(db)]
async fn delete_collection_grpc_test() {
    // Init project service
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone()).await);
    let collection_service = CollectionServiceImpl::new(db, authz).await;

    // Fast track project creation
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

    // Try delete non-existing collection --> Error
    let delete_collection_request = common::grpc_helpers::add_token(
        tonic::Request::new(DeleteCollectionRequest {
            collection_id: "".to_string(),
            force: false,
        }),
        common::oidc::ADMINTOKEN,
    );
    let delete_collection_response = collection_service
        .delete_collection(delete_collection_request)
        .await;

    assert!(delete_collection_response.is_err());

    // Try to delete collection with varying permissions
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

        let random_collection = common::functions::create_collection(TCreateCollection {
            project_id: random_project.id.to_string(),
            num_labels: 0,
            num_hooks: 0,
            col_override: None,
            creator_id: Some(user_id.to_string()),
        });

        // Create gRPC request to delete collection
        let delete_collection_request = common::grpc_helpers::add_token(
            tonic::Request::new(DeleteCollectionRequest {
                collection_id: random_collection.id.to_string(),
                force: false,
            }),
            common::oidc::REGULARTOKEN,
        );
        let delete_collection_response = collection_service
            .delete_collection(delete_collection_request)
            .await;

        match *permission {
            Permission::None | Permission::Read | Permission::Append => {
                if delete_collection_response.is_ok() {
                    println!("Permission {:#?}", permission);
                    println!("{:#?}", delete_collection_response);
                }

                // Request should fail with insufficient permissions
                assert!(delete_collection_response.is_err());
            }
            Permission::Modify | Permission::Admin => {
                assert!(delete_collection_response.is_ok());

                let get_collection_request = common::grpc_helpers::add_token(
                    tonic::Request::new(GetCollectionByIdRequest {
                        collection_id: random_collection.id.to_string(),
                    }),
                    common::oidc::REGULARTOKEN,
                );
                let get_collection_response = collection_service
                    .get_collection_by_id(get_collection_request)
                    .await;

                assert!(get_collection_response.is_err());
            }
            _ => panic!("Unspecified permission is not allowed."),
        }
    }

    // Try to delete non-empty collection --> Error
    //   - Create collection
    //   - Create object in collection
    //   - Delete collection --> Error
    let random_collection = common::functions::create_collection(TCreateCollection {
        project_id: random_project.id.to_string(),
        num_labels: 0,
        num_hooks: 0,
        col_override: None,
        creator_id: Some(user_id.to_string()),
    });

    let random_object = common::functions::create_object(&TCreateObject {
        creator_id: Some(user_id.to_string()),
        collection_id: random_collection.id.to_string(),
        default_endpoint_id: None,
        num_labels: 0,
        num_hooks: 0,
    });

    let delete_collection_request = common::grpc_helpers::add_token(
        tonic::Request::new(DeleteCollectionRequest {
            collection_id: random_collection.id.to_string(),
            force: false,
        }),
        common::oidc::REGULARTOKEN,
    );
    let delete_collection_response = collection_service
        .delete_collection(delete_collection_request)
        .await;

    assert!(delete_collection_response.is_err());

    // Try delete non-empty collection with force
    //   - Delete collection with force --> Success
    //   - Validate deletion of collection and object
    let delete_collection_request = common::grpc_helpers::add_token(
        tonic::Request::new(DeleteCollectionRequest {
            collection_id: random_collection.id.to_string(),
            force: true,
        }),
        common::oidc::REGULARTOKEN,
    );
    let delete_collection_response = collection_service
        .delete_collection(delete_collection_request)
        .await;

    assert!(delete_collection_response.is_ok());
    assert!(common::grpc_helpers::try_get_collection(
        random_collection.id.to_string(),
        common::oidc::REGULARTOKEN
    )
    .await
    .is_none());
    assert!(common::grpc_helpers::try_get_object(
        random_collection.id,
        random_object.id,
        common::oidc::REGULARTOKEN
    )
    .await
    .is_none())
}
