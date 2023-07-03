use crate::common::functions::{TCreateCollection, TCreateObject};
use crate::common::grpc_helpers::get_token_user_id;

use aruna_rust_api::api::storage::models::v1::{KeyValue, LabelOrIdQuery, Permission};
use aruna_rust_api::api::storage::services::v1::object_group_service_server::ObjectGroupService;
use aruna_rust_api::api::storage::services::v1::{
    AddLabelsToObjectGroupRequest, CreateObjectGroupRequest, DeleteObjectGroupRequest,
    GetObjectGroupByIdRequest, GetObjectGroupHistoryRequest, GetObjectGroupObjectsRequest,
    GetObjectGroupsFromObjectRequest, GetObjectGroupsRequest, UpdateObjectGroupRequest,
};

use aruna_server::config::ArunaServerConfig;
use aruna_server::server::services::objectgroup::ObjectGroupServiceImpl;
use aruna_server::{
    database::{self},
    server::services::authz::Authz,
};

use rand::seq::SliceRandom;
use serial_test::serial;
use std::sync::Arc;

mod common;

/// The individual steps of this test function contains:
/// 1. Create object group with data objects only with different permissions
/// 2. Create object group with meta objects only with different permissions
/// 3. Create object group with data and meta objects with different permissions
#[ignore]
#[tokio::test]
#[serial(db)]
async fn create_object_group_grpc_test() {
    // Init database connection
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone(), ArunaServerConfig::default()).await);

    // Init object group service
    let object_group_service = ObjectGroupServiceImpl::new(db.clone(), authz).await;

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
    let random_collection = common::functions::create_collection(TCreateCollection {
        project_id: random_project.id.to_string(),
        creator_id: Some(user_id.clone()),
        ..Default::default()
    });

    // Create random data and meta objects
    let object_meta = TCreateObject {
        creator_id: Some(user_id.to_string()),
        collection_id: random_collection.id.to_string(),
        ..Default::default()
    };
    let data_object_ids = (0..5)
        .map(|_| common::functions::create_object(&object_meta).id)
        .collect::<Vec<_>>();
    let meta_object_ids = (0..5)
        .map(|_| common::functions::create_object(&object_meta).id)
        .collect::<Vec<_>>();

    // Try to create object group with data objects only with different permissions
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

        // Create object group with data objects only
        let create_data_object_group_request = common::grpc_helpers::add_token(
            tonic::Request::new(CreateObjectGroupRequest {
                name: "Data only group".to_string(),
                description: format!("{:#?}", permission),
                collection_id: random_collection.id.to_string(),
                object_ids: data_object_ids.clone(),
                meta_object_ids: vec![],
                labels: vec![],
                hooks: vec![],
            }),
            common::oidc::REGULARTOKEN,
        );

        // Create object group with data objects only
        let create_meta_object_group_request = common::grpc_helpers::add_token(
            tonic::Request::new(CreateObjectGroupRequest {
                name: "Data only group".to_string(),
                description: format!("{:#?}", permission),
                collection_id: random_collection.id.to_string(),
                object_ids: vec![],
                meta_object_ids: meta_object_ids.clone(),
                labels: vec![],
                hooks: vec![],
            }),
            common::oidc::REGULARTOKEN,
        );

        // Create object group with data objects only
        let create_object_group_request = common::grpc_helpers::add_token(
            tonic::Request::new(CreateObjectGroupRequest {
                name: "Data only group".to_string(),
                description: format!("{:#?}", permission),
                collection_id: random_collection.id.to_string(),
                object_ids: data_object_ids.clone(),
                meta_object_ids: meta_object_ids.clone(),
                labels: vec![],
                hooks: vec![],
            }),
            common::oidc::REGULARTOKEN,
        );

        let create_data_object_group_response = object_group_service
            .create_object_group(create_data_object_group_request)
            .await;
        let create_meta_object_group_response = object_group_service
            .create_object_group(create_meta_object_group_request)
            .await;
        let create_object_group_response = object_group_service
            .create_object_group(create_object_group_request)
            .await;

        // Check if request succeeded for specific permission
        match *permission {
            Permission::None | Permission::Read => {
                assert!(create_data_object_group_response.is_err());
                assert!(create_meta_object_group_response.is_err());
                assert!(create_object_group_response.is_err());
            }
            Permission::Append | Permission::Modify | Permission::Admin => {
                // Validate object group creation
                for response in vec![
                    create_data_object_group_response,
                    create_meta_object_group_response,
                    create_object_group_response,
                ]
                .into_iter()
                {
                    let object_group = response.unwrap().into_inner().object_group.unwrap();

                    assert!(!object_group.id.is_empty());
                    assert_eq!(object_group.rev_number, 0);
                    assert_eq!(object_group.name, "Data only group".to_string());
                    assert_eq!(object_group.description, format!("{:#?}", permission));

                    let db_object_group = common::functions::get_raw_db_object_group(
                        &object_group.id,
                        &random_collection.id,
                    );

                    assert_eq!(object_group.id, db_object_group.id);
                    assert_eq!(object_group.name, db_object_group.name);
                    assert_eq!(object_group.description, db_object_group.description);
                    assert_eq!(object_group.rev_number, db_object_group.rev_number);
                    assert_eq!(object_group.hooks, db_object_group.hooks);
                    assert_eq!(object_group.labels, db_object_group.labels);
                }
            }
            _ => panic!("Unspecified permission is not allowed."),
        }
    }
}

/// The individual steps of this test function contains:
/// 1. Update object group metadata/objects with different permissions
#[ignore]
#[tokio::test]
#[serial(db)]
async fn update_object_group_grpc_test() {
    // Init database connection
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone(), ArunaServerConfig::default()).await);

    // Init object group service
    let object_group_service = ObjectGroupServiceImpl::new(db.clone(), authz).await;

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
    let random_collection = common::functions::create_collection(TCreateCollection {
        project_id: random_project.id.to_string(),
        creator_id: Some(user_id.clone()),
        ..Default::default()
    });

    // Create random data and meta objects
    let object_meta = TCreateObject {
        creator_id: Some(user_id.to_string()),
        collection_id: random_collection.id.to_string(),
        ..Default::default()
    };
    let data_object_ids = (0..5)
        .map(|_| common::functions::create_object(&object_meta).id)
        .collect::<Vec<_>>();

    let meta_object_ids = (0..5)
        .map(|_| common::functions::create_object(&object_meta).id)
        .collect::<Vec<_>>();

    // Try to update object group with with different permissions
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

        // Create object group which will be updated
        let create_object_group_request = common::grpc_helpers::add_token(
            tonic::Request::new(CreateObjectGroupRequest {
                name: "Source object group".to_string(),
                description: "Dummy object group created in update_object_group_grpc_test."
                    .to_string(),
                collection_id: random_collection.id.to_string(),
                object_ids: data_object_ids.clone(),
                meta_object_ids: meta_object_ids.clone(),
                labels: vec![],
                hooks: vec![],
            }),
            common::oidc::ADMINTOKEN,
        );

        let source_object_group = object_group_service
            .create_object_group(create_object_group_request)
            .await
            .unwrap()
            .into_inner()
            .object_group
            .unwrap();

        // Update object group:
        //  - Modify object group metadata
        //  - Add/Remove objects
        let modified_data_object_ids: Vec<_> = data_object_ids
            .choose_multiple(&mut rand::thread_rng(), 3)
            .collect::<Vec<_>>()
            .iter()
            .map(|sample| (*sample).to_string())
            .collect();
        let modified_meta_object_ids: Vec<_> = meta_object_ids
            .choose_multiple(&mut rand::thread_rng(), 3)
            .collect::<Vec<_>>()
            .iter()
            .map(|sample| (*sample).to_string())
            .collect();

        let update_object_group_request = common::grpc_helpers::add_token(
            tonic::Request::new(UpdateObjectGroupRequest {
                group_id: source_object_group.id.to_string(),
                name: format!("Updated with {:#?}", permission),
                description: "Dummy object group updated in update_object_group_grpc_test."
                    .to_string(),
                collection_id: random_collection.id.to_string(),
                object_ids: modified_data_object_ids.clone(),
                meta_object_ids: modified_meta_object_ids.clone(),
                labels: vec![],
                hooks: vec![],
            }),
            common::oidc::REGULARTOKEN,
        );

        let update_object_group_response = object_group_service
            .update_object_group(update_object_group_request)
            .await;

        // Check if request succeeded for specific permission
        match *permission {
            Permission::None | Permission::Read => {
                assert!(update_object_group_response.is_err());
            }
            Permission::Append | Permission::Modify | Permission::Admin => {
                // Validate object group update
                let updated_object_group = update_object_group_response
                    .unwrap()
                    .into_inner()
                    .object_group
                    .unwrap();

                assert_ne!(updated_object_group.id, source_object_group.id);
                assert_eq!(
                    updated_object_group.name,
                    format!("Updated with {:#?}", permission)
                );
                assert_eq!(updated_object_group.rev_number, 1);

                let object_group_objects = db
                    .get_object_group_objects(GetObjectGroupObjectsRequest {
                        collection_id: random_collection.id.to_string(),
                        group_id: updated_object_group.id.to_string(),
                        page_request: None,
                        meta_only: false,
                    })
                    .unwrap()
                    .object_group_objects;

                assert_eq!(object_group_objects.len(), 6); // 3 data and 3 meta objects

                for object_group_object in object_group_objects {
                    match object_group_object.is_metadata {
                        true => assert!(modified_meta_object_ids
                            .contains(&object_group_object.object.unwrap().id)),
                        false => assert!(modified_data_object_ids
                            .contains(&object_group_object.object.unwrap().id)),
                    }
                }
            }
            _ => panic!("Unspecified permission is not allowed."),
        }
    }
}

/// The individual steps of this test function contains:
/// 1. Get object group by its id with different permissions
#[ignore]
#[tokio::test]
#[serial(db)]
async fn get_object_group_by_id_grpc_test() {
    // Init database connection
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone(), ArunaServerConfig::default()).await);

    // Init object group service
    let object_group_service = ObjectGroupServiceImpl::new(db.clone(), authz).await;

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
    let random_collection = common::functions::create_collection(TCreateCollection {
        project_id: random_project.id.to_string(),
        creator_id: Some(user_id.clone()),
        ..Default::default()
    });

    // Create random data and meta objects
    let object_meta = TCreateObject {
        creator_id: Some(user_id.to_string()),
        collection_id: random_collection.id.to_string(),
        ..Default::default()
    };

    let data_object_ids = vec![common::functions::create_object(&object_meta).id];
    let meta_object_ids = vec![common::functions::create_object(&object_meta).id];

    // Create object group which will be updated
    let create_object_group_request = common::grpc_helpers::add_token(
        tonic::Request::new(CreateObjectGroupRequest {
            name: "Source object group".to_string(),
            description: "Created in get_object_group_by_id_grpc_test.".to_string(),
            collection_id: random_collection.id.to_string(),
            object_ids: data_object_ids.clone(),
            meta_object_ids: meta_object_ids.clone(),
            labels: vec![],
            hooks: vec![],
        }),
        common::oidc::ADMINTOKEN,
    );

    let source_object_group = object_group_service
        .create_object_group(create_object_group_request)
        .await
        .unwrap()
        .into_inner()
        .object_group
        .unwrap();

    // Try to update object group with with different permissions
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

        // Create object group which will be updated
        let get_object_group_request = common::grpc_helpers::add_token(
            tonic::Request::new(GetObjectGroupByIdRequest {
                group_id: source_object_group.id.to_string(),
                collection_id: random_collection.id.to_string(),
            }),
            common::oidc::REGULARTOKEN,
        );

        let get_object_group_response = object_group_service
            .get_object_group_by_id(get_object_group_request)
            .await;

        // Check if request succeeded for specific permission
        match *permission {
            Permission::None => {
                assert!(get_object_group_response.is_err());
            }
            Permission::Read | Permission::Append | Permission::Modify | Permission::Admin => {
                // Validate object group fetch
                let fetched_object_group = get_object_group_response
                    .unwrap()
                    .into_inner()
                    .object_group
                    .unwrap();

                assert!(!fetched_object_group.id.is_empty());
                assert_eq!(fetched_object_group.id, source_object_group.id);
                assert_eq!(fetched_object_group.rev_number, 0);
                assert_eq!(fetched_object_group.name, "Source object group".to_string());
                assert_eq!(
                    fetched_object_group.description.as_str(),
                    "Created in get_object_group_by_id_grpc_test."
                );
                assert_eq!(fetched_object_group.labels, vec![]);
                assert_eq!(fetched_object_group.hooks, vec![]);
            }
            _ => panic!("Unspecified permission is not allowed."),
        }
    }
}

/// The individual steps of this test function contains:
/// 1. Get object groups from object with different permissions
#[ignore]
#[tokio::test]
#[serial(db)]
async fn get_object_groups_from_object_grpc_test() {
    // Init database connection
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone(), ArunaServerConfig::default()).await);

    // Init object group service
    let object_group_service = ObjectGroupServiceImpl::new(db.clone(), authz).await;

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
    let random_collection = common::functions::create_collection(TCreateCollection {
        project_id: random_project.id.to_string(),
        creator_id: Some(user_id.clone()),
        ..Default::default()
    });

    // Create random data and meta objects
    let source_object = common::functions::create_object(&TCreateObject {
        creator_id: Some(user_id.to_string()),
        collection_id: random_collection.id.to_string(),
        ..Default::default()
    });

    // Create random object groups which will be queried
    let mut object_group_ids = Vec::new();
    for i in 0..5 {
        let create_object_group_request = common::grpc_helpers::add_token(
            tonic::Request::new(CreateObjectGroupRequest {
                name: format!("Dummy-Object-Group-00{i}"),
                description: "Created in get_object_groups_from_object_grpc_test.".to_string(),
                collection_id: random_collection.id.to_string(),
                object_ids: vec![source_object.id.to_string()],
                meta_object_ids: vec![],
                labels: vec![],
                hooks: vec![],
            }),
            common::oidc::ADMINTOKEN,
        );

        object_group_ids.push(
            object_group_service
                .create_object_group(create_object_group_request)
                .await
                .unwrap()
                .into_inner()
                .object_group
                .unwrap()
                .id,
        );
    }

    // Try to update object group with with different permissions
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

        // Create object group which will be updated
        let get_object_groups_request = common::grpc_helpers::add_token(
            tonic::Request::new(GetObjectGroupsFromObjectRequest {
                object_id: source_object.id.to_string(),
                collection_id: random_collection.id.to_string(),
                page_request: None,
            }),
            common::oidc::REGULARTOKEN,
        );

        let get_object_groups_response = object_group_service
            .get_object_groups_from_object(get_object_groups_request)
            .await;

        // Check if request succeeded for specific permission
        match *permission {
            Permission::None => {
                assert!(get_object_groups_response.is_err());
            }
            Permission::Read | Permission::Append | Permission::Modify | Permission::Admin => {
                // Validate object group fetch
                let fetched_object_groups = get_object_groups_response
                    .unwrap()
                    .into_inner()
                    .object_groups
                    .unwrap()
                    .object_group_overviews;

                assert_eq!(fetched_object_groups.len(), 5);

                for object_group in fetched_object_groups {
                    assert!(object_group_ids.contains(&object_group.id))
                }
            }
            _ => panic!("Unspecified permission is not allowed."),
        }
    }
}

/// The individual steps of this test function contains:
/// 1. Get multiple object groups with different permissions
#[ignore]
#[tokio::test]
#[serial(db)]
async fn get_object_groups_grpc_test() {
    // Init database connection
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone(), ArunaServerConfig::default()).await);

    // Init object group service
    let object_group_service = ObjectGroupServiceImpl::new(db.clone(), authz).await;

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
    let random_collection = common::functions::create_collection(TCreateCollection {
        project_id: random_project.id.to_string(),
        creator_id: Some(user_id.clone()),
        ..Default::default()
    });

    // Create random data and meta objects
    let object_meta = TCreateObject {
        creator_id: Some(user_id.to_string()),
        collection_id: random_collection.id.to_string(),
        ..Default::default()
    };

    let data_object_ids = vec![common::functions::create_object(&object_meta).id];
    let meta_object_ids = vec![common::functions::create_object(&object_meta).id];

    // Create object groups which will be queried
    let mut all_object_group_ids = Vec::new();
    let mut inner_create_request = CreateObjectGroupRequest {
        name: "Dummy object group 00{}".to_string(),
        description: "Created in get_object_groups_grpc_test.".to_string(),
        collection_id: random_collection.id.to_string(),
        object_ids: data_object_ids.clone(),
        meta_object_ids: meta_object_ids.clone(),
        labels: vec![],
        hooks: vec![],
    };
    for i in 0..5 {
        inner_create_request.name = format!("Dummy object group 00{i}");

        if i % 2 == 0 {
            inner_create_request.labels = vec![KeyValue {
                key: "validated".to_string(),
                value: "true".to_string(),
            }];
        } else {
            inner_create_request.labels = vec![];
        }

        let create_object_group_request = common::grpc_helpers::add_token(
            tonic::Request::new(inner_create_request.clone()),
            common::oidc::ADMINTOKEN,
        );

        all_object_group_ids.push(
            object_group_service
                .create_object_group(create_object_group_request)
                .await
                .unwrap()
                .into_inner()
                .object_group
                .unwrap()
                .id,
        );
    }

    // Try to fetch multiple object groups with with different permissions
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

        // Create basic request to generally fetch object groups of collection
        let get_all_object_groups_request = common::grpc_helpers::add_token(
            tonic::Request::new(GetObjectGroupsRequest {
                collection_id: random_collection.id.to_string(),
                page_request: None,
                label_id_filter: None,
            }),
            common::oidc::REGULARTOKEN,
        );

        let get_all_object_groups_response = object_group_service
            .get_object_groups(get_all_object_groups_request)
            .await;

        // Create request to fetch object groups filtered by id
        let id_filtered_object_group_ids = vec![
            all_object_group_ids[0].to_string(),
            all_object_group_ids[2].to_string(),
        ];
        let get_id_filtered_object_groups_request = common::grpc_helpers::add_token(
            tonic::Request::new(GetObjectGroupsRequest {
                collection_id: random_collection.id.to_string(),
                page_request: None,
                label_id_filter: Some(LabelOrIdQuery {
                    labels: None,
                    ids: id_filtered_object_group_ids.clone(),
                }),
            }),
            common::oidc::REGULARTOKEN,
        );

        let get_id_filtered_object_groups_response = object_group_service
            .get_object_groups(get_id_filtered_object_groups_request)
            .await;

        // Create request to fetch object groups filtered by label (should be the 'index % 2 == 0' elements of the vector)
        let label_filtered_object_group_ids = vec![
            all_object_group_ids[1].to_string(),
            all_object_group_ids[3].to_string(),
        ];
        let get_label_filtered_object_groups_request = common::grpc_helpers::add_token(
            tonic::Request::new(GetObjectGroupsRequest {
                collection_id: random_collection.id.to_string(),
                page_request: None,
                label_id_filter: Some(LabelOrIdQuery {
                    labels: None,
                    ids: label_filtered_object_group_ids.clone(),
                }),
            }),
            common::oidc::REGULARTOKEN,
        );

        let get_label_filtered_object_groups_response = object_group_service
            .get_object_groups(get_label_filtered_object_groups_request)
            .await;

        // Check if request succeeded for specific permission
        match *permission {
            Permission::None => {
                assert!(get_all_object_groups_response.is_err());
                assert!(get_id_filtered_object_groups_response.is_err());
                assert!(get_label_filtered_object_groups_response.is_err());
            }
            Permission::Read | Permission::Append | Permission::Modify | Permission::Admin => {
                // Validate object group fetch
                let all_object_groups = get_all_object_groups_response
                    .unwrap()
                    .into_inner()
                    .object_groups
                    .unwrap()
                    .object_group_overviews;

                assert_eq!(all_object_groups.len(), 5);
                for object_group in all_object_groups {
                    assert!(all_object_group_ids.contains(&object_group.id))
                }

                let id_filtered_object_groups = get_id_filtered_object_groups_response
                    .unwrap()
                    .into_inner()
                    .object_groups
                    .unwrap()
                    .object_group_overviews;

                assert_eq!(id_filtered_object_groups.len(), 2);
                for object_group in id_filtered_object_groups {
                    assert!(id_filtered_object_group_ids.contains(&object_group.id))
                }

                let label_filtered_object_groups = get_label_filtered_object_groups_response
                    .unwrap()
                    .into_inner()
                    .object_groups
                    .unwrap()
                    .object_group_overviews;

                assert_eq!(label_filtered_object_groups.len(), 2);
                for object_group in label_filtered_object_groups {
                    assert!(label_filtered_object_group_ids.contains(&object_group.id))
                }
            }
            _ => panic!("Unspecified permission is not allowed."),
        }
    }
}

/// The individual steps of this test function contains:
/// 1. Get object group revisions with different permissions
#[ignore]
#[tokio::test]
#[serial(db)]
async fn get_object_group_history_grpc_test() {
    // Init database connection
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone(), ArunaServerConfig::default()).await);

    // Init object group service
    let object_group_service = ObjectGroupServiceImpl::new(db.clone(), authz).await;

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
    let random_collection = common::functions::create_collection(TCreateCollection {
        project_id: random_project.id.to_string(),
        creator_id: Some(user_id.clone()),
        ..Default::default()
    });

    // Create random data and meta object
    let data_object = common::functions::create_object(&TCreateObject {
        creator_id: Some(user_id.to_string()),
        collection_id: random_collection.id.to_string(),
        ..Default::default()
    });
    let meta_object = common::functions::create_object(&TCreateObject {
        creator_id: Some(user_id.to_string()),
        collection_id: random_collection.id.to_string(),
        ..Default::default()
    });

    // Create initial object group
    let create_object_group_request = common::grpc_helpers::add_token(
        tonic::Request::new(CreateObjectGroupRequest {
            name: "Dummy-Object-Group".to_string(),
            description: "Revision 0 created in get_object_group_history_grpc_test.".to_string(),
            collection_id: random_collection.id.to_string(),
            object_ids: vec![data_object.id.to_string()],
            meta_object_ids: vec![meta_object.id.to_string()],
            labels: vec![],
            hooks: vec![],
        }),
        common::oidc::ADMINTOKEN,
    );

    let rev_0_group = object_group_service
        .create_object_group(create_object_group_request)
        .await
        .unwrap()
        .into_inner()
        .object_group
        .unwrap();

    // Randomly update object group to create revisions
    let mut object_group_revision_ids = vec![rev_0_group.id.to_string()];
    let mut source_object_group = rev_0_group.clone();
    for i in 1..3 {
        let update_object_group_request = common::grpc_helpers::add_token(
            tonic::Request::new(UpdateObjectGroupRequest {
                group_id: source_object_group.id.to_string(),
                name: "Dummy-Object-Group".to_string(),
                description: format!("Revision {i} created in get_object_group_history_grpc_test."),
                collection_id: random_collection.id.to_string(),
                object_ids: vec![data_object.id.to_string()],
                meta_object_ids: vec![meta_object.id.to_string()],
                labels: vec![],
                hooks: vec![],
            }),
            common::oidc::ADMINTOKEN,
        );

        let updated_object_group = object_group_service
            .update_object_group(update_object_group_request)
            .await
            .unwrap()
            .into_inner()
            .object_group
            .unwrap();

        // Save id of object group revision
        object_group_revision_ids.push(updated_object_group.id.to_string());

        // Use new revision as source for new update
        source_object_group = updated_object_group;
    }

    // Try to fetch object group revisions with different permissions
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

        // Create object group which will be updated
        let get_object_group_history_request = common::grpc_helpers::add_token(
            tonic::Request::new(GetObjectGroupHistoryRequest {
                collection_id: random_collection.id.to_string(),
                group_id: rev_0_group.id.to_string(),
                page_request: None,
            }),
            common::oidc::REGULARTOKEN,
        );

        let get_object_groups_response = object_group_service
            .get_object_group_history(get_object_group_history_request)
            .await;

        // Check if request succeeded for specific permission
        match *permission {
            Permission::None => {
                assert!(get_object_groups_response.is_err());
            }
            Permission::Read | Permission::Append | Permission::Modify | Permission::Admin => {
                // Validate object group fetch
                let fetched_object_groups = get_object_groups_response
                    .unwrap()
                    .into_inner()
                    .object_groups
                    .unwrap()
                    .object_group_overviews;

                assert_eq!(fetched_object_groups.len(), 3);

                for object_group in fetched_object_groups {
                    assert!(object_group_revision_ids.contains(&object_group.id))
                }
            }
            _ => panic!("Unspecified permission is not allowed."),
        }
    }
}

/// The individual steps of this test function contains:
/// 1. Get object group objects with different permissions
#[ignore]
#[tokio::test]
#[serial(db)]
async fn get_object_group_objects_grpc_test() {
    // Init database connection
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone(), ArunaServerConfig::default()).await);

    // Init object group service
    let object_group_service = ObjectGroupServiceImpl::new(db.clone(), authz).await;

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
    let random_collection = common::functions::create_collection(TCreateCollection {
        project_id: random_project.id.to_string(),
        creator_id: Some(user_id.clone()),
        ..Default::default()
    });

    // Create random data and meta objects
    let object_meta = TCreateObject {
        creator_id: Some(user_id.to_string()),
        collection_id: random_collection.id.to_string(),
        ..Default::default()
    };
    let data_object_ids = (0..3)
        .map(|_| common::functions::create_object(&object_meta).id)
        .collect::<Vec<_>>();
    let meta_object_ids = (0..3)
        .map(|_| common::functions::create_object(&object_meta).id)
        .collect::<Vec<_>>();

    // Create source object group
    let create_object_group_request = common::grpc_helpers::add_token(
        tonic::Request::new(CreateObjectGroupRequest {
            name: "Source object group".to_string(),
            description: "Created in get_object_group_objects_grpc_test.".to_string(),
            collection_id: random_collection.id.to_string(),
            object_ids: data_object_ids.clone(),
            meta_object_ids: meta_object_ids.clone(),
            labels: vec![],
            hooks: vec![],
        }),
        common::oidc::ADMINTOKEN,
    );

    let source_object_group = object_group_service
        .create_object_group(create_object_group_request)
        .await
        .unwrap()
        .into_inner()
        .object_group
        .unwrap();

    // Try to fetch object group objects with different permissions
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

        // Get all objects of object group
        let get_all_objects_request = common::grpc_helpers::add_token(
            tonic::Request::new(GetObjectGroupObjectsRequest {
                collection_id: random_collection.id.to_string(),
                group_id: source_object_group.id.to_string(),
                page_request: None,
                meta_only: false,
            }),
            common::oidc::REGULARTOKEN,
        );
        let get_all_objects_response = object_group_service
            .get_object_group_objects(get_all_objects_request)
            .await;

        // Get all meta objects of object group
        let get_meta_objects_request = common::grpc_helpers::add_token(
            tonic::Request::new(GetObjectGroupObjectsRequest {
                collection_id: random_collection.id.to_string(),
                group_id: source_object_group.id.to_string(),
                page_request: None,
                meta_only: true,
            }),
            common::oidc::REGULARTOKEN,
        );

        let get_meta_objects_response = object_group_service
            .get_object_group_objects(get_meta_objects_request)
            .await;

        // Check if request succeeded for specific permission
        match *permission {
            Permission::None => {
                assert!(get_all_objects_response.is_err());
                assert!(get_meta_objects_response.is_err());
            }
            Permission::Read | Permission::Append | Permission::Modify | Permission::Admin => {
                // Validate fetched objects from get_all_objects_response
                for group_object in get_all_objects_response
                    .unwrap()
                    .into_inner()
                    .object_group_objects
                {
                    if group_object.is_metadata {
                        assert!(meta_object_ids.contains(&group_object.object.unwrap().id))
                    } else {
                        assert!(data_object_ids.contains(&group_object.object.unwrap().id))
                    }
                }

                // Validate fetched objects from get_all_objects_response
                for group_object in get_meta_objects_response
                    .unwrap()
                    .into_inner()
                    .object_group_objects
                {
                    if group_object.is_metadata {
                        assert!(meta_object_ids.contains(&group_object.object.unwrap().id))
                    } else {
                        panic!(
                            "{}",
                            format!(
                            "Data object {} should not be included in get_meta_objects_response.",
                            group_object.object.unwrap().id
                        )
                        )
                    }
                }
            }
            _ => panic!("Unspecified permission is not allowed."),
        }
    }
}

/// The individual steps of this test function contains:
/// 1. Delete object group revisions individually with different permissions
/// 2. Delete object group revisions in single request with different permissions
#[ignore]
#[tokio::test]
#[serial(db)]
async fn delete_object_group_grpc_test() {
    // Init database connection
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone(), ArunaServerConfig::default()).await);

    // Init object group service
    let object_group_service = ObjectGroupServiceImpl::new(db.clone(), authz).await;

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
    let random_collection = common::functions::create_collection(TCreateCollection {
        project_id: random_project.id.to_string(),
        creator_id: Some(user_id.clone()),
        ..Default::default()
    });

    // Create random data and meta object
    let data_object = common::functions::create_object(&TCreateObject {
        creator_id: Some(user_id.to_string()),
        collection_id: random_collection.id.to_string(),
        ..Default::default()
    });
    let meta_object = common::functions::create_object(&TCreateObject {
        creator_id: Some(user_id.to_string()),
        collection_id: random_collection.id.to_string(),
        ..Default::default()
    });

    // Delete object group revisions individually with different permissions
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

        // Create object group with two revisions
        let create_object_group_request = common::grpc_helpers::add_token(
            tonic::Request::new(CreateObjectGroupRequest {
                name: "Dummy-Object-Group".to_string(),
                description: "Revision 0 created in get_object_group_history_grpc_test."
                    .to_string(),
                collection_id: random_collection.id.to_string(),
                object_ids: vec![data_object.id.to_string()],
                meta_object_ids: vec![meta_object.id.to_string()],
                labels: vec![],
                hooks: vec![],
            }),
            common::oidc::ADMINTOKEN,
        );
        let rev_0_group = object_group_service
            .create_object_group(create_object_group_request)
            .await
            .unwrap()
            .into_inner()
            .object_group
            .unwrap();

        let update_object_group_request = common::grpc_helpers::add_token(
            tonic::Request::new(UpdateObjectGroupRequest {
                group_id: rev_0_group.id.to_string(),
                name: "Dummy-Object-Group".to_string(),
                description: "Revision 1 created in get_object_group_history_grpc_test."
                    .to_string(),
                collection_id: random_collection.id.to_string(),
                object_ids: vec![data_object.id.to_string()],
                meta_object_ids: vec![meta_object.id.to_string()],
                labels: vec![],
                hooks: vec![],
            }),
            common::oidc::ADMINTOKEN,
        );
        let rev_1_group = object_group_service
            .update_object_group(update_object_group_request)
            .await
            .unwrap()
            .into_inner()
            .object_group
            .unwrap();

        // Delete revisions with individual requests
        let delete_rev_1_request = common::grpc_helpers::add_token(
            tonic::Request::new(DeleteObjectGroupRequest {
                group_id: rev_1_group.id.to_string(),
                collection_id: random_collection.id.to_string(),
                with_revisions: false,
            }),
            common::oidc::REGULARTOKEN,
        );
        let rev_1_deletion = object_group_service
            .delete_object_group(delete_rev_1_request)
            .await;

        let delete_rev_0_request = common::grpc_helpers::add_token(
            tonic::Request::new(DeleteObjectGroupRequest {
                group_id: rev_0_group.id.to_string(),
                collection_id: random_collection.id.to_string(),
                with_revisions: false,
            }),
            common::oidc::REGULARTOKEN,
        );
        let rev_0_deletion = object_group_service
            .delete_object_group(delete_rev_0_request)
            .await;

        // Check if request succeeded for specific permission
        match *permission {
            Permission::None | Permission::Read | Permission::Append => {
                assert!(rev_1_deletion.is_err());
                assert!(rev_0_deletion.is_err());
            }
            Permission::Modify | Permission::Admin => {
                // Validate object group deletion
                rev_1_deletion.unwrap();
                rev_0_deletion.unwrap();

                // Try to fetch non-existing object group
                let get_object_group_request = common::grpc_helpers::add_token(
                    tonic::Request::new(GetObjectGroupByIdRequest {
                        group_id: rev_0_group.id.to_string(),
                        collection_id: random_collection.id.to_string(),
                    }),
                    common::oidc::ADMINTOKEN,
                );

                assert!(object_group_service
                    .get_object_group_by_id(get_object_group_request)
                    .await
                    .is_err())
            }
            _ => panic!("Unspecified permission is not allowed."),
        }
    }

    // Delete object group revisions in single request with different permissions
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

        // Create object group with two revisions
        let create_object_group_request = common::grpc_helpers::add_token(
            tonic::Request::new(CreateObjectGroupRequest {
                name: "Dummy-Object-Group".to_string(),
                description: "Revision 0 created in get_object_group_history_grpc_test."
                    .to_string(),
                collection_id: random_collection.id.to_string(),
                object_ids: vec![data_object.id.to_string()],
                meta_object_ids: vec![meta_object.id.to_string()],
                labels: vec![],
                hooks: vec![],
            }),
            common::oidc::ADMINTOKEN,
        );
        let rev_0_group = object_group_service
            .create_object_group(create_object_group_request)
            .await
            .unwrap()
            .into_inner()
            .object_group
            .unwrap();

        let update_object_group_request = common::grpc_helpers::add_token(
            tonic::Request::new(UpdateObjectGroupRequest {
                group_id: rev_0_group.id.to_string(),
                name: "Dummy-Object-Group".to_string(),
                description: "Revision 1 created in get_object_group_history_grpc_test."
                    .to_string(),
                collection_id: random_collection.id.to_string(),
                object_ids: vec![data_object.id.to_string()],
                meta_object_ids: vec![meta_object.id.to_string()],
                labels: vec![],
                hooks: vec![],
            }),
            common::oidc::ADMINTOKEN,
        );
        let rev_1_group = object_group_service
            .update_object_group(update_object_group_request)
            .await
            .unwrap()
            .into_inner()
            .object_group
            .unwrap();

        // Delete revisions with individual requests
        let delete_revisions_request = common::grpc_helpers::add_token(
            tonic::Request::new(DeleteObjectGroupRequest {
                group_id: rev_1_group.id.to_string(),
                collection_id: random_collection.id.to_string(),
                with_revisions: true,
            }),
            common::oidc::REGULARTOKEN,
        );
        let delete_revisions_response = object_group_service
            .delete_object_group(delete_revisions_request)
            .await;

        // Check if request succeeded for specific permission
        match *permission {
            Permission::None | Permission::Read | Permission::Append => {
                assert!(delete_revisions_response.is_err());
            }
            Permission::Modify | Permission::Admin => {
                // Validate object group deletion
                delete_revisions_response.unwrap();

                // Try to fetch non-existing object group
                let get_object_group_request = common::grpc_helpers::add_token(
                    tonic::Request::new(GetObjectGroupByIdRequest {
                        group_id: rev_0_group.id.to_string(),
                        collection_id: random_collection.id.to_string(),
                    }),
                    common::oidc::ADMINTOKEN,
                );

                assert!(object_group_service
                    .get_object_group_by_id(get_object_group_request)
                    .await
                    .is_err())
            }
            _ => panic!("Unspecified permission is not allowed."),
        }
    }
}

/// The individual steps of this test function contains:
/// 1. Add labels to object group with different permissions
#[ignore]
#[tokio::test]
#[serial(db)]
async fn add_labels_to_object_group_grpc_test() {
    // Init database connection
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone(), ArunaServerConfig::default()).await);

    // Init object group service
    let object_group_service = ObjectGroupServiceImpl::new(db.clone(), authz).await;

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
    let random_collection = common::functions::create_collection(TCreateCollection {
        project_id: random_project.id.to_string(),
        creator_id: Some(user_id.clone()),
        ..Default::default()
    });

    // Create random data and meta object
    let data_object = common::functions::create_object(&TCreateObject {
        creator_id: Some(user_id.to_string()),
        collection_id: random_collection.id.to_string(),
        ..Default::default()
    });
    let meta_object = common::functions::create_object(&TCreateObject {
        creator_id: Some(user_id.to_string()),
        collection_id: random_collection.id.to_string(),
        ..Default::default()
    });

    // Create random object group without labels
    let create_object_group_request = common::grpc_helpers::add_token(
        tonic::Request::new(CreateObjectGroupRequest {
            name: "Source object group".to_string(),
            description: "Created in get_object_group_by_id_grpc_test.".to_string(),
            collection_id: random_collection.id.to_string(),
            object_ids: vec![data_object.id],
            meta_object_ids: vec![meta_object.id],
            labels: vec![],
            hooks: vec![],
        }),
        common::oidc::ADMINTOKEN,
    );

    let source_object_group = object_group_service
        .create_object_group(create_object_group_request)
        .await
        .unwrap()
        .into_inner()
        .object_group
        .unwrap();

    // Add label/s to object group with different permissions
    let mut added_key_values = Vec::new();
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

        // Add labels to object group
        let label = KeyValue {
            key: format!("{:#?}", permission),
            value: "added".to_string(),
        };
        let add_labels_request = common::grpc_helpers::add_token(
            tonic::Request::new(AddLabelsToObjectGroupRequest {
                collection_id: random_collection.id.to_string(),
                group_id: source_object_group.id.to_string(),
                labels_to_add: vec![label.clone()],
            }),
            common::oidc::REGULARTOKEN,
        );
        let add_labels_response = object_group_service
            .add_labels_to_object_group(add_labels_request)
            .await;

        // Check if request succeeded for specific permission
        match *permission {
            Permission::None | Permission::Read | Permission::Append => {
                assert!(add_labels_response.is_err());
            }
            Permission::Modify | Permission::Admin => {
                // Add created label to vector for easier validation
                added_key_values.push(label);

                // Get labels of add_labels_response
                let returned_object_group = add_labels_response
                    .unwrap()
                    .into_inner()
                    .object_group
                    .unwrap();

                assert_eq!(returned_object_group.rev_number, 0); // Object group was not updated
                assert_eq!(&returned_object_group.labels.len(), &added_key_values.len());

                for key_value in returned_object_group.labels {
                    assert!(added_key_values.contains(&key_value))
                }

                // Fetch labels to validate
                let get_object_group_request = common::grpc_helpers::add_token(
                    tonic::Request::new(GetObjectGroupByIdRequest {
                        group_id: source_object_group.id.to_string(),
                        collection_id: random_collection.id.to_string(),
                    }),
                    common::oidc::ADMINTOKEN,
                );

                let fetched_object_group = object_group_service
                    .get_object_group_by_id(get_object_group_request)
                    .await
                    .unwrap()
                    .into_inner()
                    .object_group
                    .unwrap();

                assert_eq!(fetched_object_group.rev_number, 0); // Object group was not updated
                assert_eq!(&fetched_object_group.labels.len(), &added_key_values.len());

                for key_value in fetched_object_group.labels {
                    assert!(added_key_values.contains(&key_value))
                }
            }
            _ => panic!("Unspecified permission is not allowed."),
        }
    }
}
