use crate::common::functions::{TCreateCollection, TCreateObject};
use crate::common::grpc_helpers::get_token_user_id;
use aruna_rust_api::api::storage::models::v1::Permission;
use aruna_rust_api::api::storage::services::v1::object_group_service_server::ObjectGroupService;
use aruna_rust_api::api::storage::services::v1::CreateObjectGroupRequest;
use aruna_server::server::services::objectgroup::ObjectGroupServiceImpl;
use aruna_server::{
    database::{self},
    server::services::authz::Authz,
};
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
    let authz = Arc::new(Authz::new(db.clone()).await);

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
    let collection_meta = TCreateCollection {
        project_id: random_project.id.to_string(),
        creator_id: Some(user_id.clone()),
        ..Default::default()
    };
    let random_collection = common::functions::create_collection(collection_meta.clone());

    // Create random data and meta objects
    let data_object_ids = (0..5)
        .map(|_| {
            common::functions::create_object(
                &(TCreateObject {
                    creator_id: Some(user_id.to_string()),
                    collection_id: random_collection.id.to_string(),
                    ..Default::default()
                }),
            )
            .id
        })
        .collect::<Vec<_>>();

    let meta_object_ids = (0..5)
        .map(|_| {
            common::functions::create_object(
                &(TCreateObject {
                    creator_id: Some(user_id.to_string()),
                    collection_id: random_collection.id.to_string(),
                    ..Default::default()
                }),
            )
            .id
        })
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
