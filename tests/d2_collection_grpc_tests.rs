extern crate core;

use aruna_rust_api::api::storage::models::v1::{
    DataClass, KeyValue, LabelOntology, Permission, ProjectPermission,
};
use aruna_rust_api::api::storage::services::v1::collection_service_server::CollectionService;
use aruna_rust_api::api::storage::services::v1::project_service_server::ProjectService;
use aruna_rust_api::api::storage::services::v1::user_service_server::UserService;
use aruna_rust_api::api::storage::services::v1::{
    AddUserToProjectRequest, CreateNewCollectionRequest, GetCollectionByIdRequest, GetUserRequest,
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
