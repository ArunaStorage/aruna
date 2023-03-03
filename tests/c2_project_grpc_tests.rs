use crate::common::functions::TCreateCollection;
use aruna_rust_api::api::storage::models::v1::{Permission, ProjectPermission};
use aruna_rust_api::api::storage::services::v1::collection_service_server::CollectionService;
use aruna_rust_api::api::storage::services::v1::project_service_server::ProjectService;
use aruna_rust_api::api::storage::services::v1::user_service_server::UserService;
use aruna_rust_api::api::storage::services::v1::{
    AddUserToProjectRequest, CreateProjectRequest, DeleteCollectionRequest, DestroyProjectRequest,
    EditUserPermissionsForProjectRequest, GetProjectRequest, GetProjectsRequest,
    GetUserPermissionsForProjectRequest, GetUserRequest, RemoveUserFromProjectRequest,
    UpdateProjectRequest,
};
use aruna_server::config::ArunaServerConfig;
use aruna_server::server::services::collection::CollectionServiceImpl;
use aruna_server::{
    database::{self},
    server::services::authz::Authz,
    server::services::project::ProjectServiceImpl,
    server::services::user::UserServiceImpl,
};
use serial_test::serial;
use std::sync::Arc;

mod common;

#[ignore]
#[tokio::test]
#[serial(db)]
async fn create_project_grpc_test() {
    // Init project service
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone(), ArunaServerConfig::default()).await);
    let project_service = ProjectServiceImpl::new(db, authz).await;

    // Create gPC Request for project creation
    let create_project_request = common::grpc_helpers::add_token(
        tonic::Request::new(CreateProjectRequest {
            name: "Test-Project".to_string(),
            description: "This project was created in create_project_grpc_test().".to_string(),
        }),
        common::oidc::ADMINTOKEN,
    );

    // Send request to gRPC endpoint of AOS instance
    let create_project_response = project_service
        .create_project(create_project_request)
        .await
        .unwrap()
        .into_inner();

    // Validate project id format
    uuid::Uuid::parse_str(create_project_response.project_id.as_str()).unwrap();

    // Create gPC Request for failing project creation
    let create_project_request = common::grpc_helpers::add_token(
        tonic::Request::new(CreateProjectRequest {
            name: "Test Project".to_string(),
            description: "This project was created in create_project_grpc_test().".to_string(),
        }),
        common::oidc::REGULARTOKEN,
    );

    // Send request to gRPC endpoint of AOS instance
    let create_project_response = project_service.create_project(create_project_request).await;

    // Validate project creation failed with non-admin token
    assert!(create_project_response.is_err())
}

#[ignore]
#[tokio::test]
#[serial(db)]
async fn get_projects_grpc_test() {
    // Init project service
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone(), ArunaServerConfig::default()).await);
    let project_service = ProjectServiceImpl::new(db, authz).await;

    // Fast track project creation
    let project_001 = common::functions::create_project(None);
    let project_002 = common::functions::create_project(None);

    // Create gPC Request to fetch single project
    let get_project_request = common::grpc_helpers::add_token(
        tonic::Request::new(GetProjectRequest {
            project_id: project_001.id.to_string(),
        }),
        common::oidc::ADMINTOKEN,
    );

    let get_project_response = project_service
        .get_project(get_project_request)
        .await
        .unwrap()
        .into_inner();

    let fetch_project = get_project_response.project.unwrap();

    assert_eq!(project_001.id, fetch_project.id);
    assert_eq!(project_001.name, fetch_project.name);
    assert_eq!(project_001.description, fetch_project.description);

    // Create gPC Request to fetch project with non-authorized token
    let get_project_request = common::grpc_helpers::add_token(
        tonic::Request::new(GetProjectRequest {
            project_id: project_001.id.to_string(),
        }),
        common::oidc::REGULARTOKEN,
    );

    let get_project_response = project_service.get_project(get_project_request).await;

    assert!(get_project_response.is_err());

    // Create gPC Request to fetch all projects including project_001 and project_002
    let get_projects_request = common::grpc_helpers::add_token(
        tonic::Request::new(GetProjectsRequest {}),
        common::oidc::ADMINTOKEN,
    );

    let get_projects_response = project_service
        .get_projects(get_projects_request)
        .await
        .unwrap()
        .into_inner();

    // Note: Filter for specific projects as there were created A LOT more in other tests with this token
    let filtered_projects = get_projects_response
        .projects
        .iter()
        .filter(|proj| vec![project_001.id.clone(), project_002.id.clone()].contains(&proj.id))
        .collect::<Vec<_>>();

    assert_eq!(filtered_projects.len(), 2);
    for proj in filtered_projects {
        if proj.id == project_001.id {
            assert_eq!(proj.name, project_001.name);
            assert_eq!(proj.description, project_001.description);
        } else if proj.id == project_002.id {
            assert_eq!(proj.name, project_002.name);
            assert_eq!(proj.description, project_002.description);
        } else {
            panic!("There should only be the ids of project_001 and project_002.")
        }
    }

    // Create gPC Request to fetch all projects with non-authorized token
    let get_projects_request = common::grpc_helpers::add_token(
        tonic::Request::new(GetProjectsRequest {}),
        common::oidc::REGULARTOKEN,
    );

    let get_projects_response = project_service.get_projects(get_projects_request).await;

    assert!(get_projects_response.is_err())
}

#[ignore]
#[tokio::test]
#[serial(db)]
async fn update_project_grpc_test() {
    // Init project service
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone(), ArunaServerConfig::default()).await);
    let project_service = ProjectServiceImpl::new(db, authz).await;

    // Fast track project creation
    let orig_project = common::functions::create_project(None);

    // Create gRPC Request to update single project name and description
    let update_project_request = common::grpc_helpers::add_token(
        tonic::Request::new(UpdateProjectRequest {
            project_id: orig_project.id.to_string(),
            name: "Updated-Project".to_string(),
            description: "This project was updated in update_project_grpc_test().".to_string(),
        }),
        common::oidc::ADMINTOKEN,
    );

    let update_project_response = project_service
        .update_project(update_project_request)
        .await
        .unwrap()
        .into_inner();

    let updated_project = update_project_response.project.unwrap();

    assert_eq!(orig_project.id, updated_project.id);
    assert_eq!(updated_project.name, "Updated-Project".to_string());
    assert_eq!(
        updated_project.description,
        "This project was updated in update_project_grpc_test().".to_string()
    );

    // Create gPC Request to update single project with non-authorized token
    let update_project_request = common::grpc_helpers::add_token(
        tonic::Request::new(UpdateProjectRequest {
            project_id: orig_project.id.to_string(),
            name: "Updated Project".to_string(),
            description: "This update should have been failed...".to_string(),
        }),
        common::oidc::REGULARTOKEN,
    );

    let update_project_response = project_service.update_project(update_project_request).await;

    assert!(update_project_response.is_err())
}

#[ignore]
#[tokio::test]
#[serial(db)]
async fn destroy_project_grpc_test() {
    // Init project service
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone(), ArunaServerConfig::default()).await);
    let project_service = ProjectServiceImpl::new(db.clone(), authz.clone()).await;
    let collection_service = CollectionServiceImpl::new(db, authz).await;

    // Fast track project creation
    let random_project = common::functions::create_project(None);

    // Create gPC Request to delete project with non-authorized token
    let delete_project_request = common::grpc_helpers::add_token(
        tonic::Request::new(DestroyProjectRequest {
            project_id: random_project.id.to_string(),
        }),
        common::oidc::REGULARTOKEN,
    );

    let delete_project_response = project_service
        .destroy_project(delete_project_request)
        .await;

    assert!(delete_project_response.is_err());

    // Try to delete non-empty project
    let random_collection = common::functions::create_collection(TCreateCollection {
        project_id: random_project.id.to_string(),
        num_labels: 0,
        num_hooks: 0,
        col_override: None,
        creator_id: None,
    });

    let delete_project_request = common::grpc_helpers::add_token(
        tonic::Request::new(DestroyProjectRequest {
            project_id: random_project.id.to_string(),
        }),
        common::oidc::ADMINTOKEN,
    );

    let delete_project_response = project_service
        .destroy_project(delete_project_request)
        .await;

    assert!(delete_project_response.is_err());

    // Remove collection and then empty project
    let delete_collection_request = common::grpc_helpers::add_token(
        tonic::Request::new(DeleteCollectionRequest {
            collection_id: random_collection.id.to_string(),
            force: false,
        }),
        common::oidc::ADMINTOKEN,
    );

    assert!(collection_service
        .delete_collection(delete_collection_request)
        .await
        .is_ok());

    let delete_project_request = common::grpc_helpers::add_token(
        tonic::Request::new(DestroyProjectRequest {
            project_id: random_project.id.to_string(),
        }),
        common::oidc::ADMINTOKEN,
    );

    let _delete_project_response = project_service
        .destroy_project(delete_project_request)
        .await
        .unwrap();
}

#[ignore]
#[tokio::test]
#[serial(db)]
async fn add_remove_project_user_grpc_test() {
    // Init user/project services
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone(), ArunaServerConfig::default()).await);
    let user_service = UserServiceImpl::new(db.clone(), authz.clone()).await;
    let project_service = ProjectServiceImpl::new(db, authz).await;

    // Fast track project creation
    let project_id = common::functions::create_project(None).id;

    // Create gPC Request to add non-existing user with permissions
    let add_user_request = common::grpc_helpers::add_token(
        tonic::Request::new(AddUserToProjectRequest {
            project_id: project_id.to_string(),
            user_permission: Some(ProjectPermission {
                user_id: uuid::Uuid::new_v4().to_string(), // Random id
                project_id: project_id.to_string(),
                permission: Permission::Read as i32,
                service_account: false,
            }),
        }),
        common::oidc::ADMINTOKEN,
    );

    // Send request to gRPC endpoint of AOS instance
    let add_user_response = project_service.add_user_to_project(add_user_request).await;

    // Validate that user could not get added as it does not exist
    assert!(add_user_response.is_err());

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

    // Try to add user with Unspecified permission which should fail
    let add_user_request = common::grpc_helpers::add_token(
        tonic::Request::new(AddUserToProjectRequest {
            project_id: project_id.to_string(),
            user_permission: Some(ProjectPermission {
                user_id: user_id.to_string(),
                project_id: project_id.to_string(),
                permission: Permission::Unspecified as i32,
                service_account: false,
            }),
        }),
        common::oidc::ADMINTOKEN,
    );
    let add_user_response = project_service.add_user_to_project(add_user_request).await;

    // Validate that user could not get added with Unspecified permission
    assert!(add_user_response.is_err());

    // Add user initially with None permission to project
    let add_user_request = common::grpc_helpers::add_token(
        tonic::Request::new(AddUserToProjectRequest {
            project_id: project_id.to_string(),
            user_permission: Some(ProjectPermission {
                user_id: user_id.to_string(),
                project_id: project_id.to_string(),
                permission: Permission::None as i32,
                service_account: false,
            }),
        }),
        common::oidc::ADMINTOKEN,
    );
    let add_user_response = project_service.add_user_to_project(add_user_request).await;

    // Validate that user was added successfully
    assert!(add_user_response.is_ok());

    let get_permission_request = common::grpc_helpers::add_token(
        tonic::Request::new(GetUserPermissionsForProjectRequest {
            project_id: project_id.to_string(),
            user_id: user_id.to_string(),
        }),
        common::oidc::ADMINTOKEN,
    );
    let get_permission_response = project_service
        .get_user_permissions_for_project(get_permission_request)
        .await
        .unwrap()
        .into_inner()
        .user_permission
        .unwrap();

    assert_eq!(get_permission_response.user_id, user_id);
    assert_eq!(get_permission_response.project_id, project_id);
    assert_eq!(
        get_permission_response.display_name,
        "regular_user".to_string()
    );
    assert_eq!(get_permission_response.permission, 1);

    // Add/remove user with all possible permissions
    for (permission, enum_val) in vec![
        (Permission::Read, 2),
        (Permission::Append, 3),
        (Permission::Modify, 4),
        (Permission::Admin, 5),
    ]
    .iter()
    {
        // Remove user from project
        let remove_permission_request = common::grpc_helpers::add_token(
            tonic::Request::new(RemoveUserFromProjectRequest {
                project_id: project_id.to_string(),
                user_id: user_id.to_string(),
            }),
            common::oidc::ADMINTOKEN,
        );
        let remove_permission_response = project_service
            .remove_user_from_project(remove_permission_request)
            .await;
        assert!(remove_permission_response.is_ok());

        // Add user to project with other permissions
        let add_user_request = common::grpc_helpers::add_token(
            tonic::Request::new(AddUserToProjectRequest {
                project_id: project_id.to_string(),
                user_permission: Some(ProjectPermission {
                    user_id: user_id.to_string(),
                    project_id: project_id.to_string(),
                    permission: *permission as i32,
                    service_account: false,
                }),
            }),
            common::oidc::ADMINTOKEN,
        );
        let add_user_response = project_service.add_user_to_project(add_user_request).await;

        // Validate that user was added successfully
        assert!(add_user_response.is_ok());

        let get_permission_request = common::grpc_helpers::add_token(
            tonic::Request::new(GetUserPermissionsForProjectRequest {
                project_id: project_id.to_string(),
                user_id: user_id.to_string(),
            }),
            common::oidc::ADMINTOKEN,
        );
        let get_permission_response = project_service
            .get_user_permissions_for_project(get_permission_request)
            .await
            .unwrap()
            .into_inner()
            .user_permission
            .unwrap();

        assert_eq!(get_permission_response.user_id, user_id);
        assert_eq!(get_permission_response.project_id, project_id);
        assert_eq!(
            get_permission_response.display_name,
            "regular_user".to_string()
        );
        assert_eq!(get_permission_response.permission, *enum_val);
    }

    // Try to add user twice with different permissions to project
    let add_user_request = common::grpc_helpers::add_token(
        tonic::Request::new(AddUserToProjectRequest {
            project_id: project_id.to_string(),
            user_permission: Some(ProjectPermission {
                user_id: user_id.to_string(),
                project_id: project_id.to_string(),
                permission: Permission::Read as i32,
                service_account: false,
            }),
        }),
        common::oidc::ADMINTOKEN,
    );
    let add_user_response = project_service.add_user_to_project(add_user_request).await;

    // Validate that request failed
    assert!(add_user_response.is_err());
}

#[ignore]
#[tokio::test]
#[serial(db)]
async fn edit_project_user_grpc_test() {
    // Init user/project services
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone(), ArunaServerConfig::default()).await);
    let user_service = UserServiceImpl::new(db.clone(), authz.clone()).await;
    let project_service = ProjectServiceImpl::new(db, authz).await;

    // Fast track project creation
    let project_id = common::functions::create_project(None).id;

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

    // Create gRPC Request to initially add user to project
    let add_user_request = common::grpc_helpers::add_token(
        tonic::Request::new(AddUserToProjectRequest {
            project_id: project_id.to_string(),
            user_permission: Some(ProjectPermission {
                user_id: user_id.to_string(),
                project_id: project_id.to_string(),
                permission: Permission::None as i32,
                service_account: false,
            }),
        }),
        common::oidc::ADMINTOKEN,
    );
    let add_user_response = project_service.add_user_to_project(add_user_request).await;

    // Validate that user could get added even as it is not activated
    assert!(add_user_response.is_ok());

    // Create gRPC Request to edit users project permissions with non-authorized token
    let edit_user_request = common::grpc_helpers::add_token(
        tonic::Request::new(EditUserPermissionsForProjectRequest {
            project_id: project_id.to_string(),
            user_permission: Some(ProjectPermission {
                user_id: user_id.to_string(),
                project_id: project_id.to_string(),
                permission: Permission::Admin as i32,
                service_account: false,
            }),
        }),
        common::oidc::REGULARTOKEN,
    );
    let edit_user_response = project_service
        .edit_user_permissions_for_project(edit_user_request)
        .await;

    // Validate that
    assert!(edit_user_response.is_err());

    // Create gPC Request to edit user who is not member of project / does not exist
    let edit_user_request = common::grpc_helpers::add_token(
        tonic::Request::new(EditUserPermissionsForProjectRequest {
            project_id: project_id.to_string(),
            user_permission: Some(ProjectPermission {
                user_id: uuid::Uuid::new_v4().to_string(),
                project_id: project_id.to_string(),
                permission: Permission::Admin as i32,
                service_account: false,
            }),
        }),
        common::oidc::ADMINTOKEN,
    );
    let edit_user_response = project_service
        .edit_user_permissions_for_project(edit_user_request)
        .await;

    // Validate that the request succeeds having done nothing as the update just returns Ok for 0 rows
    assert!(edit_user_response.is_ok());

    // Edit user permission once to all available permissions
    for (permission, enum_val) in vec![
        (Permission::None, 1),
        (Permission::Read, 2),
        (Permission::Append, 3),
        (Permission::Modify, 4),
        (Permission::Admin, 5),
    ]
    .iter()
    {
        // Create gPC Request to edit permissions to None
        let edit_user_request = common::grpc_helpers::add_token(
            tonic::Request::new(EditUserPermissionsForProjectRequest {
                project_id: project_id.to_string(),
                user_permission: Some(ProjectPermission {
                    user_id: user_id.to_string(),
                    project_id: project_id.to_string(),
                    permission: *permission as i32,
                    service_account: false,
                }),
            }),
            common::oidc::ADMINTOKEN,
        );
        let edit_user_response = project_service
            .edit_user_permissions_for_project(edit_user_request)
            .await;

        // Validate that the request succeeds as expected
        assert!(edit_user_response.is_ok());

        let get_permission_request = common::grpc_helpers::add_token(
            tonic::Request::new(GetUserPermissionsForProjectRequest {
                project_id: project_id.to_string(),
                user_id: user_id.to_string(),
            }),
            common::oidc::ADMINTOKEN,
        );
        let get_permission_response = project_service
            .get_user_permissions_for_project(get_permission_request)
            .await
            .unwrap()
            .into_inner()
            .user_permission
            .unwrap();

        assert_eq!(get_permission_response.user_id, user_id);
        assert_eq!(get_permission_response.project_id, project_id);
        assert_eq!(
            get_permission_response.display_name,
            "regular_user".to_string()
        );
        assert_eq!(get_permission_response.permission, *enum_val);
    }
}
