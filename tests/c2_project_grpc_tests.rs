use aruna_rust_api::api::storage::models::v1::{Permission, ProjectPermission};
use aruna_rust_api::api::storage::services::v1::project_service_server::ProjectService;
use aruna_rust_api::api::storage::services::v1::user_service_server::UserService;
use aruna_rust_api::api::storage::services::v1::{
    AddUserToProjectRequest, CreateProjectRequest, GetProjectRequest, GetProjectsRequest,
    GetUserPermissionsForProjectRequest, RegisterUserRequest, RemoveUserFromProjectRequest,
};
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
    let authz = Arc::new(Authz::new(db.clone()).await);
    let project_service = ProjectServiceImpl::new(db, authz).await;

    // Create gPC Request for project creation
    let create_project_request = common::grpc_helpers::add_token(
        tonic::Request::new(CreateProjectRequest {
            name: "Test Project".to_string(),
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
    let authz = Arc::new(Authz::new(db.clone()).await);
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
async fn add_remove_project_user_grpc_test() {
    // Init user/project services
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone()).await);
    let user_service = UserServiceImpl::new(db.clone(), authz.clone()).await;
    let project_service = ProjectServiceImpl::new(db, authz).await;

    // Fast track project creation
    let project_id = common::functions::create_project(None).id;

    // Create gPC Request to add user with permissions
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

    // Validate that user could not get added as it is not registered
    assert!(add_user_response.is_err());

    // Register user ...
    let register_user_request = common::grpc_helpers::add_token(
        tonic::Request::new(RegisterUserRequest {
            display_name: "Rando Man 001".to_string(),
        }),
        common::oidc::REGULAROIDC,
    );
    let register_user_response = user_service.register_user(register_user_request).await;
    assert!(register_user_response.is_ok());

    let user_id_001 = register_user_response.unwrap().into_inner().user_id;

    // ... and try again
    let add_user_request = common::grpc_helpers::add_token(
        tonic::Request::new(AddUserToProjectRequest {
            project_id: project_id.to_string(),
            user_permission: Some(ProjectPermission {
                user_id: user_id_001.to_string(),
                project_id: project_id.to_string(),
                permission: Permission::Read as i32,
                service_account: false,
            }),
        }),
        common::oidc::ADMINTOKEN,
    );

    let add_user_response = project_service.add_user_to_project(add_user_request).await;

    // Validate that user could get added even as it is not activated
    assert!(add_user_response.is_ok());

    // Try to add user again with other permissions --> Should Error
    let add_user_request = common::grpc_helpers::add_token(
        tonic::Request::new(AddUserToProjectRequest {
            project_id: project_id.to_string(),
            user_permission: Some(ProjectPermission {
                user_id: user_id_001.to_string(),
                project_id: project_id.to_string(),
                permission: Permission::Modify as i32,
                service_account: false,
            }),
        }),
        common::oidc::ADMINTOKEN,
    );

    let add_user_response = project_service.add_user_to_project(add_user_request).await;

    // Validate that user could not get added twice to the project
    assert!(add_user_response.is_err());

    // Try to add some user without permissions -> Should Error
    let add_user_request = common::grpc_helpers::add_token(
        tonic::Request::new(AddUserToProjectRequest {
            project_id: project_id.to_string(),
            user_permission: None,
        }),
        common::oidc::ADMINTOKEN,
    );
    let add_user_response = project_service.add_user_to_project(add_user_request).await;

    // Validate that user could not get added without permissions
    assert!(add_user_response.is_err());

    // Add another user and validate correct creation/assignment
    let register_user_request = common::grpc_helpers::add_token(
        tonic::Request::new(RegisterUserRequest {
            display_name: "Rando Man 002".to_string(),
        }),
        common::oidc::REGULAROIDC,
    );
    let register_user_response = user_service.register_user(register_user_request).await;
    assert!(register_user_response.is_ok());

    let user_id_002 = register_user_response.unwrap().into_inner().user_id;

    let add_user_request = common::grpc_helpers::add_token(
        tonic::Request::new(AddUserToProjectRequest {
            project_id: project_id.to_string(),
            user_permission: Some(ProjectPermission {
                user_id: user_id_002.to_string(),
                project_id: project_id.to_string(),
                permission: Permission::Modify as i32,
                service_account: false,
            }),
        }),
        common::oidc::ADMINTOKEN,
    );

    let add_user_response = project_service.add_user_to_project(add_user_request).await;
    assert!(add_user_response.is_ok());

    let get_project_request = common::grpc_helpers::add_token(
        tonic::Request::new(GetProjectRequest {
            project_id: project_id.to_string(),
        }),
        common::oidc::ADMINTOKEN,
    );
    let get_project_response = project_service
        .get_project(get_project_request)
        .await
        .unwrap()
        .into_inner();
    let project_users = get_project_response.project.unwrap().user_ids;

    assert!(project_users.contains(&user_id_001));
    assert!(project_users.contains(&user_id_002));

    // validate user 001
    let get_permission_request = common::grpc_helpers::add_token(
        tonic::Request::new(GetUserPermissionsForProjectRequest {
            project_id: project_id.to_string(),
            user_id: user_id_001.to_string(),
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

    assert_eq!(get_permission_response.user_id, user_id_001);
    assert_eq!(get_permission_response.project_id, project_id);
    assert_eq!(
        get_permission_response.display_name,
        "Rando Man 001".to_string()
    );
    //assert!(matches!(UserRights::from_i32(get_permission_response.permission).unwrap(), UserRights::READ));
    assert_eq!(get_permission_response.permission, 2);

    // validate user 002
    let get_permission_request = common::grpc_helpers::add_token(
        tonic::Request::new(GetUserPermissionsForProjectRequest {
            project_id: project_id.to_string(),
            user_id: user_id_002.to_string(),
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

    assert_eq!(get_permission_response.user_id, user_id_002);
    assert_eq!(get_permission_response.project_id, project_id);
    assert_eq!(
        get_permission_response.display_name,
        "Rando Man 002".to_string()
    );
    assert_eq!(get_permission_response.permission, 4);

    // Remove user permissions from project
    let remove_user_request = common::grpc_helpers::add_token(
        tonic::Request::new(RemoveUserFromProjectRequest {
            project_id: project_id.to_string(),
            user_id: user_id_001.to_string(),
        }),
        common::oidc::ADMINTOKEN,
    );

    let remove_user_response = project_service
        .remove_user_from_project(remove_user_request)
        .await;

    assert!(remove_user_response.is_ok());

    let remove_user_request = common::grpc_helpers::add_token(
        tonic::Request::new(RemoveUserFromProjectRequest {
            project_id: project_id.to_string(),
            user_id: user_id_002.to_string(),
        }),
        common::oidc::ADMINTOKEN,
    );

    let remove_user_response = project_service
        .remove_user_from_project(remove_user_request)
        .await;

    assert!(remove_user_response.is_ok());
}
