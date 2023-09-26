use std::str::FromStr;

use aruna_rust_api::api::storage::{
    models::v2::{permission::ResourceId, Permission, PermissionLevel},
    services::v2::{
        authorization_service_server::AuthorizationService, user_service_server::UserService,
        CreateAuthorizationRequest, GetUserRequest,
    },
};
use aruna_server::database::{
    crud::CrudDb,
    dsls::user_dsl::User,
    enums::{DbPermissionLevel, ObjectMapping},
};
use diesel_ulid::DieselUlid;

use crate::common::{
    init::init_service_block,
    test_utils::{add_token, fast_track_grpc_project_create, ADMIN_OIDC_TOKEN, USER_OIDC_TOKEN},
};

#[tokio::test]
async fn grpc_create_authorization() {
    // Init gRPC services
    let service_block = init_service_block().await;

    // Create random project
    let project =
        fast_track_grpc_project_create(&service_block.project_service, ADMIN_OIDC_TOKEN).await;
    let project_ulid = DieselUlid::from_str(&project.id).unwrap();

    // Get normal user
    let user = service_block
        .user_service
        .get_user(add_token(
            tonic::Request::new(GetUserRequest {
                user_id: "".to_string(),
            }),
            USER_OIDC_TOKEN,
        ))
        .await
        .unwrap()
        .into_inner()
        .user
        .unwrap();

    // User does not have permission on created project
    assert!(!user
        .attributes
        .unwrap()
        .personal_permissions
        .contains(&Permission {
            permission_level: PermissionLevel::Admin as i32,
            resource_id: Some(ResourceId::ProjectId(project.id.clone()))
        }));

    //ToDo: Add permission to user who does not exist
    let mut inner_request = CreateAuthorizationRequest {
        resource_id: project.id.clone(),
        user_id: DieselUlid::generate().to_string(),
        permission_level: PermissionLevel::Read as i32,
    };

    let grpc_request = add_token(tonic::Request::new(inner_request.clone()), ADMIN_OIDC_TOKEN);

    let response = service_block
        .auth_service
        .create_authorization(grpc_request)
        .await;

    assert!(response.is_err());

    //ToDo: Add permission to user without token
    inner_request.user_id = user.id.clone();

    let response = service_block
        .auth_service
        .create_authorization(tonic::Request::new(inner_request.clone()))
        .await;

    assert!(response.is_err());

    //ToDo: Add permission to user without sufficient permission
    let grpc_request = add_token(tonic::Request::new(inner_request.clone()), USER_OIDC_TOKEN);

    let response = service_block
        .auth_service
        .create_authorization(grpc_request)
        .await;

    assert!(response.is_err());

    //ToDo: Add permission to user
    let grpc_request = add_token(tonic::Request::new(inner_request.clone()), ADMIN_OIDC_TOKEN);

    let response = service_block
        .auth_service
        .create_authorization(grpc_request)
        .await
        .unwrap()
        .into_inner();

    // Fetch updated user from cache and database
    let cache_user = service_block
        .cache
        .get_user(&DieselUlid::from_str(&user.id).unwrap())
        .unwrap();
    let db_user = User::get(
        DieselUlid::from_str(&user.id).unwrap(),
        &service_block.db_conn.get_client().await.unwrap(),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(response.user_id, cache_user.id.to_string());
    assert_eq!(response.user_id, db_user.id.to_string());
    assert_eq!(
        cache_user
            .attributes
            .0
            .permissions
            .get(&project_ulid)
            .unwrap()
            .value(),
        &ObjectMapping::PROJECT(DbPermissionLevel::READ)
    );

    assert_eq!(response.user_name, cache_user.display_name);
    assert_eq!(response.user_name, db_user.display_name);
    assert_eq!(
        db_user
            .attributes
            .0
            .permissions
            .get(&project_ulid)
            .unwrap()
            .value(),
        &ObjectMapping::PROJECT(DbPermissionLevel::READ)
    );

    assert_eq!(response.resource_id, project.id);
    assert_eq!(response.permission_level(), PermissionLevel::Read);
}

#[tokio::test]
async fn grpc_get_authorization() {}

#[tokio::test]
async fn grpc_delete_authorization() {}
