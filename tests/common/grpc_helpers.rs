use std::str::FromStr;
use std::sync::Arc;

use aruna_rust_api::api::storage::models::v1::{
    CollectionOverview, Permission, ProjectPermission, ProjectPermissionDisplayName,
};
use aruna_rust_api::api::storage::services::v1::collection_service_server::CollectionService;
use aruna_rust_api::api::storage::services::v1::object_service_server::ObjectService;
use aruna_rust_api::api::storage::services::v1::project_service_server::ProjectService;
use aruna_rust_api::api::storage::services::v1::{
    AddUserToProjectRequest, EditUserPermissionsForProjectRequest, GetCollectionByIdRequest,
    GetObjectByIdRequest, GetUserPermissionsForProjectRequest, ObjectWithUrl,
};
use aruna_rust_api::api::storage::{
    models::v1::Token,
    services::v1::{
        user_service_server::UserService, CreateApiTokenRequest, ExpiresAt, GetUserRequest,
    },
};
use aruna_server::config::ArunaServerConfig;
use aruna_server::server::services::collection::CollectionServiceImpl;
use aruna_server::server::services::object::ObjectServiceImpl;
use aruna_server::server::services::project::ProjectServiceImpl;
use aruna_server::{
    database,
    server::services::{authz::Authz, user::UserServiceImpl},
};
use chrono::Utc;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue};

#[allow(dead_code)]
pub fn add_token<T>(mut req: tonic::Request<T>, token: &str) -> tonic::Request<T> {
    let metadata = req.metadata_mut();
    metadata.append(
        AsciiMetadataKey::from_bytes("Authorization".as_bytes()).unwrap(),
        AsciiMetadataValue::try_from(format!("Bearer {}", token)).unwrap(),
    );
    req
}

#[allow(dead_code)]
#[derive(Default)]
pub struct TCreateToken {
    name: String,
    expires_at: i64,
    permission: i32,
    project_id: String,
    collection_id: String,
    basetoken: String,
}

#[allow(dead_code)]
pub async fn create_api_token(create_token: &TCreateToken) -> Token {
    // Init services
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone(), ArunaServerConfig::default()).await);
    let userservice = UserServiceImpl::new(db, authz, None).await;

    let tokname = if create_token.name.is_empty() {
        format!("token_{}", super::functions::rand_string(5))
    } else {
        create_token.name.to_string()
    };

    let expires = if create_token.expires_at == 0 {
        Utc::now().timestamp() + 111111111
    } else {
        create_token.expires_at
    };

    let token = if create_token.basetoken.is_empty() {
        super::oidc::REGULARTOKEN.to_string()
    } else {
        create_token.basetoken.to_string()
    };

    // First Create a token
    let req = add_token(
        tonic::Request::new(CreateApiTokenRequest {
            project_id: create_token.project_id.to_string(),
            collection_id: create_token.collection_id.to_string(),
            name: tokname.to_string(),
            expires_at: Some(ExpiresAt {
                timestamp: Some(prost_types::Timestamp {
                    seconds: expires,
                    nanos: 0,
                }),
            }),
            permission: create_token.permission,
            is_session: false,
        }),
        &token,
    );

    let resp = userservice.create_api_token(req).await;

    // Check the token
    let tok = resp.unwrap().into_inner().token.unwrap();
    assert_eq!(tok.name, tokname);
    tok
}

#[allow(dead_code)]
pub async fn get_token_user_id(token: &str) -> String {
    // Init user service
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone(), ArunaServerConfig::default()).await);
    let user_service = UserServiceImpl::new(db, authz, None).await;

    // Fetch user information associated with token
    let get_user_request = add_token(
        tonic::Request::new(GetUserRequest {
            user_id: "".to_string(),
        }),
        token,
    );

    let get_user_response = user_service
        .get_user(get_user_request)
        .await
        .unwrap()
        .into_inner();

    get_user_response.user.unwrap().id
}

/// Fast track permission edit.
#[allow(dead_code)]
pub async fn add_project_permission(
    project_uuid: &str,
    user_uuid: &str,
    auth_token: &str,
) -> ProjectPermissionDisplayName {
    // Init project service
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone(), ArunaServerConfig::default()).await);
    let project_service = ProjectServiceImpl::new(db, authz, None).await;

    // Validate format of provided ids
    let project_id = diesel_ulid::DieselUlid::from_str(project_uuid).unwrap();
    let user_id = diesel_ulid::DieselUlid::from_str(user_uuid).unwrap();

    // Edit permissions through gRPC request
    let add_perm_request = add_token(
        tonic::Request::new(AddUserToProjectRequest {
            project_id: project_id.to_string(),
            user_permission: Some(ProjectPermission {
                user_id: user_id.to_string(),
                project_id: project_id.to_string(),
                permission: Permission::None as i32,
                service_account: false,
            }),
        }),
        auth_token,
    );

    let add_perm_response = project_service.add_user_to_project(add_perm_request).await;
    assert!(add_perm_response.is_ok());

    // Validate permission is set in database
    let get_perm_request = add_token(
        tonic::Request::new(GetUserPermissionsForProjectRequest {
            project_id: project_id.to_string(),
            user_id: user_id.to_string(),
        }),
        auth_token,
    );

    let response = project_service
        .get_user_permissions_for_project(get_perm_request)
        .await;

    let permission = response.unwrap().into_inner().user_permission.unwrap();
    assert_eq!(permission.permission, Permission::None as i32);

    // Always true :+1:
    permission
}

/// Fast track permission edit.
#[allow(dead_code)]
pub async fn edit_project_permission(
    project_uuid: &str,
    user_uuid: &str,
    new_perm: &Permission,
    auth_token: &str,
) -> ProjectPermissionDisplayName {
    // Init project service
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone(), ArunaServerConfig::default()).await);
    let project_service = ProjectServiceImpl::new(db, authz, None).await;

    // Validate format of provided ids
    let project_id = diesel_ulid::DieselUlid::from_str(project_uuid).unwrap();
    let user_id = diesel_ulid::DieselUlid::from_str(user_uuid).unwrap();

    // Edit permissions through gRPC request
    let edit_perm_request = add_token(
        tonic::Request::new(EditUserPermissionsForProjectRequest {
            project_id: project_id.to_string(),
            user_permission: Some(ProjectPermission {
                user_id: user_id.to_string(),
                project_id: project_id.to_string(),
                permission: *new_perm as i32,
                service_account: false,
            }),
        }),
        auth_token,
    );

    let edit_perm_response = project_service
        .edit_user_permissions_for_project(edit_perm_request)
        .await;

    assert!(edit_perm_response.is_ok());

    // Validate permission is set in database
    let get_perm_request = add_token(
        tonic::Request::new(GetUserPermissionsForProjectRequest {
            project_id: project_id.to_string(),
            user_id: user_id.to_string(),
        }),
        auth_token,
    );

    let response = project_service
        .get_user_permissions_for_project(get_perm_request)
        .await;

    let permission = response.unwrap().into_inner().user_permission.unwrap();
    assert_eq!(permission.permission, *new_perm as i32);

    // Always true :+1:
    permission
}

/// Fast track collection fetching
#[allow(dead_code)]
pub async fn try_get_collection(
    collection_uuid: String,
    auth_token: &str,
) -> Option<CollectionOverview> {
    // Init database connection
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone(), ArunaServerConfig::default()).await);

    // Init collection service
    let collection_service = CollectionServiceImpl::new(db, authz, None).await;

    // Validate format of provided ids
    let collection_id = diesel_ulid::DieselUlid::from_str(collection_uuid.as_str()).unwrap();

    // Get collection with gRPC request
    let get_collection_request = add_token(
        tonic::Request::new(GetCollectionByIdRequest {
            collection_id: collection_id.to_string(),
        }),
        auth_token,
    );
    let get_collection_response = collection_service
        .get_collection_by_id(get_collection_request)
        .await;

    if get_collection_response.is_err() {
        None
    } else {
        get_collection_response.unwrap().into_inner().collection
    }
}

/// Fast track object fetching
#[allow(dead_code)]
pub async fn try_get_object(
    collection_uuid: String,
    object_uuid: String,
    auth_token: &str,
) -> Option<ObjectWithUrl> {
    // Init database connection
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

    // Validate format of provided ids
    let collection_id = diesel_ulid::DieselUlid::from_str(collection_uuid.as_str()).unwrap();
    let object_id = diesel_ulid::DieselUlid::from_str(object_uuid.as_str()).unwrap();

    // Get object with gRPC request
    let get_object_request = add_token(
        tonic::Request::new(GetObjectByIdRequest {
            collection_id: collection_id.to_string(),
            object_id: object_id.to_string(),
            with_url: false,
        }),
        auth_token,
    );
    let get_object_response = object_service.get_object_by_id(get_object_request).await;

    if get_object_response.is_err() {
        None
    } else {
        get_object_response.unwrap().into_inner().object
    }
}
