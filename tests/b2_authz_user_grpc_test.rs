use std::sync::Arc;

use aruna_rust_api::api::storage::services::v1::{
    user_service_server::UserService, ActivateUserRequest, CreateApiTokenRequest,
    RegisterUserRequest,
};
use aruna_server::{
    database::{self},
    server::services::authz::Authz,
    server::services::user::UserServiceImpl,
};
use serial_test::serial;
use tonic::{
    metadata::{AsciiMetadataKey, AsciiMetadataValue},
    Code,
};
mod common;

#[ignore]
#[tokio::test]
#[serial(db)]
async fn register_user_grpc_test() {
    // Init services
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone()).await);
    let userservice = UserServiceImpl::new(db, authz).await;

    // Test
    let req = tonic::Request::new(RegisterUserRequest {
        display_name: "This is a test user".to_string(),
    });

    let resp = userservice.register_user(req).await;

    // Should be error without metadata
    assert!(resp.is_err());

    // Expired test

    let mut req = tonic::Request::new(RegisterUserRequest {
        display_name: "This is a test user".to_string(),
    });

    let metadata = req.metadata_mut();
    metadata.append(
        AsciiMetadataKey::from_bytes("Authorization".as_bytes()).unwrap(),
        AsciiMetadataValue::try_from(format!("Bearer {}", common::oidc::REGULAREXPIRED)).unwrap(),
    );

    let resp = userservice.register_user(req).await;

    assert!(resp.is_err());

    // Invalid token test
    let mut req = tonic::Request::new(RegisterUserRequest {
        display_name: "This is a test user".to_string(),
    });

    let metadata = req.metadata_mut();
    metadata.append(
        AsciiMetadataKey::from_bytes("Authorization".as_bytes()).unwrap(),
        AsciiMetadataValue::try_from(format!("Bearer {}", common::oidc::REGULARINVALID)).unwrap(),
    );

    let resp = userservice.register_user(req).await;

    assert!(resp.is_err());

    // Real Test
    let mut req = tonic::Request::new(RegisterUserRequest {
        display_name: "This is a test user".to_string(),
    });

    let metadata = req.metadata_mut();
    metadata.append(
        AsciiMetadataKey::from_bytes("Authorization".as_bytes()).unwrap(),
        AsciiMetadataValue::try_from(format!("Bearer {}", common::oidc::REGULAROIDC)).unwrap(),
    );

    let resp = userservice.register_user(req).await;

    println!("{:#?}", resp);
    assert!(resp.is_ok());
    assert!(!resp.unwrap().into_inner().user_id.is_empty());
}

#[ignore]
#[tokio::test]
#[serial(db)]
async fn activate_user_grpc_test() {
    // Init services
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone()).await);
    let userservice = UserServiceImpl::new(db, authz).await;

    // FAILED Test
    let mut req = tonic::Request::new(ActivateUserRequest {
        user_id: "ee4e1d0b-abab-4979-a33e-dc28ed199b17".to_string(),
    });

    let metadata = req.metadata_mut();
    metadata.append(
        AsciiMetadataKey::from_bytes("Authorization".as_bytes()).unwrap(),
        AsciiMetadataValue::try_from(format!("Bearer {}", common::oidc::REGULAROIDC)).unwrap(),
    );

    let resp = userservice.activate_user(req).await;

    // Should fail -> wrong token
    assert!(resp.is_err());

    // FAILED Test -> ADMIN OIDC TOKEN
    let mut req = tonic::Request::new(ActivateUserRequest {
        user_id: "ee4e1d0b-abab-4979-a33e-dc28ed199b17".to_string(),
    });

    let metadata = req.metadata_mut();
    metadata.append(
        AsciiMetadataKey::from_bytes("Authorization".as_bytes()).unwrap(),
        AsciiMetadataValue::try_from(format!("Bearer {}", common::oidc::ADMINOIDC)).unwrap(),
    );

    let resp = userservice.activate_user(req).await;

    // Should fail -> wrong token
    assert!(resp.is_err());

    // FAILED Test -> ADMIN OIDC TOKEN
    let mut req = tonic::Request::new(ActivateUserRequest {
        user_id: "ee4e1d0b-abab-4979-a33e-dc28ed199b17".to_string(),
    });

    let metadata = req.metadata_mut();
    metadata.append(
        AsciiMetadataKey::from_bytes("Authorization".as_bytes()).unwrap(),
        AsciiMetadataValue::try_from(format!("Bearer {}", common::oidc::ADMINTOKEN)).unwrap(),
    );

    let resp = userservice.activate_user(req).await;

    println!("{:#?}", resp);
    // Should not fail -> correct token
    assert!(resp.is_ok());
}

#[ignore]
#[tokio::test]
#[serial(db)]
async fn create_api_token_grpc_test() {
    // Init services
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone()).await);
    let userservice = UserServiceImpl::new(db, authz).await;

    // Working test
    let mut req = tonic::Request::new(CreateApiTokenRequest {
        project_id: "".to_string(),
        collection_id: "".to_string(),
        name: "test_token".to_string(),
        expires_at: None,
        permission: 0,
    });

    let metadata = req.metadata_mut();
    metadata.append(
        AsciiMetadataKey::from_bytes("Authorization".as_bytes()).unwrap(),
        AsciiMetadataValue::try_from(format!("Bearer {}", common::oidc::ADMINTOKEN)).unwrap(),
    );

    let resp = userservice.create_api_token(req).await;

    assert!(resp.is_ok());
    let resp = resp.unwrap().into_inner();
    assert!(resp.token_secret != "".to_string());
    let tok = resp.token.unwrap();
    assert_eq!(tok.name.to_string(), "test_token".to_string());
    assert_eq!(tok.collection_id, "".to_string());

    // Broken test with collection and project
    let mut req = tonic::Request::new(CreateApiTokenRequest {
        project_id: "bd62af97-6bf9-40b4-929a-686b417b8be7".to_string(),
        collection_id: "06f2c757-4c69-43f8-af82-7bd3e321ad9e".to_string(),
        name: "test_token_broken".to_string(),
        expires_at: None,
        permission: 0,
    });

    let metadata = req.metadata_mut();
    metadata.append(
        AsciiMetadataKey::from_bytes("Authorization".as_bytes()).unwrap(),
        AsciiMetadataValue::try_from(format!("Bearer {}", common::oidc::ADMINTOKEN)).unwrap(),
    );

    let resp = userservice.create_api_token(req).await;

    assert!(resp.is_err());
    assert_eq!(resp.err().unwrap().code(), Code::InvalidArgument);

    // Should work: Token for foreign collection (as_admin)
    let mut req = tonic::Request::new(CreateApiTokenRequest {
        project_id: "12345678-1111-1111-1111-111111111122".to_string(),
        collection_id: "".to_string(),
        name: "test_token_foreign".to_string(),
        expires_at: None,
        permission: 4,
    });

    let metadata = req.metadata_mut();
    metadata.append(
        AsciiMetadataKey::from_bytes("Authorization".as_bytes()).unwrap(),
        AsciiMetadataValue::try_from(format!("Bearer {}", common::oidc::ADMINTOKEN)).unwrap(),
    );

    let resp = userservice.create_api_token(req).await;

    println!("{:#?}", resp);
    assert!(resp.is_ok());
    let resp = resp.unwrap().into_inner();
    assert!(resp.token_secret != "".to_string());
    let tok = resp.token.unwrap();
    assert_eq!(tok.name.to_string(), "test_token_foreign".to_string());
    assert_eq!(tok.collection_id, "".to_string());

    // Should fail for "regular user": Token for foreign collection (as_admin)
    let mut req = tonic::Request::new(CreateApiTokenRequest {
        project_id: "12345678-1111-1111-1111-111111111122".to_string(),
        collection_id: "".to_string(),
        name: "test_token_foreign".to_string(),
        expires_at: None,
        permission: 4,
    });

    let metadata = req.metadata_mut();
    metadata.append(
        AsciiMetadataKey::from_bytes("Authorization".as_bytes()).unwrap(),
        AsciiMetadataValue::try_from(format!("Bearer {}", common::oidc::REGULARTOKEN)).unwrap(),
    );

    let resp = userservice.create_api_token(req).await;

    println!("{:#?}", resp);
    assert!(resp.is_err());
    assert_eq!(resp.err().unwrap().code(), Code::PermissionDenied);

    // Should fail for "regular user": OIDCToken for foreign collection
    let mut req = tonic::Request::new(CreateApiTokenRequest {
        project_id: "12345678-1111-1111-1111-111111111122".to_string(),
        collection_id: "".to_string(),
        name: "test_token_foreign_fail".to_string(),
        expires_at: None,
        permission: 4,
    });

    let metadata = req.metadata_mut();
    metadata.append(
        AsciiMetadataKey::from_bytes("Authorization".as_bytes()).unwrap(),
        AsciiMetadataValue::try_from(format!("Bearer {}", common::oidc::REGULAROIDC)).unwrap(),
    );

    let resp = userservice.create_api_token(req).await;

    println!("{:#?}", resp);
    assert!(resp.is_err());
    assert_eq!(resp.err().unwrap().code(), Code::InvalidArgument);

    // Should work for "regular user": OIDCToken for personal token
    let mut req = tonic::Request::new(CreateApiTokenRequest {
        project_id: "".to_string(),
        collection_id: "".to_string(),
        name: "test_personal_oidc".to_string(),
        expires_at: None,
        permission: 4,
    });

    let metadata = req.metadata_mut();
    metadata.append(
        AsciiMetadataKey::from_bytes("Authorization".as_bytes()).unwrap(),
        AsciiMetadataValue::try_from(format!("Bearer {}", common::oidc::REGULAROIDC)).unwrap(),
    );

    let resp = userservice.create_api_token(req).await;

    println!("{:#?}", resp);
    assert!(resp.is_ok());
    let resp = resp.unwrap().into_inner();
    assert!(resp.token_secret != "".to_string());
    let tok = resp.token.unwrap();
    assert_eq!(tok.name.to_string(), "test_personal_oidc".to_string());
    assert_eq!(tok.collection_id, "".to_string());
    assert_eq!(tok.project_id, "".to_string());
}
