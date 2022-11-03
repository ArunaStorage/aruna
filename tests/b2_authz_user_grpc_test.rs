use std::sync::Arc;

use aruna_rust_api::api::storage::services::v1::{
    user_service_server::UserService, ActivateUserRequest, RegisterUserRequest,
};
use aruna_server::{
    database::{self},
    server::services::authz::Authz,
    server::services::user::UserServiceImpl,
};
use serial_test::serial;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue};
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
