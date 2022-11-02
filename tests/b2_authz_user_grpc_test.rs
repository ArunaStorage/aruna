use std::sync::Arc;

use aruna_rust_api::api::storage::services::v1::{
    user_service_server::UserService, RegisterUserRequest,
};
use aruna_server::{
    database::{self},
    server::services::authz::Authz,
    server::services::user::UserServiceImpl,
};
use serial_test::serial;
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

    // Test
    let mut req = tonic::Request::new(RegisterUserRequest {
        display_name: "This is a test user".to_string(),
    });

    req.metadata_mut().append(
        "Authorization",
        format!("Bearer {}", common::oidc::REGULAROIDC)
            .parse()
            .unwrap(),
    );

    let resp = userservice.register_user(req).await;

    assert!(resp.is_ok());
    assert!(resp.unwrap().into_inner().user_id != "");
}
