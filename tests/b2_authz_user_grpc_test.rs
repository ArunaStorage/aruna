use std::sync::Arc;

use aruna_rust_api::api::storage::services::v1::{
    user_service_server::UserService, ActivateUserRequest, CreateApiTokenRequest,
    GetApiTokenRequest, GetUserRequest, RegisterUserRequest,
};
use aruna_server::{
    database::{self},
    server::services::authz::Authz,
    server::services::user::UserServiceImpl,
};
use serial_test::serial;
use tonic::Code;
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

    let req = common::grpc_helpers::add_token(
        tonic::Request::new(RegisterUserRequest {
            display_name: "This is a test user".to_string(),
        }),
        common::oidc::REGULAREXPIRED,
    );

    let resp = userservice.register_user(req).await;

    assert!(resp.is_err());

    // Invalid token test
    let req = common::grpc_helpers::add_token(
        tonic::Request::new(RegisterUserRequest {
            display_name: "This is a test user".to_string(),
        }),
        common::oidc::REGULARINVALID,
    );

    let resp = userservice.register_user(req).await;

    assert!(resp.is_err());

    // Real Test
    let req = common::grpc_helpers::add_token(
        tonic::Request::new(RegisterUserRequest {
            display_name: "This is a test user".to_string(),
        }),
        common::oidc::REGULAROIDC,
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
    let req = common::grpc_helpers::add_token(
        tonic::Request::new(ActivateUserRequest {
            user_id: "ee4e1d0b-abab-4979-a33e-dc28ed199b17".to_string(),
        }),
        common::oidc::REGULAROIDC,
    );

    let resp = userservice.activate_user(req).await;

    // Should fail -> wrong token
    assert!(resp.is_err());

    // FAILED Test -> ADMIN OIDC TOKEN
    let req = common::grpc_helpers::add_token(
        tonic::Request::new(ActivateUserRequest {
            user_id: "ee4e1d0b-abab-4979-a33e-dc28ed199b17".to_string(),
        }),
        common::oidc::ADMINOIDC,
    );

    let resp = userservice.activate_user(req).await;

    // Should fail -> wrong token
    assert!(resp.is_err());

    // Real Test 1. Register USER2
    let req = common::grpc_helpers::add_token(
        tonic::Request::new(RegisterUserRequest {
            display_name: "This is a test user2".to_string(),
        }),
        common::oidc::USER2OIDC,
    );

    let resp = userservice.register_user(req).await;

    println!("{:#?}", resp);
    assert!(resp.is_ok());
    let resp = resp.unwrap().into_inner();
    assert!(!resp.user_id.is_empty());

    // Should succeed
    let req = common::grpc_helpers::add_token(
        tonic::Request::new(ActivateUserRequest {
            user_id: resp.user_id.to_string(),
        }),
        common::oidc::ADMINTOKEN,
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
    let req = common::grpc_helpers::add_token(
        tonic::Request::new(CreateApiTokenRequest {
            project_id: "".to_string(),
            collection_id: "".to_string(),
            name: "test_token".to_string(),
            expires_at: None,
            permission: 0,
        }),
        common::oidc::ADMINTOKEN,
    );

    let resp = userservice.create_api_token(req).await;

    assert!(resp.is_ok());
    let resp = resp.unwrap().into_inner();
    assert!(resp.token_secret != *"");
    let tok = resp.token.unwrap();
    assert_eq!(tok.name.to_string(), "test_token".to_string());
    assert_eq!(tok.collection_id, "".to_string());

    // Broken test with collection and project
    let req = common::grpc_helpers::add_token(
        tonic::Request::new(CreateApiTokenRequest {
            project_id: "bd62af97-6bf9-40b4-929a-686b417b8be7".to_string(),
            collection_id: "06f2c757-4c69-43f8-af82-7bd3e321ad9e".to_string(),
            name: "test_token_broken".to_string(),
            expires_at: None,
            permission: 0,
        }),
        common::oidc::ADMINTOKEN,
    );

    let resp = userservice.create_api_token(req).await;

    assert!(resp.is_err());
    assert_eq!(resp.err().unwrap().code(), Code::InvalidArgument);

    // Should work: Token for foreign collection (as_admin)
    let req = common::grpc_helpers::add_token(
        tonic::Request::new(CreateApiTokenRequest {
            project_id: "12345678-1111-1111-1111-111111111122".to_string(),
            collection_id: "".to_string(),
            name: "test_token_foreign".to_string(),
            expires_at: None,
            permission: 4,
        }),
        common::oidc::ADMINTOKEN,
    );

    let resp = userservice.create_api_token(req).await;

    println!("{:#?}", resp);
    assert!(resp.is_ok());
    let resp = resp.unwrap().into_inner();
    assert!(resp.token_secret != *"");
    let tok = resp.token.unwrap();
    assert_eq!(tok.name.to_string(), "test_token_foreign".to_string());
    assert_eq!(tok.collection_id, "".to_string());

    // Should fail for "regular user": Token for foreign collection (as_admin)
    let req = common::grpc_helpers::add_token(
        tonic::Request::new(CreateApiTokenRequest {
            project_id: "12345678-1111-1111-1111-111111111122".to_string(),
            collection_id: "".to_string(),
            name: "test_token_foreign".to_string(),
            expires_at: None,
            permission: 4,
        }),
        common::oidc::REGULARTOKEN,
    );

    let resp = userservice.create_api_token(req).await;

    println!("{:#?}", resp);
    assert!(resp.is_err());
    assert_eq!(resp.err().unwrap().code(), Code::PermissionDenied);

    // Should fail for "regular user": OIDCToken for foreign collection
    let req = common::grpc_helpers::add_token(
        tonic::Request::new(CreateApiTokenRequest {
            project_id: "12345678-1111-1111-1111-111111111122".to_string(),
            collection_id: "".to_string(),
            name: "test_token_foreign_fail".to_string(),
            expires_at: None,
            permission: 4,
        }),
        common::oidc::REGULAROIDC,
    );

    let resp = userservice.create_api_token(req).await;

    println!("{:#?}", resp);
    assert!(resp.is_err());
    assert_eq!(resp.err().unwrap().code(), Code::InvalidArgument);

    // Should work for "regular user": OIDCToken for personal token
    let req = common::grpc_helpers::add_token(
        tonic::Request::new(CreateApiTokenRequest {
            project_id: "".to_string(),
            collection_id: "".to_string(),
            name: "test_personal_oidc".to_string(),
            expires_at: None,
            permission: 4,
        }),
        common::oidc::REGULAROIDC,
    );

    let resp = userservice.create_api_token(req).await;

    println!("{:#?}", resp);
    assert!(resp.is_ok());
    let resp = resp.unwrap().into_inner();
    assert!(resp.token_secret != *"");
    let tok = resp.token.unwrap();
    assert_eq!(tok.name, "test_personal_oidc".to_string());
    assert_eq!(tok.collection_id, "".to_string());
    assert_eq!(tok.project_id, "".to_string());
}

#[ignore]
#[tokio::test]
#[serial(db)]
async fn get_api_token_grpc_test() {
    // Init services
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone()).await);
    let userservice = UserServiceImpl::new(db, authz).await;

    // Get User
    let get_req = common::grpc_helpers::add_token(
        tonic::Request::new(GetUserRequest {
            user_id: "".to_string(),
        }),
        common::oidc::REGULAROIDC,
    );
    let _resp = userservice.get_user(get_req).await.unwrap();

    // First Create a token
    let req = common::grpc_helpers::add_token(
        tonic::Request::new(CreateApiTokenRequest {
            project_id: "".to_string(),
            collection_id: "".to_string(),
            name: "test_personal_oidc".to_string(),
            expires_at: None,
            permission: 4,
        }),
        common::oidc::REGULAROIDC,
    );

    let resp = userservice.create_api_token(req).await;

    let tok = resp.unwrap().into_inner().token.unwrap();

    let req = common::grpc_helpers::add_token(
        tonic::Request::new(GetApiTokenRequest {
            token_id: tok.id.to_string(),
        }),
        common::oidc::REGULAROIDC,
    );

    let resp = userservice.get_api_token(req).await;

    let resp = resp.unwrap().into_inner();

    let get_tok = resp.token.unwrap();

    // Both tokens should be equal
    assert_eq!(get_tok, tok);
}

#[ignore]
#[tokio::test]
#[serial(db)]
async fn get_api_tokens_grpc_test() {
    // Init services
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone()).await);
    let userservice = UserServiceImpl::new(db, authz).await;

    // First Create a token
    let req = common::grpc_helpers::add_token(
        tonic::Request::new(CreateApiTokenRequest {
            project_id: "".to_string(),
            collection_id: "".to_string(),
            name: "test_personal_oidc".to_string(),
            expires_at: None,
            permission: 4,
        }),
        common::oidc::REGULAROIDC,
    );

    let resp = userservice.create_api_token(req).await;

    let tok = resp.unwrap().into_inner().token.unwrap();

    let req = common::grpc_helpers::add_token(
        tonic::Request::new(GetApiTokenRequest {
            token_id: tok.id.to_string(),
        }),
        common::oidc::REGULAROIDC,
    );

    let resp = userservice.get_api_token(req).await;

    let resp = resp.unwrap().into_inner();

    let get_tok = resp.token.unwrap();

    // Both tokens should be equal
    assert_eq!(get_tok, tok);
}
