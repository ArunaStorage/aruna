use std::sync::Arc;

use aruna_rust_api::api::storage::{
    models::v1::Token,
    services::v1::{user_service_server::UserService, CreateApiTokenRequest, ExpiresAt},
};
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
    let authz = Arc::new(Authz::new(db.clone()).await);
    let userservice = UserServiceImpl::new(db, authz).await;

    let tokname = if create_token.name.is_empty() {
        format!("token_{}", super::functions::rand_string(5))
    } else {
        create_token.name.to_string()
    };

    let expires = if create_token.expires_at == 0 {
        Utc::now().timestamp() + 111111111
    } else {
        create_token.expires_at.clone()
    };

    let token = if create_token.basetoken == "" {
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
            permission: create_token.permission.clone(),
        }),
        &token,
    );

    let resp = userservice.create_api_token(req).await;

    // Check the token
    let tok = resp.unwrap().into_inner().token.unwrap();
    assert_eq!(tok.name.to_string(), tokname);
    tok
}
