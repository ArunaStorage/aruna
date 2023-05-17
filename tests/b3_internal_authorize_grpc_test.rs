use std::sync::Arc;

use crate::common::functions::TCreateCollection;
use aruna_rust_api::api::{
    internal::v1::{
        internal_authorize_service_server::InternalAuthorizeService, AuthorizeRequest, IdType,
        Identifier,
    },
    storage::models::v1::{Permission, ResourceAction, ResourceType},
};
use aruna_server::{
    config::ArunaServerConfig,
    database::{self},
    server::services::{authz::Authz, internal_authorize::InternalAuthorizeServiceImpl},
};
use diesel_ulid::DieselUlid;
use serial_test::serial;

mod common;

#[ignore]
#[tokio::test]
#[serial(db)]
async fn internal_authorize_grpc_test() {
    // Init project service
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone(), ArunaServerConfig::default()).await);
    let internal_authz_service = InternalAuthorizeServiceImpl::new(db.clone(), authz.clone()).await;

    // Fast track project creation
    let random_project = common::functions::create_project(None);

    // Fast track user adding with None permission
    let user_id = common::grpc_helpers::get_token_user_id(common::oidc::REGULARTOKEN).await;
    let add_perm = common::grpc_helpers::add_project_permission(
        random_project.id.as_str(),
        user_id.as_str(),
        common::oidc::ADMINTOKEN,
    )
    .await;
    assert_eq!(add_perm.permission, Permission::None as i32);

    // Fast track collection creation
    let random_collection = common::functions::create_collection(TCreateCollection {
        project_id: random_project.id.to_string(),
        creator_id: Some(user_id.clone()),
        ..Default::default()
    });

    // Try to authorize against non-existent project
    let auth_request = common::grpc_helpers::add_token(
        tonic::Request::new(AuthorizeRequest {
            authorization: None,
            resource: ResourceType::Project as i32,
            identifier: Some(Identifier {
                idtype: IdType::Uuid as i32,
                name: DieselUlid::generate().to_string(),
            }),
            resource_action: ResourceAction::Create as i32,
        }),
        common::oidc::REGULARTOKEN,
    );

    let auth_response = internal_authz_service
        .authorize(auth_request)
        .await
        .unwrap()
        .into_inner();

    assert!(!auth_response.ok); // Should not be ok

    // Try to authorize against project with not enough permissions
    let auth_request = common::grpc_helpers::add_token(
        tonic::Request::new(AuthorizeRequest {
            authorization: None,
            resource: ResourceType::Project as i32,
            identifier: Some(Identifier {
                idtype: IdType::Uuid as i32,
                name: random_project.id.to_string(),
            }),
            resource_action: ResourceAction::Read as i32,
        }),
        common::oidc::REGULARTOKEN,
    );

    let auth_response = internal_authz_service
        .authorize(auth_request)
        .await
        .unwrap()
        .into_inner();

    assert!(!auth_response.ok); // Should not be ok

    // Try to authorize against non-existent collection
    let auth_request = common::grpc_helpers::add_token(
        tonic::Request::new(AuthorizeRequest {
            authorization: None,
            resource: ResourceType::Collection as i32,
            identifier: Some(Identifier {
                idtype: IdType::Uuid as i32,
                name: DieselUlid::generate().to_string(),
            }),
            resource_action: ResourceAction::Read as i32,
        }),
        common::oidc::REGULARTOKEN,
    );

    let auth_response = internal_authz_service
        .authorize(auth_request)
        .await
        .unwrap()
        .into_inner();

    assert!(!auth_response.ok); // Should not be ok

    // Try to authorize against collection with not enough permissions
    let auth_request = common::grpc_helpers::add_token(
        tonic::Request::new(AuthorizeRequest {
            authorization: None,
            resource: ResourceType::Collection as i32,
            identifier: Some(Identifier {
                idtype: IdType::Uuid as i32,
                name: random_collection.id.to_string(),
            }),
            resource_action: ResourceAction::Read as i32,
        }),
        common::oidc::REGULARTOKEN,
    );

    let auth_response = internal_authz_service
        .authorize(auth_request)
        .await
        .unwrap()
        .into_inner();

    assert!(!auth_response.ok); // Should not be ok

    // Try to authorize project creation with admin token
    let auth_request = common::grpc_helpers::add_token(
        tonic::Request::new(AuthorizeRequest {
            authorization: None,
            resource: ResourceType::Project as i32,
            identifier: Some(Identifier {
                idtype: IdType::Uuid as i32,
                name: "".to_string(),
            }),
            resource_action: ResourceAction::Create as i32,
        }),
        common::oidc::ADMINTOKEN,
    );

    let auth_response = internal_authz_service
        .authorize(auth_request)
        .await
        .unwrap()
        .into_inner();

    assert!(auth_response.ok); // Should be ok

    //ToDo: Set permissions for project and test authorization(s)
    let edit_perm = common::grpc_helpers::edit_project_permission(
        random_project.id.as_str(),
        user_id.as_str(),
        &Permission::Read,
        common::oidc::ADMINTOKEN,
    )
    .await;
    assert_eq!(edit_perm.permission, Permission::Read as i32);

    let auth_request = common::grpc_helpers::add_token(
        tonic::Request::new(AuthorizeRequest {
            authorization: None,
            resource: ResourceType::Project as i32,
            identifier: Some(Identifier {
                idtype: IdType::Uuid as i32,
                name: random_project.id.to_string(),
            }),
            resource_action: ResourceAction::Read as i32,
        }),
        common::oidc::REGULARTOKEN,
    );

    let auth_response = internal_authz_service
        .authorize(auth_request)
        .await
        .unwrap()
        .into_inner();

    assert!(auth_response.ok); // Should be ok

    //ToDo: Set permissions for collection and test authorizations

    //ToDo: Try object authorizations (Not yet implemented)
    let auth_request = common::grpc_helpers::add_token(
        tonic::Request::new(AuthorizeRequest {
            authorization: None,
            resource: ResourceType::Object as i32,
            identifier: Some(Identifier {
                idtype: IdType::Uuid as i32,
                name: DieselUlid::generate().to_string(),
            }),
            resource_action: ResourceAction::Read as i32,
        }),
        common::oidc::REGULARTOKEN,
    );

    let auth_response = internal_authz_service.authorize(auth_request).await;

    assert!(auth_response.is_err()); // Not yet implemented

    //ToDo: Try authorize with path identifier (Object does not exist)
    let auth_request = common::grpc_helpers::add_token(
        tonic::Request::new(AuthorizeRequest {
            authorization: None,
            resource: ResourceType::Object as i32,
            identifier: Some(Identifier {
                idtype: IdType::Path as i32,
                name: format!(
                    "s3://latest.{}.{}/example.file",
                    random_collection.name, random_project.name
                ),
                //name: format!("s3://latest.{}.{}.lorem.aruna-storage.org/example.file", random_project.name, random_collection.name),
            }),
            resource_action: ResourceAction::Delete as i32,
        }),
        common::oidc::REGULARTOKEN,
    );

    let auth_response = internal_authz_service.authorize(auth_request).await;

    assert!(auth_response.is_err()); // Path authorization not yet implemented
}

#[ignore]
#[tokio::test]
#[serial(db)]
async fn get_secret_grpc_test() {
    // Init project service
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));
    let authz = Arc::new(Authz::new(db.clone(), ArunaServerConfig::default()).await);
    let _internal_authz_service =
        InternalAuthorizeServiceImpl::new(db.clone(), authz.clone()).await;

    //ToDo: Create API token for some user

    //ToDo: Call get_secret to re-fetch token specific S3 access secret
}
