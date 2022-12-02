use aruna_rust_api::api::storage::services::v1::project_service_server::ProjectService;
use aruna_rust_api::api::storage::services::v1::CreateProjectRequest;
use aruna_server::{
    database::{self},
    server::services::authz::Authz,
    server::services::project::ProjectServiceImpl,
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
    let create_project_response = project_service
        .create_project(create_project_request)
        .await;

    // Validate project creation failed with non-admin token
    assert!(create_project_response.is_err())
}
