use crate::common::{
    init::init_service_block,
    test_utils::{add_token, ADMIN_OIDC_TOKEN, DEFAULT_ENDPOINT_ULID, USER1_OIDC_TOKEN},
};
use aruna_rust_api::api::storage::services::v2::{
    license_service_server::LicenseService, CreateLicenseRequest,
};

#[tokio::test]
async fn create_and_get_licenses() {
    // Init
    let services = init_service_block().await;

    let create_request = CreateLicenseRequest {
        tag: "grpc_license_test".to_string(),
        name: "grpc license test".to_string(),
        text: "Tests grpc licenses".to_string(),
        url: "test.org/grpc-test-license".to_string(),
    };
    let second_create_request = CreateLicenseRequest {
        tag: "second_grpc_license_test".to_string(),
        name: "grpc license test".to_string(),
        text: "Tests grpc licenses".to_string(),
        url: "test.org/grpc-test-license".to_string(),
    };
    let first_response = services
        .license_service
        .create_license(add_token(
            tonic::Request::new(create_request.clone()),
            USER1_OIDC_TOKEN,
        ))
        .await
        .unwrap();
    let second_response = services
        .license_service
        .create_license(add_token(
            tonic::Request::new(second_create_request.clone()),
            USER1_OIDC_TOKEN,
        ))
        .await
        .unwrap();

    assert_eq!(first_response.into_inner().tag, create_request.tag);
    assert_eq!(second_response.into_inner().tag, second_create_request.tag);
}
