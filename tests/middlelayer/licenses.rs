use aruna_rust_api::api::storage::services::v2::CreateLicenseRequest;
use aruna_server::database::dsls::license_dsl::License;
use itertools::Itertools;

use crate::common::init::{
    init_database_handler_middlelayer, init_permission_handler, init_token_handler,
};
use crate::common::test_utils;

#[tokio::test]
async fn create_and_read_license() {
    // Init
    let db_handler = init_database_handler_middlelayer().await;

    // Create & Get License
    let request = CreateLicenseRequest {
        tag: "middlelayer_license_test".to_string(),
        name: "middlelayer license test".to_string(),
        text: "Tests create license function in middlelayer".to_string(),
        url: "test.org/middelayer-test-license".to_string(),
    };
    let tag = db_handler.create_license(request.clone()).await.unwrap();
    assert_eq!(tag, request.tag);
    let license = db_handler.get_license(tag).await.unwrap();
    assert_eq!(license.tag, request.tag);
    assert_eq!(license.name, request.name);
    assert_eq!(license.description, request.text);
    assert_eq!(license.url, request.url);

    // List Licenses
    let dummy_one_req = CreateLicenseRequest {
        tag: "dummy_one".to_string(),
        name: "middlelayer license test".to_string(),
        text: "Tests create license function in middlelayer".to_string(),
        url: "test.org/middelayer-test-license".to_string(),
    };
    let dummy_two_req = CreateLicenseRequest {
        tag: "dummy_two".to_string(),
        name: "middlelayer license test".to_string(),
        text: "Tests create license function in middlelayer".to_string(),
        url: "test.org/middelayer-test-license".to_string(),
    };
    db_handler
        .create_license(dummy_one_req.clone())
        .await
        .unwrap();
    db_handler
        .create_license(dummy_two_req.clone())
        .await
        .unwrap();
    let all = db_handler.list_licenses().await.unwrap();
    let dummy_one: License = dummy_one_req.into();
    let dummy_two: License = dummy_two_req.into();
    assert!(all.iter().contains(&dummy_one));
    assert!(all.iter().contains(&dummy_two));
}

#[tokio::test]
async fn hierarchies_and_licenses() {
    // Init
    let db_handler = init_database_handler_middlelayer().await;
    let request = CreateLicenseRequest {
        tag: "default_middlelayer_license_test".to_string(),
        name: "default middlelayer license test".to_string(),
        text: "Tests default licenses".to_string(),
        url: "test.org/default-middelayer-test-license".to_string(),
    };
    let tag_one = db_handler.create_license(request).await.unwrap();
    let request = CreateLicenseRequest {
        tag: "second_default_middlelayer_license_test".to_string(),
        name: "default middlelayer license test".to_string(),
        text: "Tests default licenses".to_string(),
        url: "test.org/default-middelayer-test-license".to_string(),
    };
    let tag_two = db_handler.create_license(request).await.unwrap();

    // Create Project, Collection, Dataset and Object
    let request = CreateRequest::Project(inner_request, self.default_endpoint.clone());

    let (project, user) = database_handler
        .create_resource(self.authorizer.clone(), request, user_id, is_dataproxy)
        .await
        .unwrap();
}
