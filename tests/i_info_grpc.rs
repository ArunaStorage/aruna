use std::sync::Arc;

use aruna_rust_api::api::storage::services::v1::{
    storage_info_service_server::StorageInfoService, GetStorageStatusRequest,
    GetStorageVersionRequest,
};
use aruna_server::{
    config::ArunaServerConfig,
    database::{self},
    server::services::{authz::Authz, info::StorageInfoServiceImpl},
};
use serial_test::serial;
mod common;

#[ignore]
#[tokio::test]
#[serial(db)]
async fn storage_info_status_test() {
    // Init services
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));

    let authz = Arc::new(Authz::new(db.clone()).await);
    let config = ArunaServerConfig::new();
    let infoservice = StorageInfoServiceImpl::new(db, authz, config.config.loc_version).await;

    let req = tonic::Request::new(GetStorageStatusRequest {});
    let resp = infoservice
        .get_storage_status(req)
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        resp.component_status.first().unwrap().component_name,
        "backend".to_string()
    )
}

#[ignore]
#[tokio::test]
#[serial(db)]
async fn storage_info_version_test() {
    // Init services
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));

    let authz = Arc::new(Authz::new(db.clone()).await);
    let config = ArunaServerConfig::new();
    let infoservice = StorageInfoServiceImpl::new(db, authz, config.config.loc_version).await;

    let req = tonic::Request::new(GetStorageVersionRequest {});
    let resp = infoservice
        .get_storage_version(req)
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        resp.clone()
            .component_version
            .first()
            .unwrap()
            .location_version
            .first()
            .unwrap()
            .location,
        "FOO".to_string()
    );

    assert_eq!(
        resp.component_version
            .first()
            .unwrap()
            .location_version
            .first()
            .unwrap()
            .version
            .as_ref()
            .unwrap()
            .minor,
        1
    )
}
