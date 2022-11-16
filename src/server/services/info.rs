use super::authz::Authz;
use crate::database::connection::Database;
use aruna_rust_api::api::storage::services::v1::{
    resource_info_service_server::ResourceInfoService,
    storage_info_service_server::StorageInfoService, GetResourceHierarchyRequest,
    GetResourceHierarchyResponse, GetStorageStatusRequest, GetStorageStatusResponse,
    GetStorageVersionRequest, GetStorageVersionResponse,
};
use std::sync::Arc;

// This macro automatically creates the Impl struct with all associated fields
crate::impl_grpc_server!(ResourceInfoServiceImpl);

#[tonic::async_trait]
impl ResourceInfoService for ResourceInfoServiceImpl {
    async fn get_resource_hierarchy(
        &self,
        _request: tonic::Request<GetResourceHierarchyRequest>,
    ) -> Result<tonic::Response<GetResourceHierarchyResponse>, tonic::Status> {
        todo!()
    }
}

// This macro automatically creates the Impl struct with all associated fields
crate::impl_grpc_server!(StorageInfoServiceImpl);

#[tonic::async_trait]
impl StorageInfoService for StorageInfoServiceImpl {
    /// GetStorageVersion
    ///
    /// A request to get the current version of the server application
    /// String representation and https://semver.org/
    async fn get_storage_version(
        &self,
        _request: tonic::Request<GetStorageVersionRequest>,
    ) -> Result<tonic::Response<GetStorageVersionResponse>, tonic::Status> {
        todo!()
    }
    /// GetStorageStatus
    ///
    /// A request to get the current status of the storage components by location(s)
    async fn get_storage_status(
        &self,
        _request: tonic::Request<GetStorageStatusRequest>,
    ) -> Result<tonic::Response<GetStorageStatusResponse>, tonic::Status> {
        todo!()
    }
}
