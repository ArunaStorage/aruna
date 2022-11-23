use super::authz::Authz;
use crate::config::LocationVersion;
use crate::database::connection::Database;
use aruna_rust_api::api::storage::services::v1::{
    resource_info_service_server::ResourceInfoService,
    storage_info_service_server::StorageInfoService, ComponentStatus,
    ComponentVersion as GRPCCVersion, GetResourceHierarchyRequest, GetResourceHierarchyResponse,
    GetStorageStatusRequest, GetStorageStatusResponse, GetStorageVersionRequest,
    GetStorageVersionResponse, LocationStatus, LocationVersion as GRPCLocationVersion,
    SemanticVersion, Status,
};
use std::sync::Arc;
use tonic::Response;

// This macro automatically creates the Impl struct with all associated fields
crate::impl_grpc_server!(ResourceInfoServiceImpl);

#[tonic::async_trait]
impl ResourceInfoService for ResourceInfoServiceImpl {
    async fn get_resource_hierarchy(
        &self,
        _request: tonic::Request<GetResourceHierarchyRequest>,
    ) -> Result<tonic::Response<GetResourceHierarchyResponse>, tonic::Status> {
        // Validate token ?
        // This indicates a "valid token"

        return Err(tonic::Status::unimplemented("In development"));

        // let token_id = self
        //     .authz
        //     .validate_and_query_token(request.metadata())
        //     .await?;

        // todo!()
    }
}

// This macro automatically creates the Impl struct with all associated fields
crate::impl_grpc_server!(StorageInfoServiceImpl, config: LocationVersion);

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
        let resp = GetStorageVersionResponse {
            component_version: self
                .config
                .components
                .iter()
                .map(|component| GRPCCVersion {
                    component_name: component.component_name.to_string(),
                    location_version: vec![GRPCLocationVersion {
                        location: self.config.location.to_string(),
                        version: Some(SemanticVersion {
                            major: component.semver.major,
                            minor: component.semver.minor,
                            patch: component.semver.patch,
                            labels: component.semver.labels.to_string(),
                            version_string: format!(
                                "{}.{}.{}-{}",
                                component.semver.major,
                                component.semver.minor,
                                component.semver.patch,
                                component.semver.labels.to_string()
                            ),
                        }),
                    }],
                })
                .collect(),
        };

        Ok(Response::new(resp))
    }
    /// GetStorageStatus
    ///
    /// A request to get the current status of the storage components by location(s)
    async fn get_storage_status(
        &self,
        _request: tonic::Request<GetStorageStatusRequest>,
    ) -> Result<tonic::Response<GetStorageStatusResponse>, tonic::Status> {
        Ok(Response::new(GetStorageStatusResponse {
            component_status: vec![ComponentStatus {
                component_name: "backend".to_string(),
                location_status: vec![LocationStatus {
                    location: self.config.location.to_string(),
                    status: Status::Available as i32,
                }],
            }],
        }))
    }
}
