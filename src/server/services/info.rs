use super::authz::{Authz, CtxTarget};
use crate::database::connection::Database;
use crate::{config::LocationVersion, error::ArunaError};
use aruna_policy::ape::structs::ResourceTarget;
use aruna_rust_api::api::storage::models::v1::ResourceType;
use aruna_rust_api::api::storage::services::v1::{
    resource_info_service_server::ResourceInfoService,
    storage_info_service_server::StorageInfoService, ComponentStatus,
    ComponentVersion as GRPCCVersion, GetResourceHierarchyRequest, GetResourceHierarchyResponse,
    GetStorageStatusRequest, GetStorageStatusResponse, GetStorageVersionRequest,
    GetStorageVersionResponse, LocationStatus, LocationVersion as GRPCLocationVersion,
    SemanticVersion, Status,
};
use std::str::FromStr;
use std::sync::Arc;
use tokio::task;
use tonic::Response;

// This macro automatically creates the Impl struct with all associated fields
crate::impl_grpc_server!(ResourceInfoServiceImpl);

#[tonic::async_trait]
impl ResourceInfoService for ResourceInfoServiceImpl {
    async fn get_resource_hierarchy(
        &self,
        request: tonic::Request<GetResourceHierarchyRequest>,
    ) -> Result<tonic::Response<GetResourceHierarchyResponse>, tonic::Status> {
        return Err(tonic::Status::unimplemented(
            "Implementation not yet finished.",
        ));

        // Authorize project - WRITE
        let (user_id, constraints) = self
            .authz
            .authorize(
                request.metadata(),
                CtxTarget {
                    action: ResourceType::Read,
                    target: ResourceTarget::Object(diesel_ulid::DieselUlid::generate()),
                },
            )
            .await?;

        //ToDo: How to correctly authorize against resource_id/resource_type without specific action?
        //self.authz.resource_read_authorize(metadata, resource_ulid, resource_type)?;

        // Extract other request fields
        let resource_ulid = diesel_ulid::DieselUlid::from_str(&request.get_ref().resource_id)
            .map_err(ArunaError::from)?;
        let resource_type = ResourceType::from_i32(&request.get_ref().resource_type)
            .ok_or_else(|| ArunaError::InvalidRequest("Invalid resource type".to_string()))?;

        // Fetch resource hierarchy
        let database_clone = self.database.clone();
        let response = task::spawn_blocking(move || {
            database_clone.validate_and_query_hierarchy(user_id, &resource_ulid, resource_type)
        })
        .await
        .map_err(ArunaError::from)??;

        // Return gRPC response
        Ok(tonic::Response::new(response))
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
                                component.semver.labels
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
