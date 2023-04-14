//! This file contains the gRPC implementation for the ObjectGroupService
use std::str::FromStr;
use std::sync::Arc;
use tokio::task;
use tonic::Response;

use super::authz::Authz;
use crate::database::connection::Database;
use crate::database::models::enums::UserRights;
use crate::error::ArunaError;
use crate::server::services::utils::{format_grpc_request, format_grpc_response};
use aruna_rust_api::api::storage::services::v1::object_group_service_server::ObjectGroupService;
use aruna_rust_api::api::storage::services::v1::*;

// This automatically creates the ObjectGroupServiceImpl struct and ::new methods
crate::impl_grpc_server!(ObjectGroupServiceImpl);

#[tonic::async_trait]
impl ObjectGroupService for ObjectGroupServiceImpl {
    /// CreateObjectGroup creates a new ObjectGroup in the collection
    async fn create_object_group(
        &self,
        request: tonic::Request<CreateObjectGroupRequest>,
    ) -> Result<tonic::Response<CreateObjectGroupResponse>, tonic::Status> {
        log::info!("Received CreateObjectGroupRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Check if user is authorized to create objects in this collection
        let collection_id = diesel_ulid::DieselUlid::from_str(&request.get_ref().collection_id)
            .map_err(ArunaError::from)?;

        let creator_id = self
            .authz
            .collection_authorize(
                request.metadata(),
                collection_id, // This is the collection uuid in which this object should be created
                UserRights::APPEND, // User needs at least append permission to create an object
            )
            .await?;

        // Create Object in database
        let database_clone = self.database.clone();
        let response = Response::new(
            task::spawn_blocking(move || {
                database_clone.create_object_group(request.get_ref(), &creator_id)
            })
            .await
            .map_err(ArunaError::from)??,
        );

        // Return gRPC response after everything succeeded
        log::info!("Sending CreateObjectGroupResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        return Ok(response);
    }

    /// UpdateObjectGroup creates an updated ObjectGroup
    /// ObjectGroups are immutable
    /// Updating an ObjectGroup will create a new Revision of the ObjectGroup
    async fn update_object_group(
        &self,
        request: tonic::Request<UpdateObjectGroupRequest>,
    ) -> Result<tonic::Response<UpdateObjectGroupResponse>, tonic::Status> {
        log::info!("Received UpdateObjectGroupRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Check if user is authorized to create objects in this collection
        let collection_id = diesel_ulid::DieselUlid::from_str(&request.get_ref().collection_id)
            .map_err(ArunaError::from)?;

        let creator_id = self
            .authz
            .collection_authorize(
                request.metadata(),
                collection_id, // This is the collection uuid in which this object should be created
                UserRights::APPEND, // User needs at least append permission to create an object
            )
            .await?;

        // Create Object in database
        let database_clone = self.database.clone();
        let response = Response::new(
            task::spawn_blocking(move || {
                database_clone.update_object_group(request.get_ref(), &creator_id)
            })
            .await
            .map_err(ArunaError::from)??,
        );

        // Return gRPC response after everything succeeded
        log::info!("Sending UpdateObjectGroupResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        return Ok(response);
    }

    /// GetObjectGroupById gets a specific ObjectGroup by ID
    /// By default the latest revision is always returned, older revisions need to
    /// be specified separately
    async fn get_object_group_by_id(
        &self,
        request: tonic::Request<GetObjectGroupByIdRequest>,
    ) -> Result<tonic::Response<GetObjectGroupByIdResponse>, tonic::Status> {
        log::info!("Received GetObjectGroupByIdRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Check if user is authorized to create objects in this collection
        let collection_id = diesel_ulid::DieselUlid::from_str(&request.get_ref().collection_id)
            .map_err(ArunaError::from)?;

        self.authz
            .collection_authorize(
                request.metadata(),
                collection_id, // This is the collection uuid in which this object should be created
                UserRights::READ, // User needs at least append permission to create an object
            )
            .await?;

        // Create Object in database
        let database_clone = self.database.clone();
        let response = Response::new(
            task::spawn_blocking(move || database_clone.get_object_group_by_id(request.get_ref()))
                .await
                .map_err(ArunaError::from)??,
        );

        // Return gRPC response after everything succeeded
        log::info!("Sending GetObjectGroupByIdResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        return Ok(response);
    }

    /// GetObjectGroupsFromObject gets all ObjectGroups associated to a specific
    /// Object Objects can be part of multiple ObjectGroups at once
    async fn get_object_groups_from_object(
        &self,
        request: tonic::Request<GetObjectGroupsFromObjectRequest>,
    ) -> Result<tonic::Response<GetObjectGroupsFromObjectResponse>, tonic::Status> {
        log::info!("Received GetObjectGroupsFromObjectRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Check if user is authorized to create objects in this collection
        let collection_id = diesel_ulid::DieselUlid::from_str(&request.get_ref().collection_id)
            .map_err(ArunaError::from)?;

        self.authz
            .collection_authorize(
                request.metadata(),
                collection_id, // This is the collection uuid in which this object should be created
                UserRights::READ, // User needs at least append permission to create an object
            )
            .await?;

        // Create Object in database
        let database_clone = self.database.clone();
        let response = Response::new(
            task::spawn_blocking(move || {
                database_clone.get_object_groups_from_object(request.get_ref())
            })
            .await
            .map_err(ArunaError::from)??,
        );

        // Return gRPC response after everything succeeded
        log::info!("Sending GetObjectGroupsFromObjectResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        return Ok(response);
    }

    /// GetObjectGroups is a request that returns a (paginated) list of
    /// ObjectGroups that contain a specific set of labels.
    async fn get_object_groups(
        &self,
        request: tonic::Request<GetObjectGroupsRequest>,
    ) -> Result<tonic::Response<GetObjectGroupsResponse>, tonic::Status> {
        log::info!("Received GetObjectGroupsRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Check if user is authorized to create objects in this collection
        let collection_id = diesel_ulid::DieselUlid::from_str(&request.get_ref().collection_id)
            .map_err(ArunaError::from)?;

        self.authz
            .collection_authorize(
                request.metadata(),
                collection_id, // This is the collection uuid in which this object should be created
                UserRights::READ, // User needs at least append permission to create an object
            )
            .await?;

        // Query object_group in database
        let database_clone = self.database.clone();
        let response = Response::new(
            task::spawn_blocking(move || database_clone.get_object_groups(request.into_inner()))
                .await
                .map_err(ArunaError::from)??,
        );

        // Return gRPC response after everything succeeded
        log::info!("Sending GetObjectGroupsResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        return Ok(response);
    }

    async fn get_object_group_history(
        &self,
        request: tonic::Request<GetObjectGroupHistoryRequest>,
    ) -> Result<tonic::Response<GetObjectGroupHistoryResponse>, tonic::Status> {
        log::info!("Received GetObjectGroupHistoryRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Check if user is authorized to create objects in this collection
        let collection_id = diesel_ulid::DieselUlid::from_str(&request.get_ref().collection_id)
            .map_err(ArunaError::from)?;

        self.authz
            .collection_authorize(
                request.metadata(),
                collection_id, // This is the collection uuid in which this object should be created
                UserRights::READ, // User needs at least append permission to create an object
            )
            .await?;

        // Query object_group in database
        let database_clone = self.database.clone();
        let response = Response::new(
            task::spawn_blocking(move || {
                database_clone.get_object_group_history(request.into_inner())
            })
            .await
            .map_err(ArunaError::from)??,
        );

        // Return gRPC response after everything succeeded
        log::info!("Sending GetObjectGroupHistoryResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        return Ok(response);
    }

    async fn get_object_group_objects(
        &self,
        request: tonic::Request<GetObjectGroupObjectsRequest>,
    ) -> Result<tonic::Response<GetObjectGroupObjectsResponse>, tonic::Status> {
        log::info!("Received GetObjectGroupObjectsRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Check if user is authorized to create objects in this collection
        let collection_id = diesel_ulid::DieselUlid::from_str(&request.get_ref().collection_id)
            .map_err(ArunaError::from)?;

        self.authz
            .collection_authorize(
                request.metadata(),
                collection_id, // This is the collection uuid in which this object should be created
                UserRights::READ, // User needs at least append permission to create an object
            )
            .await?;

        // Query object_group_objects in database
        let database_clone = self.database.clone();
        let response = Response::new(
            task::spawn_blocking(move || {
                database_clone.get_object_group_objects(request.into_inner())
            })
            .await
            .map_err(ArunaError::from)??,
        );

        // Return gRPC response after everything succeeded
        log::info!("Sending GetObjectGroupObjectsResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        return Ok(response);
    }

    /// DeleteObjectGroup is a request that deletes a specified ObjectGroup
    /// This does not delete the associated Objects
    async fn delete_object_group(
        &self,
        request: tonic::Request<DeleteObjectGroupRequest>,
    ) -> Result<tonic::Response<DeleteObjectGroupResponse>, tonic::Status> {
        log::info!("Received DeleteObjectGroupRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // Check if user is authorized to create objects in this collection
        let collection_id = diesel_ulid::DieselUlid::from_str(&request.get_ref().collection_id)
            .map_err(ArunaError::from)?;

        self.authz
            .collection_authorize(
                request.metadata(),
                collection_id, // This is the collection uuid in which this object should be created
                UserRights::WRITE, // User needs at least append permission to create an object
            )
            .await?;

        // Query object_group_objects in database
        let database_clone = self.database.clone();
        let response = Response::new(
            task::spawn_blocking(move || database_clone.delete_object_group(request.into_inner()))
                .await
                .map_err(ArunaError::from)??,
        );

        // Return gRPC response after everything succeeded
        log::info!("Sending DeleteObjectGroupResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        return Ok(response);
    }

    /// AddLabelsToObjectGroup
    ///
    /// This is a specific request to add new label(s)
    /// to an existing object_group, in contrast to UpdateObjectGroup
    /// this will not create a new revision for the specific object_group
    /// Instead it will directly add the specified label(s) to the object_group
    async fn add_labels_to_object_group(
        &self,
        request: tonic::Request<AddLabelsToObjectGroupRequest>,
    ) -> Result<tonic::Response<AddLabelsToObjectGroupResponse>, tonic::Status> {
        log::info!("Received AddLabelsToObjectGroupRequest.");
        log::debug!("{}", format_grpc_request(&request));

        let target_collection_uuid =
            diesel_ulid::DieselUlid::from_str(&request.get_ref().collection_id)
                .map_err(ArunaError::from)?;
        self.authz
            .collection_authorize(
                request.metadata(),
                target_collection_uuid, // This is the collection uuid in which this object should be created
                UserRights::WRITE,      // User needs at least append permission to create an object
            )
            .await?;

        // Create Object in database
        let database_clone = self.database.clone();
        let response = Response::new(
            task::spawn_blocking(move || {
                database_clone.add_labels_to_object_group(request.into_inner())
            })
            .await
            .map_err(ArunaError::from)??,
        );

        // Return gRPC response after everything succeeded
        log::info!("Sending AddLabelsToObjectGroupResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        return Ok(response);
    }
}
