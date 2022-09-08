//! This file contains the gRPC implementation for the ObjectGroupService
use tokio::task;
use tonic::Response;

use super::authz::Authz;
use crate::api::aruna::api::storage::services::v1::object_group_service_server::ObjectGroupService;
use crate::api::aruna::api::storage::services::v1::*;
use crate::database::connection::Database;
use crate::database::models::enums::UserRights;
use crate::error::ArunaError;
use std::sync::Arc;

// This automatically creates the ObjectGroupServiceImpl struct and ::new methods
crate::impl_grpc_server!(ObjectGroupServiceImpl);

#[tonic::async_trait]
impl ObjectGroupService for ObjectGroupServiceImpl {
    /// CreateObjectGroup creates a new ObjectGroup in the collection
    async fn create_object_group(
        &self,
        request: tonic::Request<CreateObjectGroupRequest>
    ) -> Result<tonic::Response<CreateObjectGroupResponse>, tonic::Status> {
        // Check if user is authorized to create objects in this collection
        let collection_id = uuid::Uuid
            ::parse_str(&request.get_ref().collection_id)
            .map_err(ArunaError::from)?;

        let creator_id = self.authz.collection_authorize(
            request.metadata(),
            collection_id, // This is the collection uuid in which this object should be created
            UserRights::APPEND // User needs at least append permission to create an object
        ).await?;

        // Create Object in database
        let database_clone = self.database.clone();
        let response = task
            ::spawn_blocking(move ||
                database_clone.create_object_group(request.get_ref(), &creator_id)
            ).await
            .map_err(ArunaError::from)??;

        // Return gRPC response after everything succeeded
        return Ok(Response::new(response));
    }
    /// UpdateObjectGroup creates an updated ObjectGroup
    /// ObjectGroups are immutable
    /// Updating an ObjectGroup will create a new Revision of the ObjectGroup
    async fn update_object_group(
        &self,
        request: tonic::Request<UpdateObjectGroupRequest>
    ) -> Result<tonic::Response<UpdateObjectGroupResponse>, tonic::Status> {
        // Check if user is authorized to create objects in this collection
        let collection_id = uuid::Uuid
            ::parse_str(&request.get_ref().collection_id)
            .map_err(ArunaError::from)?;

        let creator_id = self.authz.collection_authorize(
            request.metadata(),
            collection_id, // This is the collection uuid in which this object should be created
            UserRights::APPEND // User needs at least append permission to create an object
        ).await?;

        // Create Object in database
        let database_clone = self.database.clone();
        let response = task
            ::spawn_blocking(move ||
                database_clone.update_object_group(request.get_ref(), &creator_id)
            ).await
            .map_err(ArunaError::from)??;

        // Return gRPC response after everything succeeded
        return Ok(Response::new(response));
    }
    /// GetObjectGroupById gets a specific ObjectGroup by ID
    /// By default the latest revision is always returned, older revisions need to
    /// be specified separately
    async fn get_object_group_by_id(
        &self,
        request: tonic::Request<GetObjectGroupByIdRequest>
    ) -> Result<tonic::Response<GetObjectGroupByIdResponse>, tonic::Status> {
        // Check if user is authorized to create objects in this collection
        let collection_id = uuid::Uuid
            ::parse_str(&request.get_ref().collection_id)
            .map_err(ArunaError::from)?;

        self.authz.collection_authorize(
            request.metadata(),
            collection_id, // This is the collection uuid in which this object should be created
            UserRights::READ // User needs at least append permission to create an object
        ).await?;

        // Create Object in database
        let database_clone = self.database.clone();
        let response = task
            ::spawn_blocking(move || database_clone.get_object_group_by_id(request.get_ref())).await
            .map_err(ArunaError::from)??;

        // Return gRPC response after everything succeeded
        return Ok(Response::new(response));
    }
    /// GetObjectGroupsFromObject gets all ObjectGroups associated to a specific
    /// Object Objects can be part of multiple ObjectGroups at once
    async fn get_object_groups_from_object(
        &self,
        _request: tonic::Request<GetObjectGroupsFromObjectRequest>
    ) -> Result<tonic::Response<GetObjectGroupsFromObjectResponse>, tonic::Status> {
        todo!()
    }
    /// GetObjectGroups is a request that returns a (paginated) list of
    /// ObjectGroups that contain a specific set of labels.
    async fn get_object_groups(
        &self,
        _request: tonic::Request<GetObjectGroupsRequest>
    ) -> Result<tonic::Response<GetObjectGroupsResponse>, tonic::Status> {
        todo!()
    }
    async fn get_object_group_history(
        &self,
        _request: tonic::Request<GetObjectGroupHistoryRequest>
    ) -> Result<tonic::Response<GetObjectGroupHistoryResponse>, tonic::Status> {
        todo!()
    }
    async fn get_object_group_objects(
        &self,
        _request: tonic::Request<GetObjectGroupObjectsRequest>
    ) -> Result<tonic::Response<GetObjectGroupObjectsResponse>, tonic::Status> {
        todo!()
    }
    /// DeleteObjectGroup is a request that deletes a specified ObjectGroup
    /// This does not delete the associated Objects
    async fn delete_object_group(
        &self,
        _request: tonic::Request<DeleteObjectGroupRequest>
    ) -> Result<tonic::Response<DeleteObjectGroupResponse>, tonic::Status> {
        todo!()
    }
}