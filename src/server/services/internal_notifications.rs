use super::authz::Authz;
use aruna_rust_api::api::{
    internal::v1::{
        internal_event_service_server::InternalEventService, CreateStreamGroupRequest,
        CreateStreamGroupResponse, DeleteStreamGroupRequest, DeleteStreamGroupResponse,
        GetSharedRevisionRequest, GetSharedRevisionResponse, GetStreamGroupRequest,
        GetStreamGroupResponse, StreamGroup,
    },
    notification::services::v1::EventType,
    storage::models::v1::ResourceType,
};

use crate::{
    database::{
        connection::Database,
        crud::utils::{db_to_grpc_resource, grpc_to_db_resource},
        models::enums::Resources,
    },
    error::ArunaError,
};

use std::{str::FromStr, sync::Arc};
use tokio::task;
use tonic::metadata::{MetadataMap, MetadataValue};

// This macro automatically creates the Impl struct with all associated fields
crate::impl_grpc_server!(InternalEventServiceImpl);

#[tonic::async_trait]
impl InternalEventService for InternalEventServiceImpl {
    /// Creates a new notification stream group which can be used to fetch event notifications.
    ///
    /// ## Arguments:
    ///
    /// * `request` - gRPC request containing the information to create a new notification stream group
    ///
    /// ## Returns:
    ///
    /// * `Result<tonic::Response<CreateStreamGroupResponse>, tonic::Status>` -
    /// gRPC response containing the id of the newly created notification stream group
    ///
    async fn create_stream_group(
        &self,
        request: tonic::Request<CreateStreamGroupRequest>,
    ) -> Result<tonic::Response<CreateStreamGroupResponse>, tonic::Status> {
        // Consume gRPC request
        let inner_request = request.into_inner();

        // Create metadata map with provided token for authorization
        let mut metadata = MetadataMap::new();
        metadata.insert(
            "Authorization",
            MetadataValue::try_from(format!("Bearer {}", inner_request.token).as_str())
                .map_err(|err| tonic::Status::invalid_argument(err.to_string()))?,
        );

        // Extract provided resource id from request
        let resource_ulid = diesel_ulid::DieselUlid::from_str(&inner_request.resource_id)
            .map_err(ArunaError::from)?;

        // Authorize against resource associated with the stream group
        self.authz
            .resource_read_authorize(
                metadata,
                resource_ulid,
                ResourceType::from_i32(inner_request.resource_type).ok_or_else(|| {
                    ArunaError::InvalidRequest("Invalid resource type".to_string())
                })?,
            )
            .await?;

        // Create NotificationStreamGroup in database
        let database_clone = self.database.clone();
        let db_stream_group = task::spawn_blocking(move || {
            database_clone.create_notification_stream_group(
                diesel_ulid::DieselUlid::generate(),
                resource_ulid.clone(),
                grpc_to_db_resource(&inner_request.resource_type)?,
                inner_request.notify_on_sub_resource,
            )
        })
        .await
        .map_err(ArunaError::from)??;

        // Convert to proto StreamGroup
        let proto_stream_group = StreamGroup {
            id: db_stream_group.id.to_string(),
            event_type: inner_request.event_type, // Currently everytime EventType::All
            resource_type: inner_request.resource_type,
            resource_id: resource_ulid.to_string(),
            notify_on_sub_resource: inner_request.notify_on_sub_resource,
        };

        // Create and return gRPC response
        Ok(tonic::Response::new(CreateStreamGroupResponse {
            stream_group: Some(proto_stream_group),
        }))
    }

    /// Get all meta information associated with an already existing notification stream group.
    ///
    /// ## Arguments::
    ///
    /// * `request` - gRPC request containing the stream group id and an authorization token
    ///
    /// ## Results::
    ///
    /// * `Result<tonic::Response<GetStreamGroupResponse>, tonic::Status>` -
    /// gRPC response containing the stream group information; Error else.
    ///
    async fn get_stream_group(
        &self,
        request: tonic::Request<GetStreamGroupRequest>,
    ) -> Result<tonic::Response<GetStreamGroupResponse>, tonic::Status> {
        // Consume gRPC request
        let inner_request = request.into_inner();

        // Extract stream group ulid from request
        let stream_group_ulid = diesel_ulid::DieselUlid::from_str(&inner_request.stream_group_id)
            .map_err(ArunaError::from)?;

        // Create dummy header metadata for authorization
        let mut metadata = MetadataMap::new();
        metadata.insert(
            "Authorization",
            MetadataValue::try_from(format!("Bearer {}", inner_request.token).as_str())
                .map_err(|err| tonic::Status::invalid_argument(err.to_string()))?,
        );

        // Fetch NotificationStreamGroup from database
        let database_clone = self.database.clone();
        let db_stream_group = task::spawn_blocking(move || {
            database_clone.get_notification_stream_group(stream_group_ulid)
        })
        .await
        .map_err(ArunaError::from)??;

        // Create dummy header metadata for authorization
        let mut metadata = MetadataMap::new();
        metadata.insert(
            "Authorization",
            MetadataValue::try_from(format!("Bearer {}", inner_request.token).as_str())
                .map_err(|err| tonic::Status::invalid_argument(err.to_string()))?,
        );

        // Authorize against resource associated with the stream group
        self.authz
            .resource_read_authorize(
                metadata,
                db_stream_group.resource_id,
                db_to_grpc_resource(db_stream_group.resource_type)?,
            )
            .await?;

        // Convert to proto StreamGroup
        let proto_stream_group = StreamGroup {
            id: db_stream_group.id.to_string(),
            event_type: EventType::All as i32, //Note: Currently this is permanent EventType::All
            resource_type: db_to_grpc_resource(db_stream_group.resource_type)? as i32,
            resource_id: db_stream_group.resource_id.to_string(),
            notify_on_sub_resource: db_stream_group.notify_on_sub_resources,
        };

        // Create and return gRPC response
        Ok(tonic::Response::new(GetStreamGroupResponse {
            stream_group: Some(proto_stream_group),
        }))
    }

    /// Deletes the notification stream group associated with the provided id.
    ///
    /// ## Arguments:
    ///
    /// * `request` - gRPC request containing the notification stream group id and
    /// the authorization token
    ///
    /// ## Returns:
    ///
    /// * `Result<tonic::Response<DeleteStreamGroupResponse>, tonic::Status>` -
    /// An empty response signals a successful deletion; Error else.
    async fn delete_stream_group(
        &self,
        request: tonic::Request<DeleteStreamGroupRequest>,
    ) -> Result<tonic::Response<DeleteStreamGroupResponse>, tonic::Status> {
        // Consume gRPC request
        let inner_request = request.into_inner();

        // Extract request parameter
        let stream_group_ulid = diesel_ulid::DieselUlid::from_str(&inner_request.stream_group_id)
            .map_err(ArunaError::from)?;

        // Create dummy header metadata for authorization
        let mut metadata = MetadataMap::new();
        metadata.insert(
            "Authorization",
            MetadataValue::try_from(format!("Bearer {}", inner_request.token).as_str())
                .map_err(|err| tonic::Status::invalid_argument(err.to_string()))?,
        );

        // Fetch NotificationStreamGroup from database
        let database_clone = self.database.clone();
        let authz_clone = self.authz.clone();
        task::spawn(async move {
            // Fetch stream group meta
            let db_stream_group =
                database_clone.get_notification_stream_group(stream_group_ulid)?;

            // Authorize against resource associated with the stream group
            authz_clone
                .resource_read_authorize(
                    metadata,
                    db_stream_group.resource_id,
                    db_to_grpc_resource(db_stream_group.resource_type)?,
                )
                .await?;

            database_clone.delete_notification_stream_group(stream_group_ulid)
        })
        .await
        .map_err(ArunaError::from)??;

        // Create and return gRPC response
        Ok(tonic::Response::new(DeleteStreamGroupResponse {}))
    }

    /// Get the shared revision id of the provided resource.
    ///
    /// This service call is only viable for resources which have a shared revision id
    /// i.e. objects and object groups.
    ///
    /// ## Arguments:
    ///
    /// * `request` - gRPC request containing the resource id and resource type
    ///
    /// ## Returns:
    ///
    /// * `Result<tonic::Response<GetSharedRevisionResponse>, tonic::Status>` -
    /// gRPC response containing the shared revision id; Error else.
    ///
    async fn get_shared_revision(
        &self,
        request: tonic::Request<GetSharedRevisionRequest>,
    ) -> Result<tonic::Response<GetSharedRevisionResponse>, tonic::Status> {
        // Consume gRPC request
        let inner_request = request.into_inner();

        // Extract request parameter
        let resource_ulid = diesel_ulid::DieselUlid::from_str(&inner_request.resource_id)
            .map_err(ArunaError::from)?;
        let resource_type = grpc_to_db_resource(&inner_request.resource_type)?;

        // Check resource type to fail fast before opening a transaction
        match resource_type {
            Resources::PROJECT | Resources::COLLECTION => {
                return Err(tonic::Status::new(
                    tonic::Code::InvalidArgument,
                    "Only object or object groups are supported",
                ))
            }
            _ => {} // Ignore valid types
        };

        // Fetch NotificationStreamGroup from database
        let database_clone = self.database.clone();
        let shared_revision_ulid = task::spawn_blocking(move || {
            database_clone.get_resource_shared_revision(resource_ulid, resource_type)
        })
        .await
        .map_err(ArunaError::from)??;

        // Create and return gRPC response
        Ok(tonic::Response::new(GetSharedRevisionResponse {
            shared_revision_id: shared_revision_ulid.to_string(),
        }))
    }
}
