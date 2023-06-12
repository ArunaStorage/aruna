use aruna_rust_api::api::{
    internal::v1::{
        internal_event_emitter_service_client::InternalEventEmitterServiceClient, EmitEventRequest,
        EmitEventResponse, EmittedResource,
    },
    notification::services::v1::EventType,
    storage::models::v1::ResourceType,
};
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue};

use crate::error::ArunaError;

// Create a client interceptor which always adds the specified API token to the request header
#[derive(Clone)]
pub struct ClientInterceptor {
    pub internal_token: String,
}
// Implement a request interceptor which always adds the authorization header with a specific API token to all requests
impl tonic::service::Interceptor for ClientInterceptor {
    fn call(&mut self, request: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        let mut mut_req: tonic::Request<()> = request;
        let metadata = mut_req.metadata_mut();
        metadata.append(
            AsciiMetadataKey::from_bytes("Authorization".as_bytes())
                .map_err(|err| tonic::Status::invalid_argument(err.to_string()))?,
            AsciiMetadataValue::try_from(self.internal_token.to_string())
                .map_err(|err| tonic::Status::invalid_argument(err.to_string()))?,
        );

        Ok(mut_req)
    }
}

#[derive(Clone)]
pub struct NotificationEmitClient {
    pub emitter_endpoint: tonic::transport::Endpoint,
    interceptor: ClientInterceptor,
}

impl NotificationEmitClient {
    pub async fn new(emitter_host: String, internal_token: String) -> Result<Self, ArunaError> {
        Ok(Self {
            emitter_endpoint: tonic::transport::Endpoint::try_from(emitter_host).map_err(
                |err| {
                    ArunaError::ConnectionError(
                        crate::error::ConnectionError::TonicConnectionError(err),
                    )
                },
            )?,
            interceptor: ClientInterceptor { internal_token },
        })
    }

    /// Tries to connect to an internal event emitter service to emit the
    /// event notification.
    ///
    /// ## Arguments:
    ///
    /// * `resource_id` - Unique id of the resource
    /// * `resource_type` - Type of the resource
    /// * `event_type` - Action that was performed with the resource
    /// * `emitted_resource` - Hierarchy of ids associated with the resource
    ///
    /// ## Returns::
    ///
    /// * `Result<(), ArunaError>` - An empty Ok result signals success; Error else.
    ///
    pub async fn emit_event(
        &self,
        resource_id: String,
        resource_type: ResourceType,
        event_type: EventType,
        emitted_resource: Vec<EmittedResource>,
    ) -> Result<EmitEventResponse, ArunaError> {
        let response = InternalEventEmitterServiceClient::with_interceptor(
            self.emitter_endpoint
                .clone()
                .connect()
                .await
                .map_err(|err| {
                    ArunaError::ConnectionError(
                        crate::error::ConnectionError::TonicConnectionError(err),
                    )
                })?,
            self.interceptor.clone(),
        )
        .emit_event(EmitEventRequest {
            event_resource: resource_type as i32,
            resource_id,
            event_type: event_type as i32,
            resources: emitted_resource,
        })
        .await?
        .into_inner();

        Ok(response)
    }
}
