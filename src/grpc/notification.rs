use aruna_cache::notifications::NotificationCache;
use aruna_policy::ape::{
    policy_evaluator::PolicyEvaluator,
    structs::{
        ApeResourcePermission, ApeUserPermission, Context, PermissionLevels, ResourceContext,
    },
};
use aruna_rust_api::api::{
    notification::services::v2::{
        create_stream_consumer_request::{StreamType, Target},
        event_message::MessageVariant,
        event_notification_service_server::EventNotificationService,
        AcknowledgeMessageBatchRequest, AcknowledgeMessageBatchResponse,
        CreateStreamConsumerRequest, CreateStreamConsumerResponse,
        DeleteEventStreamingGroupRequest, DeleteEventStreamingGroupResponse, EventMessage,
        GetEventMessageBatchRequest, GetEventMessageBatchResponse,
        GetEventMessageBatchStreamRequest, GetEventMessageBatchStreamResponse,
    },
    storage::models::v2::ResourceVariant,
};
use async_nats::jetstream::consumer::DeliverPolicy;
use diesel_ulid::DieselUlid;
use std::str::FromStr;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Result, Status};

use crate::{
    database::{
        connection::Database, crud::CrudDb, dsls::notification_dsl::StreamConsumer,
        enums::ObjectType,
    },
    notification::{
        handler::{EventHandler, EventType},
        natsio_handler::NatsIoHandler,
        utils::{calculate_reply_hmac, parse_event_consumer_subject},
    },
    utils::conversions::get_token_from_md,
};

crate::impl_grpc_server!(
    NotificationServiceImpl,
    natsio_handler: NatsIoHandler
);

#[tonic::async_trait]
impl EventNotificationService for NotificationServiceImpl {
    /// Create a stream group which can be used to fetch event messages depending on the
    /// provided resource.
    ///
    /// ## Arguments:
    ///
    /// * `request: tonic::Request<CreateEventStreamingGroupRequest>` -
    /// Contains the needed information to create a new stream group.
    ///
    /// ## Returns:
    ///
    /// * `Result<tonic::Response<CreateEventStreamingGroupResponse>, tonic::Status>` -
    /// Response contains a vector of fetched messages; Error else.
    ///
    async fn create_stream_consumer(
        &self,
        request: tonic::Request<CreateStreamConsumerRequest>,
    ) -> Result<tonic::Response<CreateStreamConsumerResponse>, tonic::Status> {
        // Log some stuff
        log::info!("Received CreateStreamConsumerRequest.");
        log::debug!("{:?}", &request);

        // Consume gRPC request into its parts
        let (request_metadata, _, inner_request) = request.into_parts();

        // Extract token from request metadata
        let token = get_token_from_md(&request_metadata).map_err(|e| {
            log::debug!("{}", e);
            tonic::Status::unauthenticated("Token authentication error.")
        })?;

        // Evaluate fitting context and check permissions
        let perm_context = match inner_request.target {
            Some(stream_target) => {
                if let Target::Resource(resource) = stream_target {
                    let resource_permission = ApeResourcePermission {
                        id: DieselUlid::from_str(&resource.resource_id)
                            .map_err(|err| Status::invalid_argument(err.to_string()))?,
                        level: PermissionLevels::READ,
                        allow_sa: true,
                    };

                    Context::ResourceContext(match resource.resource_variant() {
                        ResourceVariant::Unspecified => {
                            return Err(Status::invalid_argument("Unspecified resource variant"))
                        }
                        ResourceVariant::Project => {
                            ResourceContext::Project(Some(resource_permission))
                        }
                        ResourceVariant::Collection => {
                            ResourceContext::Collection(resource_permission)
                        }
                        ResourceVariant::Dataset => ResourceContext::Dataset(resource_permission),
                        ResourceVariant::Object => ResourceContext::Object(resource_permission),
                    })
                } else if let Target::User(user_id) = stream_target {
                    let user_ulid = DieselUlid::from_str(&user_id)
                        .map_err(|_| Status::invalid_argument("Invalid user id format"))?;
                    Context::User(ApeUserPermission {
                        id: user_id,
                        allow_proxy: true,
                    })
                } else if let Target::Anouncements(_) = stream_target {
                    // Empty context -> just active user
                    return Err(Status::unimplemented(
                        "No permission checking context yet implemented",
                    ));
                } else {
                    // Rest should be Target::All
                    Context::GlobalAdmin
                }
            }
            None => return Err(Status::invalid_argument("Event target required")),
        };

        tonic_auth!(
            &self.authorizer.check_context(&token, perm_context).await,
            "Permission denied"
        );

        // Extract and convert delivery policy
        let deliver_policy = if let Some(stream_type) = inner_request.stream_type {
            convert_stream_type(stream_type)
                .map_err(|err| Status::invalid_argument(err.to_string()))?
        } else {
            DeliverPolicy::All
        };

        // Create stream consumer in Nats.io
        let (consumer_id, consumer_config) = self
            .natsio_handler
            .create_event_consumer(EventType::All, deliver_policy)
            .await
            .map_err(|_| Status::internal("Consumer creation failed"))?;

        // Create stream consumer in database
        let stream_consumer = StreamConsumer {
            id: DieselUlid::generate(),
            user_id: None,
            config: postgres_types::Json(consumer_config),
        };

        // Get database client
        let client = self.database.get_client().await.map_err(|e| {
            log::error!("{}", e);
            tonic::Status::unavailable("Database not avaliable.")
        })?;

        // Create consumer in Nats.io and delete database stream consumer on error
        stream_consumer.create(&client).await.map_err(|e| {
            let _ = self
                .natsio_handler
                .delete_event_consumer(consumer_id.to_string());
            Status::internal(e.to_string())
        })?;

        // Create gRPC response
        let grpc_response = Response::new(CreateStreamConsumerResponse {
            stream_consumer: consumer_id.to_string(),
        });

        log::info!("Sending CreateStreamConsumerResponse back to client.");
        log::debug!("{:?}", &grpc_response);
        return Ok(grpc_response);
    }

    /// Fetch messages from an existing stream group.
    ///
    /// ## Arguments:
    ///
    /// * `request: tonic::Request<GetEventMessageBatchRequest>` -
    /// Contains the consumer id and the maximum number of messages to be fetched
    ///
    /// ## Returns:
    ///
    /// * `Result<tonic::Response<GetEventMessageBatchResponse>, tonic::Status>` -
    /// Response contains a vector of fetched messages; Error else.
    ///
    async fn get_event_message_batch(
        &self,
        request: tonic::Request<GetEventMessageBatchRequest>,
    ) -> Result<tonic::Response<GetEventMessageBatchResponse>, tonic::Status> {
        // Log some stuff
        log::info!("Received GetEventMessageBatchRequest.");
        log::debug!("{:?}", &request);

        // Consume gRPC request into its parts
        let (request_metadata, _, inner_request) = request.into_parts();

        // Exrtact and
        let consumer_id = DieselUlid::from_str(&inner_request.stream_consumer)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;

        // Extract token from request metadata
        let token = get_token_from_md(&request_metadata).map_err(|e| {
            log::debug!("{}", e);
            tonic::Status::unauthenticated("Token authentication error.")
        })?;

        // Check empty permission context just to validate registered and active user
        tonic_auth!(
            &self.authorizer.check_context(&token, Context::Empty).await,
            "Permission denied"
        );

        // Fetch stream consumer, parse subject and check specific permissions. This is shit.
        let mut client = self.database.get_client().await.map_err(|e| {
            log::error!("{}", e);
            tonic::Status::unavailable("Database not available.")
        })?;

        let stream_consumer = StreamConsumer::get(consumer_id, &client)
            .await
            .map_err(|err| Status::aborted("Stream consumer fetech failed"))?;

        let specific_context = if let Some(consumer) = stream_consumer {
            match parse_event_consumer_subject(&consumer.config.0.filter_subject)
                .map_err(|_| Status::invalid_argument("Invalid consumer subject format"))?
            {
                EventType::Resource((resource_id, object_type, _)) => {
                    let res_perm = ApeResourcePermission {
                        id: tonic_invalid!(
                            DieselUlid::from_str(&resource_id),
                            "Invalid resource id"
                        ),
                        level: PermissionLevels::READ,
                        allow_sa: true,
                    };

                    Context::ResourceContext(match object_type {
                        ObjectType::PROJECT => ResourceContext::Project(Some(res_perm)),
                        ObjectType::COLLECTION => ResourceContext::Collection(res_perm),
                        ObjectType::DATASET => ResourceContext::Dataset(res_perm),
                        ObjectType::OBJECT => ResourceContext::Object(res_perm),
                    })
                }
                EventType::User(user_id) => Context::User(ApeUserPermission {
                    id: tonic_invalid!(DieselUlid::from_str(&user_id), "Invalid user id"),
                    allow_proxy: true,
                }),
                EventType::Announcement(_) => Context::Empty,
                EventType::All => Context::GlobalAdmin,
            }
        } else {
            return Err(Status::invalid_argument(format!(
                "Consumer with id {} does not exist.",
                consumer_id
            )));
        };

        tonic_auth!(
            &self
                .authorizer
                .check_context(&token, specific_context)
                .await,
            "Nope."
        );

        // Fetch messages of event consumer
        let nats_messages = self
            .natsio_handler
            .get_event_consumer_messages(consumer_id.to_string(), inner_request.batch_size)
            .await
            .map_err(|_| Status::internal("Stream consumer message fetch failed"))?;

        // Convert messages and add reply
        let mut proto_messages = vec![];
        for nats_message in nats_messages {
            // Convert Nats.io message to proto message
            let mut msg_variant: MessageVariant = serde_json::from_slice(
                nats_message.message.payload.to_vec().as_slice(),
            )
            .map_err(|_| tonic::Status::internal("Could not convert received Nats.io message"))?;

            // Create reply option
            let reply_subject = nats_message.reply.as_ref().ok_or_else(|| {
                tonic::Status::internal("Nats.io message is missing reply subject")
            })?;
            let msg_reply =
                calculate_reply_hmac(reply_subject, self.natsio_handler.reply_secret.clone());

            // Modify message with reply
            match msg_variant {
                MessageVariant::ResourceEvent(ref mut event) => event.reply = Some(msg_reply),
                MessageVariant::UserEvent(ref mut event) => event.reply = Some(msg_reply),
                MessageVariant::AnnouncementEvent(ref mut event) => event.reply = Some(msg_reply),
            }

            proto_messages.push(EventMessage {
                message_variant: Some(msg_variant),
            })
        }

        // Create gRPC response
        let grpc_response = Response::new(GetEventMessageBatchResponse {
            messages: proto_messages,
        });

        // Log some stuff and return response
        log::info!("Sending GetEventMessageBatchResponse back to client.");
        log::debug!("{:?}", &grpc_response);
        return Ok(grpc_response);
    }

    ///ToDo: Rust Doc
    type GetEventMessageBatchStreamStream =
        ReceiverStream<Result<GetEventMessageBatchStreamResponse, tonic::Status>>;

    ///ToDo: Rust Doc
    async fn get_event_message_batch_stream(
        &self,
        request: tonic::Request<GetEventMessageBatchStreamRequest>,
    ) -> Result<tonic::Response<Self::GetEventMessageBatchStreamStream>, tonic::Status> {
        todo!()
    }

    /// Manually acknowledges the provided message in the Nats cluster.
    /// The acknowledged messages will not be included in future requests that fetch
    /// messages of a subject under which the messages would be delivered.
    ///
    /// ## Arguments:
    ///
    /// * `tonic::Request<AcknowledgeMessageBatchRequest>` - Contains the replies
    /// of all messages which shall be acknowledged.
    ///
    /// ## Returns:
    ///
    /// * `Result<tonic::Response<AcknowledgeMessageBatchResponse>, tonic::Status>` -
    /// An empty response signals success that the specific messages could be acknowklkedged.
    ///
    async fn acknowledge_message_batch(
        &self,
        request: tonic::Request<AcknowledgeMessageBatchRequest>,
    ) -> Result<tonic::Response<AcknowledgeMessageBatchResponse>, tonic::Status> {
        log::info!("Received AcknowledgeMessageBatchRequest.");
        log::debug!("{:?}", &request);

        // Consume gRPC request into its parts
        let (request_metadata, _, inner_request) = request.into_parts();

        // Extract token from request metadata
        let token = get_token_from_md(&request_metadata).map_err(|e| {
            log::debug!("{}", e);
            tonic::Status::unauthenticated("Token authentication error.")
        })?;

        // Check empty permission context just to validate registered and active user
        tonic_auth!(
            &self.authorizer.check_context(&token, Context::Empty).await,
            "Permission denied"
        );

        // Acknowledge provided messages
        if let Err(err) = &self
            .natsio_handler
            .acknowledge_from_reply(inner_request.replies)
            .await
        {
            return Err(Status::aborted(err.to_string()));
        }

        // Create and return gRPC response
        let grpc_response = Response::new(AcknowledgeMessageBatchResponse {});

        log::info!("Sending AcknowledgeMessageBatchResponse back to client.");
        log::debug!("{:?}", &grpc_response);
        return Ok(grpc_response);
    }

    /// Deletes the notification stream group associated with the provided stream group id.
    ///
    /// ## Arguments:
    ///
    /// * `request` - Contains the stream group id to be deleted
    ///
    /// ## Returns:
    ///
    /// - `Result<tonic::Response<DeleteEventStreamingGroupResponse>, tonic::Status>` -
    /// An empty response signals deletion success; Error else.
    async fn delete_event_streaming_group(
        &self,
        request: tonic::Request<DeleteEventStreamingGroupRequest>,
    ) -> Result<tonic::Response<DeleteEventStreamingGroupResponse>, tonic::Status> {
        todo!()
    }
}

// ------------------------------------------- //
// ----- Helper functions -------------------- //
// ------------------------------------------- //

///ToDo: Rust Doc
fn convert_stream_type(stream_type: StreamType) -> anyhow::Result<DeliverPolicy> {
    match stream_type {
        StreamType::StreamAll(_) => Ok(DeliverPolicy::All),
        StreamType::StreamFromDate(info) => {
            if let Some(timestamp) = info.timestamp {
                Ok(DeliverPolicy::ByStartTime {
                    start_time: OffsetDateTime::from_unix_timestamp(timestamp.seconds)
                        .map_err(|_| Status::invalid_argument("Incorrect timestamp format"))?,
                })
            } else {
                return Err(anyhow::anyhow!("No timestamp provided"));
            }
        }
        StreamType::StreamFromSequence(info) => Ok(DeliverPolicy::ByStartSequence {
            start_sequence: info.sequence,
        }),
    }
}
