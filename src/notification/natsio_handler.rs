use std::time::Duration;

use aruna_rust_api::api::notification::services::v2::event_message::MessageVariant;
use aruna_rust_api::api::notification::services::v2::{
    EventVariant, Reply, Resource, ResourceEvent, UserEvent,
};

use aruna_rust_api::api::storage::models::v2::{ResourceVariant, User as ApiUser};
use async_nats::jetstream::consumer::{Config, DeliverPolicy, PullConsumer};

use async_nats::jetstream::{stream::Stream, Context, Message};

use async_trait::async_trait;
use diesel_ulid::DieselUlid;
use futures::future::try_join_all;
use futures::StreamExt;
use prost::bytes::Bytes;

use crate::database::dsls::object_dsl::ObjectWithRelations;
use crate::database::dsls::user_dsl::User;
use crate::database::enums::ObjectType;
use crate::utils::grpc_utils::{checksum_resource, checksum_user};

use super::handler::{EventHandler, EventStreamHandler, EventType};
use super::utils::{
    generate_announcement_subject, generate_resource_message_subject, generate_resource_subject,
    generate_user_message_subject, generate_user_subject, validate_reply_msg,
};

// ----- Constants used for notifications -------------------- //
pub const STREAM_NAME: &str = "AOS_STREAM";
pub const STREAM_SUBJECTS: [&str; 3] = ["AOS.RESOURCE.>", "AOS.USER.>", "AOS.ANNOUNCEMENT.>"];
// ----------------------------------------------------------- //

#[derive(Debug, Clone)]
pub struct NatsIoHandler {
    jetstream_context: Context,
    stream: Stream,
    pub reply_secret: String,
}

#[derive(Debug, Clone)]
pub struct NatsIOEventStreamHandler {
    pub consumer: PullConsumer,
}

#[async_trait::async_trait]
impl EventHandler for NatsIoHandler {
    ///ToDo: Rust Doc
    async fn register_event(&self, message_variant: MessageVariant) -> anyhow::Result<()> {
        // Generate subject
        let subject = match &message_variant {
            MessageVariant::ResourceEvent(event) => match &event.resource {
                Some(resource) => generate_resource_message_subject(
                    &resource.resource_id,
                    ObjectType::try_from(resource.resource_variant)?,
                ),
                None => return Err(anyhow::anyhow!("No event resource provided")),
            },
            MessageVariant::UserEvent(event) => generate_user_message_subject(&event.user_id),
            MessageVariant::AnnouncementEvent(_event) => todo!(), //generate_announcement_message_subject(event),
        };

        // Encode message
        let json_message = serde_json::to_string_pretty(&message_variant)?;
        let message_bytes = Bytes::from(json_message);

        // Publish message on stream
        match self.jetstream_context.publish(subject, message_bytes).await {
            Ok(_) => Ok(()),
            Err(err) => {
                log::error!("{}", err);
                Err(err.into())
            }
        }
    }

    ///ToDo: Rust Doc
    async fn create_event_consumer(
        &self,
        event_type: EventType,
        delivery_policy: DeliverPolicy,
    ) -> anyhow::Result<(DieselUlid, Config)> {
        // Generate stream consumer id/name
        let consumer_id = DieselUlid::generate();

        // Generate consumer subject
        let consumer_subject = match event_type {
            EventType::Resource((resource_id, resource_type, inc_sub)) => {
                generate_resource_subject(&resource_id, resource_type, inc_sub)
            }
            EventType::User(user_id) => generate_user_subject(&user_id),
            EventType::Announcement(_) => generate_announcement_subject(), // Currently all announcement messages are consumed equally
            EventType::All => "AOS.>".to_string(),
        };

        // Define consumer config
        let consumer_config = Config {
            name: Some(consumer_id.to_string()),
            durable_name: Some(consumer_id.to_string()),
            filter_subject: consumer_subject,
            deliver_policy: delivery_policy,
            ..Default::default()
        };

        // Create consumer with the generated filter subject
        self.stream.create_consumer(consumer_config.clone()).await?;

        // Return consumer id
        return Ok((consumer_id, consumer_config));
    }

    ///ToDo: Rust Doc
    async fn get_event_consumer_messages(
        &self,
        event_consumer_id: String,
        batch_size: u32,
    ) -> anyhow::Result<Vec<Message>> {
        // Get consumer from Nats.io stream
        let consumer = match self.stream.get_consumer(&event_consumer_id).await {
            Ok(consumer) => consumer,
            Err(err) => return Err(anyhow::anyhow!(err)),
        };

        // Fetch message batch from consumer
        let mut batch = consumer
            .batch()
            .expires(Duration::from_millis(250))
            .max_messages(batch_size as usize)
            .messages()
            .await?;

        // Convert Batch to vector of Message
        let mut messages = Vec::new();
        while let Some(Ok(message)) = batch.next().await {
            messages.push(message);
        }

        // Return vector
        return Ok(messages);
    }

    //ToDo: Rust Doc
    async fn acknowledge_from_reply(&self, replies: Vec<Reply>) -> anyhow::Result<()> {
        // Create vector to collect Nats.io acknowledge replies
        let mut reply_ack = Vec::new();
        for reply in replies {
            // Validate reply hmac
            match validate_reply_msg(reply.clone(), self.reply_secret.clone()) {
                Ok(hmac_matches) => {
                    if !hmac_matches {
                        return Err(anyhow::anyhow!(
                            "Message hmac signature did not match original signature"
                        ));
                    }
                }
                Err(err) => {
                    log::error!("{}", err);
                    return Err(err);
                    //anyhow::anyhow!("Could not validate reply msg")
                }
            }

            // Acknowledge message in Nats.io
            reply_ack.push(
                self.jetstream_context
                    .publish(reply.reply.clone(), "".into()),
            );
        }

        // Check if all messages could be acknowledged
        match try_join_all(reply_ack).await {
            Ok(_) => {}
            Err(err) => {
                return {
                    log::error!("{}", err);
                    Err(anyhow::anyhow!("Could not acknowledge all messages"))
                }
            }
        }

        return Ok(());
    }

    ///ToDo: Rust Doc
    async fn create_event_stream_handler(
        &self,
        event_consumer_id: String,
    ) -> anyhow::Result<Box<dyn EventStreamHandler + Send + Sync>> {
        // Fetch consumer from Nats.io stream
        let consumer = self
            .stream
            .get_consumer(&event_consumer_id)
            .await
            .map_err(|err| anyhow::anyhow!(err))?;

        // Create and return event stream handler
        let stream_handler = Box::new(NatsIOEventStreamHandler { consumer });

        return Ok(stream_handler);
    }

    async fn delete_event_consumer(&self, event_consumer_id: String) -> anyhow::Result<()> {
        // Just remove the consumer from the stream
        self.stream.delete_consumer(&event_consumer_id).await?;

        // Return empty Ok to signal success
        Ok(())
    }
}

impl NatsIoHandler {
    /// Convenience function to simplify the usage of NatsIoHandler::register_event(...)
    pub async fn register_resource_event(
        &self,
        object: &ObjectWithRelations,
        event_variant: EventVariant,
    ) -> anyhow::Result<()> {
        // Calculate resource checksum
        let resource_checksum = tonic_internal!(
            checksum_resource(tonic_internal!(
                object.clone().try_into(),
                "Proto conversion failed"
            )),
            "Checksum calculation failed"
        );

        // Emit resource event message
        self.register_event(MessageVariant::ResourceEvent(ResourceEvent {
            resource: Some(Resource {
                resource_id: object.object.id.to_string(),
                associated_id: "deprecated".to_string(), //ToDo: Will be removed with the next API version release
                persistent_resource_id: object.object.dynamic,
                checksum: resource_checksum,
                resource_variant: ResourceVariant::from(object.object.object_type) as i32,
            }),
            event_variant: event_variant as i32,
            reply: None, // Will be filled on message fetch
        }))
        .await
    }

    /// Convenience function to simplify the usage of NatsIoHandler::register_event(...)
    pub async fn register_user_event(
        &self,
        user: User,
        event_variant: EventVariant,
    ) -> anyhow::Result<()> {
        // Calculate user checksum
        let user_checksum = tonic_internal!(checksum_user(&ApiUser::from(user.clone())), "");

        self.register_event(MessageVariant::UserEvent(UserEvent {
            user_id: user.id.to_string(),
            event_variant: event_variant as i32,
            checksum: user_checksum,
            reply: None,
        }))
        .await
    }
}

#[async_trait]
impl EventStreamHandler for NatsIOEventStreamHandler {
    async fn get_event_consumer_messages(
        &self,
        max_batch_size: u32,
    ) -> anyhow::Result<Vec<async_nats::jetstream::Message>> {
        // Fetch messages from Nats.io
        let mut batch = self
            .consumer
            .batch()
            .expires(Duration::from_millis(250))
            .max_messages(max_batch_size as usize)
            .messages()
            .await?;

        // Convert message batch to message vector
        let mut messages = Vec::new();
        while let Some(Ok(message)) = batch.next().await {
            messages.push(message);
        }

        Ok(messages)
    }
}
