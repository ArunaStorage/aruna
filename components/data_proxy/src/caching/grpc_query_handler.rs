use anyhow::anyhow;
use anyhow::Result;
use aruna_rust_api::api::notification::services::v2::anouncement_event;
use aruna_rust_api::api::notification::services::v2::event_message::MessageVariant;
use aruna_rust_api::api::notification::services::v2::AcknowledgeMessageBatchRequest;
use aruna_rust_api::api::notification::services::v2::AnouncementEvent;
use aruna_rust_api::api::notification::services::v2::EventMessage;
use aruna_rust_api::api::notification::services::v2::EventVariant;
use aruna_rust_api::api::notification::services::v2::GetEventMessageBatchStreamRequest;
use aruna_rust_api::api::notification::services::v2::Reply;
use aruna_rust_api::api::notification::services::v2::ResourceEvent;
use aruna_rust_api::api::notification::services::v2::UserEvent;
use aruna_rust_api::api::storage::models::v2::Collection;
use aruna_rust_api::api::storage::models::v2::Dataset;
use aruna_rust_api::api::storage::models::v2::Object;
use aruna_rust_api::api::storage::models::v2::Project;
use aruna_rust_api::api::storage::models::v2::User as GrpcUser;
use aruna_rust_api::api::storage::services::v2::GetCollectionRequest;
use aruna_rust_api::api::storage::services::v2::GetDatasetRequest;
use aruna_rust_api::api::storage::services::v2::GetObjectRequest;
use aruna_rust_api::api::storage::services::v2::GetProjectRequest;
use aruna_rust_api::api::storage::services::v2::GetPubkeysRequest;
use aruna_rust_api::api::storage::services::v2::GetUserRedactedRequest;
use aruna_rust_api::api::storage::services::v2::Pubkey;
use aruna_rust_api::api::{
    notification::services::v2::event_notification_service_client::{
        self, EventNotificationServiceClient,
    },
    storage::services::v2::{
        collection_service_client::{self, CollectionServiceClient},
        dataset_service_client::{self, DatasetServiceClient},
        endpoint_service_client::{self, EndpointServiceClient},
        object_service_client::{self, ObjectServiceClient},
        project_service_client::{self, ProjectServiceClient},
        storage_status_service_client::{self, StorageStatusServiceClient},
        user_service_client::{self, UserServiceClient},
    },
};
use diesel_ulid::DieselUlid;
use std::str::FromStr;
use std::sync::Arc;
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::Request;

use crate::structs::PubKey;

use super::cache::Cache;

pub struct GrpcQueryHandler {
    project_service: ProjectServiceClient<Channel>,
    collection_service: CollectionServiceClient<Channel>,
    dataset_service: DatasetServiceClient<Channel>,
    object_service: ObjectServiceClient<Channel>,
    user_service: UserServiceClient<Channel>,
    endpoint_service: EndpointServiceClient<Channel>,
    storage_status_service: StorageStatusServiceClient<Channel>,
    event_notification_service: EventNotificationServiceClient<Channel>,
    cache: Arc<Cache>,
}

impl GrpcQueryHandler {
    #[allow(dead_code)]
    pub async fn new(server: impl Into<String>, cache: Arc<Cache>) -> Result<Self> {
        let tls_config = ClientTlsConfig::new();
        let endpoint = Channel::from_shared(server.into())?.tls_config(tls_config)?;
        let channel = endpoint.connect().await?;

        let project_service = project_service_client::ProjectServiceClient::new(channel.clone());

        let collection_service =
            collection_service_client::CollectionServiceClient::new(channel.clone());

        let dataset_service = dataset_service_client::DatasetServiceClient::new(channel.clone());

        let object_service = object_service_client::ObjectServiceClient::new(channel.clone());

        let user_service = user_service_client::UserServiceClient::new(channel.clone());

        let endpoint_service = endpoint_service_client::EndpointServiceClient::new(channel.clone());

        let storage_status_service =
            storage_status_service_client::StorageStatusServiceClient::new(channel.clone());

        let event_notification_service =
            event_notification_service_client::EventNotificationServiceClient::new(channel);

        Ok(GrpcQueryHandler {
            project_service,
            collection_service,
            dataset_service,
            object_service,
            user_service,
            endpoint_service,
            storage_status_service,
            event_notification_service,
            cache,
        })
    }
}

impl GrpcQueryHandler {
    pub async fn get_user(&self, id: DieselUlid, _checksum: String) -> Result<GrpcUser> {
        let user = self
            .user_service
            .clone()
            .get_user_redacted(Request::new(GetUserRedactedRequest {
                user_id: id.to_string(),
            }))
            .await?
            .into_inner()
            .user
            .ok_or(anyhow!("Unknown user"))?;
        Ok(user)
    }
    async fn get_pubkeys(&self) -> Result<Vec<Pubkey>> {
        Ok(self
            .storage_status_service
            .clone()
            .get_pubkeys(Request::new(GetPubkeysRequest {}))
            .await?
            .into_inner()
            .pubkeys)
    }
    async fn get_project(&self, id: &DieselUlid, _checksum: String) -> Result<Project> {
        self.project_service
            .clone()
            .get_project(Request::new(GetProjectRequest {
                project_id: id.to_string(),
            }))
            .await?
            .into_inner()
            .project
            .ok_or(anyhow!("unknown project"))
    }

    async fn get_collection(&self, id: &DieselUlid, _checksum: String) -> Result<Collection> {
        self.collection_service
            .clone()
            .get_collection(Request::new(GetCollectionRequest {
                collection_id: id.to_string(),
            }))
            .await?
            .into_inner()
            .collection
            .ok_or(anyhow!("unknown collection"))
    }

    async fn get_dataset(&self, id: &DieselUlid, _checksum: String) -> Result<Dataset> {
        self.dataset_service
            .clone()
            .get_dataset(Request::new(GetDatasetRequest {
                dataset_id: id.to_string(),
            }))
            .await?
            .into_inner()
            .dataset
            .ok_or(anyhow!("unknown dataset"))
    }

    async fn get_object(&self, id: &DieselUlid, _checksum: String) -> Result<Object> {
        self.object_service
            .clone()
            .get_object(Request::new(GetObjectRequest {
                object_id: id.to_string(),
            }))
            .await?
            .into_inner()
            .object
            .ok_or(anyhow!("unknown object"))
    }

    pub async fn create_notifications_channel(&self, stream_consumer: String) -> Result<()> {
        let stream = self
            .event_notification_service
            .clone()
            .get_event_message_batch_stream(Request::new(GetEventMessageBatchStreamRequest {
                stream_consumer,
                batch_size: 10,
            }))
            .await?;

        let mut inner_stream = stream.into_inner();

        while let Some(m) = inner_stream.message().await? {
            let mut acks = Vec::new();
            for message in m.messages {
                if let Ok(Some(r)) = self.process_message(message).await {
                    acks.push(r)
                }
            }
            self.event_notification_service
                .clone()
                .acknowledge_message_batch(Request::new(AcknowledgeMessageBatchRequest {
                    replies: acks,
                }))
                .await?;
        }
        Err(anyhow!("Stream was closed by sender"))
    }

    async fn process_message(&self, message: EventMessage) -> Result<Option<Reply>> {
        match message.message_variant.unwrap() {
            MessageVariant::ResourceEvent(r_event) => self.process_resource_event(r_event).await,
            MessageVariant::UserEvent(u_event) => self.process_user_event(u_event).await,
            MessageVariant::AnnouncementEvent(a_event) => {
                self.process_announcements_event(a_event).await
            }
        }
    }

    async fn process_announcements_event(
        &self,
        message: AnouncementEvent,
    ) -> Result<Option<Reply>> {
        match message
            .event_variant
            .ok_or_else(|| anyhow!("No event variant"))?
        {
            anouncement_event::EventVariant::NewPubkey(_)
            | anouncement_event::EventVariant::RemovePubkey(_)
            | anouncement_event::EventVariant::NewDataProxyId(_)
            | anouncement_event::EventVariant::RemoveDataProxyId(_)
            | anouncement_event::EventVariant::UpdateDataProxyId(_) => {
                let pks = self
                    .get_pubkeys()
                    .await?
                    .into_iter()
                    .map(PubKey::from)
                    .collect();
                self.cache.set_pubkeys(pks).await?
            }
            anouncement_event::EventVariant::Downtime(_) => (),
            anouncement_event::EventVariant::Version(_) => (),
        }
        Ok(message.reply)
    }

    async fn process_user_event(&self, message: UserEvent) -> Result<Option<Reply>> {
        match message.event_variant() {
            EventVariant::Created | EventVariant::Available | EventVariant::Updated => {
                let uid = DieselUlid::from_str(&message.user_id)?;
                if self.cache.is_user(uid) {
                    let user_info = self.get_user(uid, message.checksum.clone()).await?;
                    self.cache.upsert_user(user_info.clone()).await?;
                };
            }
            EventVariant::Deleted => {
                let uid = DieselUlid::from_str(&message.user_id)?;

                if self.cache.is_user(uid) {
                    self.cache.remove_user(uid).await?;
                };
            }
            EventVariant::Unspecified => (),
        }

        Ok(message.reply)
    }

    async fn process_resource_event(&self, event: ResourceEvent) -> Result<Option<Reply>> {
        match event.event_variant() {
            EventVariant::Created | EventVariant::Updated => {
                if let Some(r) = event.resource {
                    if !self
                        .cache
                        .is_resource(DieselUlid::from_str(&r.resource_id)?)
                    {
                        return Ok(event.reply);
                    };
                    match r.resource_variant() {
                        aruna_rust_api::api::storage::models::v2::ResourceVariant::Project => {
                            let object = self
                                .get_project(&DieselUlid::from_str(&r.resource_id)?, r.checksum)
                                .await?;
                            self.cache.upsert_object(object.try_into()?, None).await?;
                        }
                        aruna_rust_api::api::storage::models::v2::ResourceVariant::Collection => {
                            let object = self
                                .get_collection(&DieselUlid::from_str(&r.resource_id)?, r.checksum)
                                .await?;
                            self.cache.upsert_object(object.try_into()?, None).await?;
                        }
                        aruna_rust_api::api::storage::models::v2::ResourceVariant::Dataset => {
                            let object = self
                                .get_dataset(&DieselUlid::from_str(&r.resource_id)?, r.checksum)
                                .await?;
                            self.cache.upsert_object(object.try_into()?, None).await?;
                        }
                        aruna_rust_api::api::storage::models::v2::ResourceVariant::Object => {
                            let object = self
                                .get_object(&DieselUlid::from_str(&r.resource_id)?, r.checksum)
                                .await?;
                            self.cache.upsert_object(object.try_into()?, None).await?;
                        }
                        _ => (),
                    }
                }
            }
            EventVariant::Deleted => {
                if let Some(r) = event.resource {
                    if !self
                        .cache
                        .is_resource(DieselUlid::from_str(&r.resource_id)?)
                    {
                        return Ok(event.reply);
                    };
                    self.cache
                        .delete_object(DieselUlid::from_str(&r.resource_id)?)
                        .await?;
                }
            }
            _ => (),
        }
        Ok(event.reply)
    }
}
