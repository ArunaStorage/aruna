use crate::structs::Object as DPObject;
use crate::structs::ObjectLocation;
use crate::structs::PubKey;
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
use aruna_rust_api::api::storage::models::v2::Hash;
use aruna_rust_api::api::storage::models::v2::Object;
use aruna_rust_api::api::storage::models::v2::Project;
use aruna_rust_api::api::storage::models::v2::User as GrpcUser;
use aruna_rust_api::api::storage::services::v2::CreateCollectionRequest;
use aruna_rust_api::api::storage::services::v2::CreateDatasetRequest;
use aruna_rust_api::api::storage::services::v2::CreateObjectRequest;
use aruna_rust_api::api::storage::services::v2::CreateProjectRequest;
use aruna_rust_api::api::storage::services::v2::FinishObjectStagingRequest;
use aruna_rust_api::api::storage::services::v2::GetCollectionRequest;
use aruna_rust_api::api::storage::services::v2::GetDatasetRequest;
use aruna_rust_api::api::storage::services::v2::GetObjectRequest;
use aruna_rust_api::api::storage::services::v2::GetProjectRequest;
use aruna_rust_api::api::storage::services::v2::GetPubkeysRequest;
use aruna_rust_api::api::storage::services::v2::GetUserRedactedRequest;
use aruna_rust_api::api::storage::services::v2::Pubkey;
use aruna_rust_api::api::{
    notification::services::v2::event_notification_service_client::EventNotificationServiceClient,
    storage::services::v2::{
        collection_service_client::CollectionServiceClient,
        dataset_service_client::DatasetServiceClient,
        endpoint_service_client::EndpointServiceClient, object_service_client::ObjectServiceClient,
        project_service_client::ProjectServiceClient,
        storage_status_service_client::StorageStatusServiceClient,
        user_service_client::UserServiceClient,
    },
};
use diesel_ulid::DieselUlid;
use std::str::FromStr;
use std::sync::Arc;
use tonic::metadata::AsciiMetadataKey;
use tonic::metadata::AsciiMetadataValue;
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::Request;

use super::cache::Cache;

pub struct GrpcQueryHandler {
    project_service: ProjectServiceClient<Channel>,
    collection_service: CollectionServiceClient<Channel>,
    dataset_service: DatasetServiceClient<Channel>,
    object_service: ObjectServiceClient<Channel>,
    user_service: UserServiceClient<Channel>,
    _endpoint_service: EndpointServiceClient<Channel>,
    storage_status_service: StorageStatusServiceClient<Channel>,
    event_notification_service: EventNotificationServiceClient<Channel>,
    cache: Arc<Cache>,
    endpoint_id: String,
    long_lived_token: String,
}

impl GrpcQueryHandler {
    #[allow(dead_code)]
    pub async fn new(
        server: impl Into<String>,
        cache: Arc<Cache>,
        endpoint_id: String,
    ) -> Result<Self> {
        //let tls_config = ClientTlsConfig::new();
        let endpoint = Channel::from_shared(server.into())?;
        let channel = endpoint.connect().await?;

        let project_service = ProjectServiceClient::new(channel.clone());

        let collection_service = CollectionServiceClient::new(channel.clone());

        let dataset_service = DatasetServiceClient::new(channel.clone());

        let object_service = ObjectServiceClient::new(channel.clone());

        let user_service = UserServiceClient::new(channel.clone());

        let _endpoint_service = EndpointServiceClient::new(channel.clone());

        let storage_status_service = StorageStatusServiceClient::new(channel.clone());

        let event_notification_service = EventNotificationServiceClient::new(channel);

        let long_lived_token = cache
            .auth
            .read()
            .await
            .as_ref()
            .ok_or_else(|| anyhow!("No auth found"))?
            .sign_notification_token()?;

        let handler = GrpcQueryHandler {
            project_service,
            collection_service,
            dataset_service,
            object_service,
            user_service,
            _endpoint_service,
            storage_status_service,
            event_notification_service,
            cache,
            endpoint_id,
            long_lived_token,
        };

        let pks = handler
            .get_pubkeys()
            .await?
            .into_iter()
            .map(PubKey::from)
            .collect();
        handler.cache.set_pubkeys(pks).await?;

        Ok(handler)
    }
}

// Aruna grpc request section
impl GrpcQueryHandler {
    pub async fn get_user(&self, id: DieselUlid, _checksum: String) -> Result<GrpcUser> {
        let mut req = Request::new(GetUserRedactedRequest {
            user_id: id.to_string(),
        });

        req.metadata_mut().append(
            AsciiMetadataKey::from_bytes("authorization".as_bytes())?,
            AsciiMetadataValue::try_from(format!("Bearer {}", self.long_lived_token))?,
        );

        let user = self
            .user_service
            .clone()
            .get_user_redacted(req)
            .await?
            .into_inner()
            .user
            .ok_or(anyhow!("Unknown user"))?;
        Ok(user)
    }
    async fn get_pubkeys(&self) -> Result<Vec<Pubkey>> {
        let mut req = Request::new(GetPubkeysRequest {});

        req.metadata_mut().append(
            AsciiMetadataKey::from_bytes("authorization".as_bytes())?,
            AsciiMetadataValue::try_from(format!("Bearer {}", self.long_lived_token.as_str()))?,
        );

        Ok(self
            .storage_status_service
            .clone()
            .get_pubkeys(req)
            .await?
            .into_inner()
            .pubkeys)
    }

    pub async fn create_project(&self, object: DPObject, token: &str) -> Result<DPObject> {
        let mut req = Request::new(CreateProjectRequest::from(object));

        req.metadata_mut().append(
            AsciiMetadataKey::from_bytes("authorization".as_bytes())?,
            AsciiMetadataValue::try_from(format!("Bearer {}", token))?,
        );

        let response = self
            .project_service
            .clone()
            .create_project(req)
            .await?
            .into_inner()
            .project
            .ok_or(anyhow!("unknown project"))?;

        let object = DPObject::try_from(response)?;

        self.cache.upsert_object(object.clone(), None).await?;

        Ok(object)
    }

    async fn get_project(&self, id: &DieselUlid, _checksum: String) -> Result<Project> {
        let mut req = Request::new(GetProjectRequest {
            project_id: id.to_string(),
        });

        req.metadata_mut().append(
            AsciiMetadataKey::from_bytes("authorization".as_bytes())?,
            AsciiMetadataValue::try_from(format!("Bearer {}", self.long_lived_token.as_str()))?,
        );

        self.project_service
            .clone()
            .get_project(req)
            .await?
            .into_inner()
            .project
            .ok_or(anyhow!("unknown project"))
    }

    async fn get_collection(&self, id: &DieselUlid, _checksum: String) -> Result<Collection> {
        let mut req = Request::new(GetCollectionRequest {
            collection_id: id.to_string(),
        });

        req.metadata_mut().append(
            AsciiMetadataKey::from_bytes("authorization".as_bytes())?,
            AsciiMetadataValue::try_from(format!("Bearer {}", self.long_lived_token.as_str()))?,
        );

        self.collection_service
            .clone()
            .get_collection(req)
            .await?
            .into_inner()
            .collection
            .ok_or(anyhow!("unknown collection"))
    }

    pub async fn create_collection(&self, object: DPObject, token: &str) -> Result<DPObject> {
        let mut req = Request::new(CreateCollectionRequest::from(object));

        req.metadata_mut().append(
            AsciiMetadataKey::from_bytes("authorization".as_bytes())?,
            AsciiMetadataValue::try_from(format!("Bearer {}", token))?,
        );

        let response = self
            .collection_service
            .clone()
            .create_collection(req)
            .await?
            .into_inner()
            .collection
            .ok_or(anyhow!("unknown project"))?;

        let object = DPObject::try_from(response)?;

        self.cache.upsert_object(object.clone(), None).await?;

        Ok(object)
    }

    async fn get_dataset(&self, id: &DieselUlid, _checksum: String) -> Result<Dataset> {
        let mut req = Request::new(GetDatasetRequest {
            dataset_id: id.to_string(),
        });

        req.metadata_mut().append(
            AsciiMetadataKey::from_bytes("authorization".as_bytes())?,
            AsciiMetadataValue::try_from(format!("Bearer {}", self.long_lived_token.as_str()))?,
        );

        self.dataset_service
            .clone()
            .get_dataset(req)
            .await?
            .into_inner()
            .dataset
            .ok_or(anyhow!("unknown dataset"))
    }

    pub async fn create_dataset(&self, object: DPObject, token: &str) -> Result<DPObject> {
        let mut req = Request::new(CreateDatasetRequest::from(object));

        req.metadata_mut().append(
            AsciiMetadataKey::from_bytes("authorization".as_bytes())?,
            AsciiMetadataValue::try_from(format!("Bearer {}", token))?,
        );

        let response = self
            .dataset_service
            .clone()
            .create_dataset(req)
            .await?
            .into_inner()
            .dataset
            .ok_or(anyhow!("unknown project"))?;

        let object = DPObject::try_from(response)?;

        self.cache.upsert_object(object.clone(), None).await?;

        Ok(object)
    }

    async fn get_object(&self, id: &DieselUlid, _checksum: String) -> Result<Object> {
        let mut req = Request::new(GetObjectRequest {
            object_id: id.to_string(),
        });

        req.metadata_mut().append(
            AsciiMetadataKey::from_bytes("authorization".as_bytes())?,
            AsciiMetadataValue::try_from(format!("Bearer {}", self.long_lived_token.as_str()))?,
        );

        self.object_service
            .clone()
            .get_object(req)
            .await?
            .into_inner()
            .object
            .ok_or(anyhow!("unknown object"))
    }

    pub async fn create_object(&self, object: DPObject, token: &str) -> Result<DPObject> {
        let mut req = Request::new(CreateObjectRequest::from(object));

        req.metadata_mut().append(
            AsciiMetadataKey::from_bytes("authorization".as_bytes())?,
            AsciiMetadataValue::try_from(format!("Bearer {}", token))?,
        );

        let response = self
            .object_service
            .clone()
            .create_object(req)
            .await?
            .into_inner()
            .object
            .ok_or(anyhow!("unknown project"))?;

        let object = DPObject::try_from(response)?;

        self.cache.upsert_object(object.clone(), None).await?;

        Ok(object)
    }

    pub async fn finish_object(
        &self,
        object_id: DieselUlid,
        content_len: i64,
        hashes: Vec<Hash>,
        token: &str,
    ) -> Result<DPObject> {
        let mut req = Request::new(FinishObjectStagingRequest {
            object_id: object_id.to_string(),
            content_len,
            hashes,
            completed_parts: vec![],
        });

        req.metadata_mut().append(
            AsciiMetadataKey::from_bytes("authorization".as_bytes())?,
            AsciiMetadataValue::try_from(format!("Bearer {}", token))?,
        );

        let response = self
            .object_service
            .clone()
            .finish_object_staging(req)
            .await?
            .into_inner()
            .object
            .ok_or(anyhow!("unknown project"))?;

        let object = DPObject::try_from(response)?;

        self.cache.upsert_object(object.clone(), None).await?;

        Ok(object)
    }

    pub async fn create_and_finish(
        &self,
        object: DPObject,
        loc: ObjectLocation,
        token: &str,
    ) -> Result<DPObject> {
        let mut req = Request::new(CreateObjectRequest::from(object.clone()));

        req.metadata_mut().append(
            AsciiMetadataKey::from_bytes("authorization".as_bytes())?,
            AsciiMetadataValue::try_from(format!("Bearer {}", token))?,
        );

        self.object_service.clone().create_object(req).await?;

        let mut req = Request::new(FinishObjectStagingRequest {
            object_id: object.id.to_string(),
            content_len: loc.raw_content_len,
            hashes: object.get_hashes(),
            completed_parts: vec![],
        });

        req.metadata_mut().append(
            AsciiMetadataKey::from_bytes("authorization".as_bytes())?,
            AsciiMetadataValue::try_from(format!("Bearer {}", token))?,
        );

        let response = self
            .object_service
            .clone()
            .finish_object_staging(req)
            .await?
            .into_inner()
            .object
            .ok_or(anyhow!("unknown project"))?;
        let object = DPObject::try_from(response)?;
        self.cache.upsert_object(object.clone(), None).await?;
        Ok(object)
    }

    pub async fn create_notifications_channel(&self) -> Result<()> {
        let mut req = Request::new(GetEventMessageBatchStreamRequest {
            stream_consumer: self.endpoint_id.to_string(),
            batch_size: 10,
        });

        req.metadata_mut().append(
            AsciiMetadataKey::from_bytes("authorization".as_bytes())?,
            AsciiMetadataValue::try_from(format!("Bearer {}", self.long_lived_token.as_str()))?,
        );

        let stream = self
            .event_notification_service
            .clone()
            .get_event_message_batch_stream(req)
            .await?;

        let mut inner_stream = stream.into_inner();

        while let Some(m) = inner_stream.message().await? {
            let mut acks = Vec::new();
            for message in m.messages {
                log::debug!("Received message: {:?}", message);

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
}

/// Request handling section
impl GrpcQueryHandler {
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
                let user_info = self.get_user(uid, message.checksum.clone()).await?;
                self.cache.upsert_user(user_info.clone()).await?;
            }
            EventVariant::Deleted => {
                let uid = DieselUlid::from_str(&message.user_id)?;

                self.cache.remove_user(uid).await?;
            }
            EventVariant::Unspecified => (),
        }

        Ok(message.reply)
    }

    async fn process_resource_event(&self, event: ResourceEvent) -> Result<Option<Reply>> {
        match event.event_variant() {
            EventVariant::Created | EventVariant::Updated => {
                if let Some(r) = event.resource {
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
