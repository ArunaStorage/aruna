use crate::structs::Object as DPObject;
use crate::structs::ObjectLocation;
use crate::structs::ObjectType;
use crate::structs::PubKey;
use crate::trace_err;
use anyhow::anyhow;
use anyhow::Result;
use aruna_rust_api::api::notification::services::v2::announcement_event;
use aruna_rust_api::api::notification::services::v2::event_message::MessageVariant;
use aruna_rust_api::api::notification::services::v2::AcknowledgeMessageBatchRequest;
use aruna_rust_api::api::notification::services::v2::AnnouncementEvent;
use aruna_rust_api::api::notification::services::v2::EventMessage;
use aruna_rust_api::api::notification::services::v2::EventVariant;
use aruna_rust_api::api::notification::services::v2::GetEventMessageStreamRequest;
use aruna_rust_api::api::notification::services::v2::Reply;
use aruna_rust_api::api::notification::services::v2::ResourceEvent;
use aruna_rust_api::api::notification::services::v2::UserEvent;
use aruna_rust_api::api::storage::models::v2::generic_resource::Resource;
use aruna_rust_api::api::storage::models::v2::Collection;
use aruna_rust_api::api::storage::models::v2::Dataset;
use aruna_rust_api::api::storage::models::v2::GenericResource;
use aruna_rust_api::api::storage::models::v2::Hash;
use aruna_rust_api::api::storage::models::v2::KeyValue;
use aruna_rust_api::api::storage::models::v2::KeyValueVariant;
use aruna_rust_api::api::storage::models::v2::Object;
use aruna_rust_api::api::storage::models::v2::Project;
use aruna_rust_api::api::storage::models::v2::Pubkey;
use aruna_rust_api::api::storage::models::v2::User as GrpcUser;
use aruna_rust_api::api::storage::services::v2::full_sync_endpoint_response::Target;
use aruna_rust_api::api::storage::services::v2::CreateCollectionRequest;
use aruna_rust_api::api::storage::services::v2::CreateDatasetRequest;
use aruna_rust_api::api::storage::services::v2::CreateObjectRequest;
use aruna_rust_api::api::storage::services::v2::CreateProjectRequest;
use aruna_rust_api::api::storage::services::v2::FinishObjectStagingRequest;
use aruna_rust_api::api::storage::services::v2::FullSyncEndpointRequest;
use aruna_rust_api::api::storage::services::v2::GetCollectionRequest;
use aruna_rust_api::api::storage::services::v2::GetDatasetRequest;
use aruna_rust_api::api::storage::services::v2::GetObjectRequest;
use aruna_rust_api::api::storage::services::v2::GetProjectRequest;
use aruna_rust_api::api::storage::services::v2::GetPubkeysRequest;
use aruna_rust_api::api::storage::services::v2::GetUserRedactedRequest;
use aruna_rust_api::api::storage::services::v2::UpdateObjectRequest;
use aruna_rust_api::api::storage::services::v2::UpdateProjectKeyValuesRequest;
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
use jsonwebtoken::DecodingKey;
use std::str::FromStr;
use std::sync::Arc;
use tonic::metadata::AsciiMetadataKey;
use tonic::metadata::AsciiMetadataValue;
use tonic::transport::Channel;
use tonic::transport::ClientTlsConfig;
use tonic::Request;
use tracing::debug;
use tracing::error;
use tracing::trace;
use tracing::Instrument;

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
    endpoint_id: String,
    long_lived_token: String,
}

impl GrpcQueryHandler {
    #[tracing::instrument(level = "trace", skip(server, cache, endpoint_id))]
    #[allow(dead_code)]
    pub async fn new(
        server: impl Into<String>,
        cache: Arc<Cache>,
        endpoint_id: String,
    ) -> Result<Self> {
        // Check if server host url is tls
        let server_url: String = server.into();
        let endpoint = if server_url.starts_with("https") {
            trace_err!(
                trace_err!(Channel::from_shared(server_url))?.tls_config(ClientTlsConfig::new())
            )?
        } else {
            trace_err!(Channel::from_shared(server_url))?
        };
        let channel = trace_err!(endpoint.connect().await)?;

        let project_service = ProjectServiceClient::new(channel.clone());

        let collection_service = CollectionServiceClient::new(channel.clone());

        let dataset_service = DatasetServiceClient::new(channel.clone());

        let object_service = ObjectServiceClient::new(channel.clone());

        let user_service = UserServiceClient::new(channel.clone());

        let endpoint_service = EndpointServiceClient::new(channel.clone());

        let storage_status_service = StorageStatusServiceClient::new(channel.clone());

        let event_notification_service = EventNotificationServiceClient::new(channel);

        let long_lived_token = trace_err!(cache
            .auth
            .read()
            .await
            .as_ref()
            .ok_or_else(|| anyhow!("No auth found")))?
        .sign_notification_token()?;

        let handler = GrpcQueryHandler {
            project_service,
            collection_service,
            dataset_service,
            object_service,
            user_service,
            endpoint_service,
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
    #[tracing::instrument(level = "trace", skip(self, _checksum))]
    pub async fn get_user(&self, id: DieselUlid, _checksum: String) -> Result<GrpcUser> {
        let mut req = Request::new(GetUserRedactedRequest {
            user_id: id.to_string(),
        });

        req.metadata_mut().append(
            trace_err!(AsciiMetadataKey::from_bytes("authorization".as_bytes()))?,
            trace_err!(AsciiMetadataValue::try_from(format!(
                "Bearer {}",
                self.long_lived_token
            )))?,
        );

        let user = trace_err!(
            trace_err!(self.user_service.clone().get_user_redacted(req).await)?
                .into_inner()
                .user
                .ok_or(anyhow!("Unknown user"))
        )?;
        Ok(user)
    }
    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_pubkeys(&self) -> Result<Vec<Pubkey>> {
        let mut req = Request::new(GetPubkeysRequest {});

        req.metadata_mut().append(
            trace_err!(AsciiMetadataKey::from_bytes("authorization".as_bytes()))?,
            trace_err!(AsciiMetadataValue::try_from(format!(
                "Bearer {}",
                self.long_lived_token.as_str()
            )))?,
        );

        Ok(
            trace_err!(self.storage_status_service.clone().get_pubkeys(req).await)?
                .into_inner()
                .pubkeys,
        )
    }

    #[tracing::instrument(level = "trace", skip(self, object, token))]
    pub async fn create_project(&self, object: DPObject, token: &str) -> Result<DPObject> {
        let mut req = Request::new(CreateProjectRequest::from(object));

        req.metadata_mut().append(
            trace_err!(AsciiMetadataKey::from_bytes("authorization".as_bytes()))?,
            trace_err!(AsciiMetadataValue::try_from(format!("Bearer {}", token)))?,
        );

        let response = trace_err!(trace_err!(
            self.project_service.clone().create_project(req).await
        )?
        .into_inner()
        .project
        .ok_or(anyhow!("unknown project")))?;

        let object = trace_err!(DPObject::try_from(response))?;

        self.cache.upsert_object(object.clone(), None).await?;

        Ok(object)
    }

    #[tracing::instrument(level = "trace", skip(self, id, _checksum))]
    async fn get_project(&self, id: &DieselUlid, _checksum: String) -> Result<Project> {
        let mut req = Request::new(GetProjectRequest {
            project_id: id.to_string(),
        });

        req.metadata_mut().append(
            trace_err!(AsciiMetadataKey::from_bytes("authorization".as_bytes()))?,
            trace_err!(AsciiMetadataValue::try_from(format!(
                "Bearer {}",
                self.long_lived_token.as_str()
            )))?,
        );

        trace_err!(
            trace_err!(self.project_service.clone().get_project(req).await)?
                .into_inner()
                .project
                .ok_or(anyhow!("unknown project"))
        )
    }

    #[tracing::instrument(level = "trace", skip(self, _checksum))]
    async fn get_collection(&self, id: &DieselUlid, _checksum: String) -> Result<Collection> {
        let mut req = Request::new(GetCollectionRequest {
            collection_id: id.to_string(),
        });

        req.metadata_mut().append(
            trace_err!(AsciiMetadataKey::from_bytes("authorization".as_bytes()))?,
            trace_err!(AsciiMetadataValue::try_from(format!(
                "Bearer {}",
                self.long_lived_token.as_str()
            )))?,
        );

        trace_err!(
            trace_err!(self.collection_service.clone().get_collection(req).await)?
                .into_inner()
                .collection
                .ok_or(anyhow!("unknown collection"))
        )
    }

    #[tracing::instrument(level = "trace", skip(self, object, token))]
    pub async fn create_collection(&self, object: DPObject, token: &str) -> Result<DPObject> {
        let mut req = Request::new(CreateCollectionRequest::from(object));

        req.metadata_mut().append(
            trace_err!(AsciiMetadataKey::from_bytes("authorization".as_bytes()))?,
            trace_err!(AsciiMetadataValue::try_from(format!("Bearer {}", token)))?,
        );

        let response = trace_err!(trace_err!(
            self.collection_service.clone().create_collection(req).await
        )?
        .into_inner()
        .collection
        .ok_or(anyhow!("unknown project")))?;

        let object = trace_err!(DPObject::try_from(response))?;

        self.cache.upsert_object(object.clone(), None).await?;

        Ok(object)
    }

    #[tracing::instrument(level = "trace", skip(self, id, _checksum))]
    async fn get_dataset(&self, id: &DieselUlid, _checksum: String) -> Result<Dataset> {
        let mut req = Request::new(GetDatasetRequest {
            dataset_id: id.to_string(),
        });

        req.metadata_mut().append(
            trace_err!(AsciiMetadataKey::from_bytes("authorization".as_bytes()))?,
            trace_err!(AsciiMetadataValue::try_from(format!(
                "Bearer {}",
                self.long_lived_token.as_str()
            )))?,
        );

        trace_err!(
            trace_err!(self.dataset_service.clone().get_dataset(req).await)?
                .into_inner()
                .dataset
                .ok_or(anyhow!("unknown dataset"))
        )
    }

    #[tracing::instrument(level = "trace", skip(self, object, token))]
    pub async fn create_dataset(&self, object: DPObject, token: &str) -> Result<DPObject> {
        let mut req = Request::new(CreateDatasetRequest::from(object));

        req.metadata_mut().append(
            trace_err!(AsciiMetadataKey::from_bytes("authorization".as_bytes()))?,
            trace_err!(AsciiMetadataValue::try_from(format!("Bearer {}", token)))?,
        );

        let response = trace_err!(trace_err!(
            self.dataset_service.clone().create_dataset(req).await
        )?
        .into_inner()
        .dataset
        .ok_or(anyhow!("unknown project")))?;

        let object = trace_err!(DPObject::try_from(response))?;

        self.cache.upsert_object(object.clone(), None).await?;

        Ok(object)
    }

    #[tracing::instrument(level = "trace", skip(self, _checksum))]
    async fn get_object(&self, id: &DieselUlid, _checksum: String) -> Result<Object> {
        let mut req = Request::new(GetObjectRequest {
            object_id: id.to_string(),
        });

        req.metadata_mut().append(
            trace_err!(AsciiMetadataKey::from_bytes("authorization".as_bytes()))?,
            trace_err!(AsciiMetadataValue::try_from(format!(
                "Bearer {}",
                self.long_lived_token.as_str()
            )))?,
        );

        trace_err!(
            trace_err!(self.object_service.clone().get_object(req).await)?
                .into_inner()
                .object
                .ok_or(anyhow!("unknown object"))
        )
    }

    #[tracing::instrument(level = "trace", skip(self, object, loc, token))]
    pub async fn create_object(
        &self,
        object: DPObject,
        mut loc: Option<ObjectLocation>,
        token: &str,
    ) -> Result<DPObject> {
        let mut req = Request::new(CreateObjectRequest::from(object));

        req.metadata_mut().append(
            trace_err!(AsciiMetadataKey::from_bytes("authorization".as_bytes()))?,
            trace_err!(AsciiMetadataValue::try_from(format!("Bearer {}", token)))?,
        );

        let response = trace_err!(trace_err!(
            self.object_service.clone().create_object(req).await
        )?
        .into_inner()
        .object
        .ok_or(anyhow!("unknown project")))?;

        let object = trace_err!(DPObject::try_from(response))?;

        if let Some(ref mut loc) = loc {
            loc.id = object.id;
        }

        self.cache.upsert_object(object.clone(), loc).await?;
        Ok(object)
    }

    #[tracing::instrument(level = "trace", skip(self, obj, token))]
    pub async fn add_or_replace_key_value_project(
        &self,
        token: &str,
        obj: DPObject,
        key: &str,
        value: &str,
    ) -> Result<()> {
        let remove_cors = obj
            .key_values
            .iter()
            .filter(|e| e.key == "app.aruna-storage.org/cors")
            .cloned()
            .collect();

        let mut req = Request::new(UpdateProjectKeyValuesRequest {
            project_id: obj.id.to_string(),
            add_key_values: vec![KeyValue {
                key: key.to_string(),
                value: value.to_string(),
                variant: KeyValueVariant::Label as i32,
            }],
            remove_key_values: remove_cors,
        });

        req.metadata_mut().append(
            trace_err!(AsciiMetadataKey::from_bytes("authorization".as_bytes()))?,
            trace_err!(AsciiMetadataValue::try_from(format!("Bearer {}", token)))?,
        );

        trace_err!(
            self.project_service
                .clone()
                .update_project_key_values(req)
                .await
        )?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, object, token, force_update))]
    pub async fn init_object_update(
        &self,
        object: DPObject,
        token: &str,
        force_update: bool,
    ) -> Result<DPObject> {
        // Create UpdateObjectRequest with provided value for force_revision parameter
        let mut inner_request = UpdateObjectRequest::from(object);
        inner_request.force_revision = force_update;

        // Crate gRPC request with provided token in header
        let mut req = Request::new(inner_request);

        req.metadata_mut().append(
            trace_err!(AsciiMetadataKey::from_bytes("authorization".as_bytes()))?,
            trace_err!(AsciiMetadataValue::try_from(format!("Bearer {}", token)))?,
        );

        // Update Object in ArunaServer and validate response
        let response =
            trace_err!(self.object_service.clone().update_object(req).await)?.into_inner();

        let object = trace_err!(DPObject::try_from(trace_err!(response
            .object
            .ok_or(anyhow!("response does not contain object")))?,))?;

        self.cache.upsert_object(object.clone(), None).await?;

        Ok(object)
    }

    #[tracing::instrument(level = "trace", skip(self, hashes, location, token))]
    pub async fn finish_object(
        &self,
        object_id: DieselUlid,
        content_len: i64,
        hashes: Vec<Hash>,
        location: Option<ObjectLocation>,
        token: &str,
    ) -> Result<DPObject> {
        let mut req = Request::new(FinishObjectStagingRequest {
            object_id: object_id.to_string(),
            content_len,
            hashes,
            completed_parts: vec![],
        });

        req.metadata_mut().append(
            trace_err!(AsciiMetadataKey::from_bytes("authorization".as_bytes()))?,
            trace_err!(AsciiMetadataValue::try_from(format!("Bearer {}", token)))?,
        );

        let response = trace_err!(trace_err!(
            self.object_service.clone().finish_object_staging(req).await
        )?
        .into_inner()
        .object
        .ok_or(anyhow!("unknown project")))?;

        let object = trace_err!(DPObject::try_from(response))?;

        // Persist Object and Location in cache/database
        if let Some(mut location) = location {
            location.id = object.id;
            self.cache
                .upsert_object(object.clone(), Some(location))
                .await?;
        } else {
            self.cache.upsert_object(object.clone(), None).await?;
        }

        Ok(object)
    }

    #[tracing::instrument(level = "trace", skip(self, proxy_object, loc, token))]
    pub async fn create_and_finish(
        &self,
        proxy_object: DPObject,
        mut loc: ObjectLocation,
        token: &str,
    ) -> Result<DPObject> {
        // Create Object in Aruna Server
        let mut req = Request::new(CreateObjectRequest::from(proxy_object.clone()));

        req.metadata_mut().append(
            trace_err!(AsciiMetadataKey::from_bytes("authorization".as_bytes()))?,
            trace_err!(AsciiMetadataValue::try_from(format!("Bearer {}", token)))?,
        );

        let server_object: DPObject = trace_err!(trace_err!(
            self.object_service.clone().create_object(req).await
        )?
        .into_inner()
        .object
        .ok_or(anyhow!("Object missing in CreateObjectResponse")))?
        .try_into()?;

        let mut req = Request::new(FinishObjectStagingRequest {
            object_id: server_object.id.to_string(),
            content_len: loc.raw_content_len,
            hashes: proxy_object.get_hashes(), // Hashes stay the same
            completed_parts: vec![],
        });

        req.metadata_mut().append(
            trace_err!(AsciiMetadataKey::from_bytes("authorization".as_bytes()))?,
            trace_err!(AsciiMetadataValue::try_from(format!("Bearer {}", token)))?,
        );

        let response = trace_err!(trace_err!(
            self.object_service.clone().finish_object_staging(req).await
        )?
        .into_inner()
        .object
        .ok_or(anyhow!("Object missing in FinishObjectResponse")))?;

        // Id of location record should be set to Dataproxy Object id but is set to Server Object id... the fuck?
        let object = trace_err!(DPObject::try_from(response))?;
        loc.id = object.id;

        // Persist Object and Location in cache/database
        self.cache.upsert_object(object.clone(), Some(loc)).await?;

        Ok(object)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn create_notifications_channel(&self) -> Result<()> {
        let mut req = Request::new(GetEventMessageStreamRequest {
            stream_consumer: self.endpoint_id.to_string(),
        });

        req.metadata_mut().append(
            trace_err!(AsciiMetadataKey::from_bytes("authorization".as_bytes()))?,
            trace_err!(AsciiMetadataValue::try_from(format!(
                "Bearer {}",
                self.long_lived_token.as_str()
            )))?,
        );

        let stream = trace_err!(
            self.event_notification_service
                .clone()
                .get_event_message_stream(req)
                .await
        )?;

        let mut inner_stream = stream.into_inner();

        // Fullsync
        let mut req = Request::new(FullSyncEndpointRequest {});
        req.metadata_mut().append(
            trace_err!(AsciiMetadataKey::from_bytes("authorization".as_bytes()))?,
            trace_err!(AsciiMetadataValue::try_from(format!(
                "Bearer {}",
                self.long_lived_token.as_str()
            )))?,
        );
        let mut full_sync_stream =
            trace_err!(self.endpoint_service.clone().full_sync_endpoint(req).await)?.into_inner();
        let mut resources = Vec::new();
        while let Some(full_sync_message) = trace_err!(full_sync_stream.message().await)? {
            debug!("received full_sync_message");
            trace!(?full_sync_message);
            match trace_err!(full_sync_message
                .target
                .ok_or_else(|| anyhow!("Missing target in full_sync")))?
            {
                Target::GenericResource(GenericResource { resource: Some(r) }) => {
                    resources.push(r);
                }
                Target::User(u) => self.cache.clone().upsert_user(u).await?,
                Target::Pubkey(pk) => {
                    let dec_key = DecodingKey::from_ed_pem(
                        format!(
                            "-----BEGIN PUBLIC KEY-----{}-----END PUBLIC KEY-----",
                            pk.key
                        )
                        .as_bytes(),
                    )?;
                    self.cache
                        .pubkeys
                        .insert(pk.id, (pk.clone().into(), dec_key));
                }
                _ => (),
            }
        }

        sort_resources(&mut resources);
        for res in resources {
            self.cache
                .upsert_object(trace_err!(DPObject::try_from(res))?, None)
                .await?
        }

        let (keep_alive_tx, mut keep_alive_rx) = tokio::sync::mpsc::channel::<()>(1);
        tokio::spawn(
            async move {
                while keep_alive_rx.try_recv().is_ok() {
                    tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                }
                // ABORT!
                error!("keep alive failed");
                panic!("keep alive failed");
            }
            .instrument(tracing::info_span!("keep_alive")),
        );

        debug!("querying events");
        while let Some(m) = inner_stream.message().await? {
            if let Some(message) = m.message {
                debug!(?message, "received event message");

                if let Ok(Some(r)) = self.process_message(message).await {
                    let mut resp =
                        Request::new(AcknowledgeMessageBatchRequest { replies: vec![r] });

                    resp.metadata_mut().append(
                        trace_err!(AsciiMetadataKey::from_bytes("authorization".as_bytes()))?,
                        trace_err!(AsciiMetadataValue::try_from(format!(
                            "Bearer {}",
                            self.long_lived_token.as_str()
                        )))?,
                    );

                    trace_err!(
                        self.event_notification_service
                            .clone()
                            .acknowledge_message_batch(resp)
                            .await
                    )?;
                    debug!("acknowledged message");
                }
            } else {
                let _ = keep_alive_tx.try_send(());
                trace!("received ping");
            }
        }
        error!("Stream was closed by sender");
        Err(anyhow!("Stream was closed by sender"))
    }
}

/// Request handling section
impl GrpcQueryHandler {
    #[tracing::instrument(level = "trace", skip(self, message))]
    async fn process_message(&self, message: EventMessage) -> Result<Option<Reply>> {
        match message.message_variant.unwrap() {
            MessageVariant::ResourceEvent(r_event) => self.process_resource_event(r_event).await,
            MessageVariant::UserEvent(u_event) => self.process_user_event(u_event).await,
            MessageVariant::AnnouncementEvent(a_event) => {
                self.process_announcements_event(a_event).await
            }
        }
    }

    #[tracing::instrument(level = "trace", skip(self, message))]
    async fn process_announcements_event(
        &self,
        message: AnnouncementEvent,
    ) -> Result<Option<Reply>> {
        debug!("processing announcement event");
        match trace_err!(message
            .event_variant
            .ok_or_else(|| anyhow!("No event variant")))?
        {
            announcement_event::EventVariant::NewPubkey(_)
            | announcement_event::EventVariant::RemovePubkey(_)
            | announcement_event::EventVariant::NewDataProxyId(_)
            | announcement_event::EventVariant::RemoveDataProxyId(_)
            | announcement_event::EventVariant::UpdateDataProxyId(_) => {
                let pks = trace_err!(self.get_pubkeys().await)?
                    .into_iter()
                    .map(PubKey::from)
                    .collect();
                trace_err!(self.cache.set_pubkeys(pks).await)?;
            }
            announcement_event::EventVariant::Downtime(_) => (),
            announcement_event::EventVariant::Version(_) => (),
        };
        Ok(message.reply)
    }

    #[tracing::instrument(level = "trace", skip(self, message))]
    async fn process_user_event(&self, message: UserEvent) -> Result<Option<Reply>> {
        debug!("processing user event");
        match message.event_variant() {
            EventVariant::Created | EventVariant::Available | EventVariant::Updated => {
                let uid = DieselUlid::from_str(&message.user_id)?;
                let user_info = self.get_user(uid, message.checksum.clone()).await?;
                self.cache.clone().upsert_user(user_info.clone()).await?;
            }
            EventVariant::Deleted => {
                let uid = DieselUlid::from_str(&message.user_id)?;

                self.cache.remove_user(uid).await?;
            }
            _ => (),
        }

        Ok(message.reply)
    }

    #[tracing::instrument(level = "trace", skip(self, event))]
    async fn process_resource_event(&self, event: ResourceEvent) -> Result<Option<Reply>> {
        debug!("processing resource event");
        match event.event_variant() {
            EventVariant::Created | EventVariant::Updated => {
                trace!("upserting object");
                if let Some(r) = event.resource {
                    match r.resource_variant() {
                        aruna_rust_api::api::storage::models::v2::ResourceVariant::Project => {
                            let object = trace_err!(
                                self.get_project(
                                    &trace_err!(DieselUlid::from_str(&r.resource_id))?,
                                    r.checksum
                                )
                                .await
                            )?;
                            self.cache.upsert_object(object.try_into()?, None).await?;
                        }
                        aruna_rust_api::api::storage::models::v2::ResourceVariant::Collection => {
                            let object = trace_err!(
                                self.get_collection(
                                    &trace_err!(DieselUlid::from_str(&r.resource_id))?,
                                    r.checksum
                                )
                                .await
                            )?;
                            self.cache.upsert_object(object.try_into()?, None).await?;
                        }
                        aruna_rust_api::api::storage::models::v2::ResourceVariant::Dataset => {
                            let object = trace_err!(
                                self.get_dataset(
                                    &trace_err!(DieselUlid::from_str(&r.resource_id))?,
                                    r.checksum
                                )
                                .await
                            )?;
                            self.cache.upsert_object(object.try_into()?, None).await?;
                        }
                        aruna_rust_api::api::storage::models::v2::ResourceVariant::Object => {
                            let object = trace_err!(
                                self.get_object(
                                    &trace_err!(DieselUlid::from_str(&r.resource_id))?,
                                    r.checksum
                                )
                                .await
                            )?;
                            self.cache.upsert_object(object.try_into()?, None).await?;
                        }
                        _ => (),
                    }
                }
            }
            EventVariant::Deleted => {
                trace!("deleting object");
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

#[tracing::instrument(level = "trace", skip(res))]
pub fn sort_resources(res: &mut [Resource]) {
    res.sort_by(|x, y| match (x, y) {
        (Resource::Project(_), Resource::Project(_)) => std::cmp::Ordering::Equal,
        (Resource::Project(_), Resource::Collection(_))
        | (Resource::Project(_), Resource::Dataset(_))
        | (Resource::Project(_), Resource::Object(_)) => std::cmp::Ordering::Less,
        (Resource::Collection(_), Resource::Project(_)) => std::cmp::Ordering::Greater,
        (Resource::Collection(_), Resource::Collection(_)) => std::cmp::Ordering::Equal,
        (Resource::Collection(_), Resource::Dataset(_)) => std::cmp::Ordering::Less,
        (Resource::Collection(_), Resource::Object(_)) => std::cmp::Ordering::Less,
        (Resource::Dataset(_), Resource::Project(_)) => std::cmp::Ordering::Greater,
        (Resource::Dataset(_), Resource::Collection(_)) => std::cmp::Ordering::Greater,
        (Resource::Dataset(_), Resource::Dataset(_)) => std::cmp::Ordering::Equal,
        (Resource::Dataset(_), Resource::Object(_)) => std::cmp::Ordering::Less,
        (Resource::Object(_), Resource::Project(_)) => std::cmp::Ordering::Greater,
        (Resource::Object(_), Resource::Collection(_)) => std::cmp::Ordering::Greater,
        (Resource::Object(_), Resource::Dataset(_)) => std::cmp::Ordering::Greater,
        (Resource::Object(_), Resource::Object(_)) => std::cmp::Ordering::Equal,
    })
}

#[tracing::instrument(level = "trace", skip(res))]
pub fn sort_objects(res: &mut [DPObject]) {
    res.sort_by(|x, y| match (&x.object_type, &y.object_type) {
        (ObjectType::Bundle, ObjectType::Bundle) => std::cmp::Ordering::Equal,
        (ObjectType::Bundle, ObjectType::Project)
        | (ObjectType::Bundle, ObjectType::Collection)
        | (ObjectType::Bundle, ObjectType::Dataset)
        | (ObjectType::Bundle, ObjectType::Object) => std::cmp::Ordering::Less,

        (ObjectType::Project, ObjectType::Bundle) => std::cmp::Ordering::Greater,
        (ObjectType::Project, ObjectType::Project) => std::cmp::Ordering::Equal,
        (ObjectType::Project, ObjectType::Collection)
        | (ObjectType::Project, ObjectType::Dataset)
        | (ObjectType::Project, ObjectType::Object) => std::cmp::Ordering::Less,

        (ObjectType::Collection, ObjectType::Bundle)
        | (ObjectType::Collection, ObjectType::Project) => std::cmp::Ordering::Greater,
        (ObjectType::Collection, ObjectType::Collection) => std::cmp::Ordering::Equal,
        (ObjectType::Collection, ObjectType::Dataset)
        | (ObjectType::Collection, ObjectType::Object) => std::cmp::Ordering::Less,

        (ObjectType::Dataset, ObjectType::Bundle)
        | (ObjectType::Dataset, ObjectType::Project)
        | (ObjectType::Dataset, ObjectType::Collection) => std::cmp::Ordering::Greater,
        (ObjectType::Dataset, ObjectType::Dataset) => std::cmp::Ordering::Equal,
        (ObjectType::Dataset, ObjectType::Object) => std::cmp::Ordering::Less,

        (ObjectType::Object, ObjectType::Bundle)
        | (ObjectType::Object, ObjectType::Project)
        | (ObjectType::Object, ObjectType::Collection)
        | (ObjectType::Object, ObjectType::Dataset) => std::cmp::Ordering::Greater,
        (ObjectType::Object, ObjectType::Object) => std::cmp::Ordering::Equal,
    })
}
