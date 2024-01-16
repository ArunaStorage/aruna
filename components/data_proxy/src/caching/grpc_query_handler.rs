use crate::replication::replication_handler::Direction;
use crate::replication::replication_handler::ReplicationMessage;
use crate::structs::Object as DPObject;
use crate::structs::ObjectLocation;
use crate::structs::ObjectType;
use crate::structs::PubKey;
use crate::trace_err;
use anyhow::anyhow;
use anyhow::Result;
use aruna_rust_api::api::dataproxy::services::v2::dataproxy_replication_service_client::DataproxyReplicationServiceClient;
use aruna_rust_api::api::dataproxy::services::v2::PullReplicationRequest;
use aruna_rust_api::api::dataproxy::services::v2::PullReplicationResponse;
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
use aruna_rust_api::api::storage::models::v2::data_endpoint::Variant;
use aruna_rust_api::api::storage::models::v2::generic_resource::Resource;
use aruna_rust_api::api::storage::models::v2::Collection;
use aruna_rust_api::api::storage::models::v2::Dataset;
use aruna_rust_api::api::storage::models::v2::EndpointHostVariant;
use aruna_rust_api::api::storage::models::v2::FullSync;
use aruna_rust_api::api::storage::models::v2::GenericResource;
use aruna_rust_api::api::storage::models::v2::Hash;
use aruna_rust_api::api::storage::models::v2::KeyValue;
use aruna_rust_api::api::storage::models::v2::KeyValueVariant;
use aruna_rust_api::api::storage::models::v2::Object;
use aruna_rust_api::api::storage::models::v2::Project;
use aruna_rust_api::api::storage::models::v2::Pubkey;
use aruna_rust_api::api::storage::models::v2::ReplicationStatus;
use aruna_rust_api::api::storage::models::v2::User as GrpcUser;
use aruna_rust_api::api::storage::services::v2::data_replication_service_client::DataReplicationServiceClient;
use aruna_rust_api::api::storage::services::v2::full_sync_endpoint_response::Target;
use aruna_rust_api::api::storage::services::v2::get_endpoint_request::Endpoint;
use aruna_rust_api::api::storage::services::v2::CreateCollectionRequest;
use aruna_rust_api::api::storage::services::v2::CreateDatasetRequest;
use aruna_rust_api::api::storage::services::v2::CreateObjectRequest;
use aruna_rust_api::api::storage::services::v2::CreateProjectRequest;
use aruna_rust_api::api::storage::services::v2::FinishObjectStagingRequest;
use aruna_rust_api::api::storage::services::v2::FullSyncEndpointRequest;
use aruna_rust_api::api::storage::services::v2::GetCollectionRequest;
use aruna_rust_api::api::storage::services::v2::GetDatasetRequest;
use aruna_rust_api::api::storage::services::v2::GetEndpointRequest;
use aruna_rust_api::api::storage::services::v2::GetObjectRequest;
use aruna_rust_api::api::storage::services::v2::GetProjectRequest;
use aruna_rust_api::api::storage::services::v2::GetPubkeysRequest;
use aruna_rust_api::api::storage::services::v2::GetUserRedactedRequest;
use aruna_rust_api::api::storage::services::v2::UpdateObjectRequest;
use aruna_rust_api::api::storage::services::v2::UpdateProjectKeyValuesRequest;
use aruna_rust_api::api::storage::services::v2::UpdateReplicationStatusRequest;
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
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::AsciiMetadataKey;
use tonic::metadata::AsciiMetadataValue;
use tonic::transport::Channel;
use tonic::transport::ClientTlsConfig;
use tonic::Request;
use tonic::Streaming;
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
    data_replication_service: DataReplicationServiceClient<Channel>,
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

        let event_notification_service = EventNotificationServiceClient::new(channel.clone());

        let data_replication_service = DataReplicationServiceClient::new(channel.clone());

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
            data_replication_service,
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

        req.get_mut().preferred_endpoint = self.endpoint_id.clone();
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
        kv: Option<(&str, &str)>,
    ) -> Result<()> {
        let remove_cors = obj
            .key_values
            .iter()
            .filter(|e| e.key == "app.aruna-storage.org/cors")
            .cloned()
            .collect();

        let mut req = Request::new(UpdateProjectKeyValuesRequest {
            project_id: obj.id.to_string(),
            add_key_values: kv
                .map(|(k, v)| {
                    vec![KeyValue {
                        key: k.to_string(),
                        value: v.to_string(),
                        variant: KeyValueVariant::Label as i32,
                    }]
                })
                .unwrap_or_default(),
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
            let object = trace_err!(DPObject::try_from(res))?;
            self.cache.upsert_object(object, None).await?
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

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn pull_replication(
        &self,
        init_request: PullReplicationRequest,
        endpoint_ulid: DieselUlid,
    ) -> Result<(
        Sender<PullReplicationRequest>,
        Streaming<PullReplicationResponse>,
    )> {
        let get_ep_request = Request::new(GetEndpointRequest {
            endpoint: Some(Endpoint::EndpointId(endpoint_ulid.to_string())),
        });
        let get_ep_response = trace_err!(
            self.endpoint_service
                .clone()
                .get_endpoint(get_ep_request)
                .await
        )?
        .into_inner();
        let endpoint = trace_err!(get_ep_response
            .endpoint
            .ok_or_else(|| anyhow!("No endpoint found in GetEndpointResponse")))?;
        let config = trace_err!(endpoint
            .host_configs
            .iter()
            .find(|config| config.host_variant() == EndpointHostVariant::Grpc)
            .ok_or_else(|| anyhow!("No grpc config found for endpoint")))?;
        let channel = if config.ssl {
            let proxy_channel = trace_err!(Channel::from_shared(config.url.clone()))?;
            let tls_config = ClientTlsConfig::new();
            trace_err!(
                trace_err!(proxy_channel.tls_config(tls_config))?
                    .connect()
                    .await
            )?
        } else {
            trace_err!(Channel::from_shared(config.url.clone())?.connect().await)?
        };
        let token = if let Some(auth) = self.cache.auth.read().await.as_ref() {
            trace_err!(auth.sign_dataproxy_token(endpoint_ulid))?
        } else {
            trace_err!(Err(anyhow!("Cannot read auth handler")))?
        };

        let dataproxy_service = DataproxyReplicationServiceClient::new(channel.clone())
            .max_decoding_message_size(1024 * 1024 * 10);
        let (request_stream_sender, request_stream_receiver) = tokio::sync::mpsc::channel(1000);
        let mut req = Request::new(ReceiverStream::new(request_stream_receiver));
        req.metadata_mut().append(
            trace_err!(AsciiMetadataKey::from_bytes("authorization".as_bytes()))?,
            trace_err!(AsciiMetadataValue::try_from(format!(
                "Bearer {}",
                token.as_str()
            )))?,
        );
        let response_stream =
            trace_err!(dataproxy_service.clone().pull_replication(req).await)?.into_inner();
        trace_err!(request_stream_sender.send(init_request).await)?;
        Ok((request_stream_sender, response_stream))
    }

    #[tracing::instrument(level = "trace", skip(self, request))]
    pub async fn update_replication_status(
        &self,
        request: UpdateReplicationStatusRequest,
    ) -> Result<()> {
        let mut request = Request::new(request);
        request.metadata_mut().append(
            trace_err!(AsciiMetadataKey::from_bytes("authorization".as_bytes()))?,
            trace_err!(AsciiMetadataValue::try_from(format!(
                "Bearer {}",
                self.long_lived_token.as_str()
            )))?,
        );
        trace_err!(
            self.data_replication_service
                .clone()
                .update_replication_status(request)
                .await
        )?;
        Ok(())
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
                            // Update anyway
                            trace_err!(
                                self.cache
                                    .upsert_object(object.clone().try_into()?, None)
                                    .await
                            )?;
                            // Try pull replication
                            trace_err!(self.handle_replication(object).await)?;
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

    #[tracing::instrument(level = "trace", skip(self, object))]
    async fn handle_replication(&self, object: Object) -> Result<()> {
        // if ObjectStatus::AVAILABLE ...
        if object.status == 3 {
            // ... object should be synced in at least one ep
            for ep in &object.endpoints {
                // ... then find out if I have to do anything ...
                match (ep.status(), &ep.id, &ep.variant) {
                    // ... if my id, waiting and FullSync -> I should request a FullSync
                    (
                        ReplicationStatus::Waiting,
                        id,
                        Some(Variant::FullSync(FullSync { project_id: wanted })),
                    ) if id == &self.endpoint_id => {
                        // Find a proxy that has a fullsync
                        let full_sync_proxy = &object.endpoints.iter().find_map(|ep| {
                            match (&ep.variant, ep.status()) {
                                (
                                    Some(Variant::FullSync(FullSync { project_id })),
                                    ReplicationStatus::Finished,
                                ) if wanted == project_id => Some(ep.id.clone()),
                                _ => None,
                            }
                        });
                        match full_sync_proxy {
                            Some(ep_id) => {
                                let direction =
                                    Direction::Pull(trace_err!(DieselUlid::from_str(&object.id))?);
                                let endpoint_id = trace_err!(DieselUlid::from_str(ep_id))?;
                                trace_err!(
                                    self.cache
                                        .sender
                                        .send(ReplicationMessage {
                                            direction,
                                            endpoint_id,
                                        })
                                        .await
                                )?;
                            }
                            None => {
                                error!("ReplicationError: No available proxy found");
                                trace_err!(
                                    self.update_replication_status(
                                        UpdateReplicationStatusRequest {
                                            object_id: object.id.to_string(),
                                            endpoint_id: self.endpoint_id.clone(),
                                            status: ReplicationStatus::Error as i32,
                                        }
                                    )
                                    .await
                                )?;
                            }
                        }
                    }
                    // ... if my id, waiting and partial sync -> I should request a PartialSync
                    (ReplicationStatus::Waiting, id, Some(Variant::PartialSync(_)))
                        if id == &self.endpoint_id =>
                    {
                        // Find the full sync proxy and partial sync from there
                        let full_sync_proxy = &object.endpoints.iter().find_map(|ep| {
                            match (&ep.variant, ep.status()) {
                                (Some(Variant::FullSync(_)), ReplicationStatus::Finished) => {
                                    Some(ep.id.clone())
                                }
                                _ => None,
                            }
                        });
                        match full_sync_proxy {
                            Some(ep_id) => {
                                let direction =
                                    Direction::Pull(trace_err!(DieselUlid::from_str(&object.id))?);
                                let endpoint_id = trace_err!(DieselUlid::from_str(ep_id))?;
                                trace_err!(
                                    self.cache
                                        .sender
                                        .send(ReplicationMessage {
                                            direction,
                                            endpoint_id,
                                        })
                                        .await
                                )?;
                            }
                            None => {
                                error!("ReplicationError: No available proxy found");
                                trace_err!(
                                    self.update_replication_status(
                                        UpdateReplicationStatusRequest {
                                            object_id: object.id.to_string(),
                                            endpoint_id: self.endpoint_id.clone(),
                                            status: ReplicationStatus::Error as i32,
                                        }
                                    )
                                    .await
                                )?;
                            }
                        }
                    }
                    // ... if others are waiting, and I finished -> Should I sync to other dataproxies?
                    (ReplicationStatus::Waiting, id, _) if id != &self.endpoint_id => {
                        // TODO
                        // - How to find out if pushing is appropriate for a given dataproxy that is
                        //   not this one? -> Am I the only one/main dataproxy?
                        // - Check if object location exists here
                        // - Create presigned url for object
                        // - send message to replication handler
                    }
                    _ => (),
                }
            }
        }
        Ok(())
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
