use crate::{
    caching::{auth::get_token_from_md, cache::Cache},
    data_backends::storage_backend::StorageBackend,
    helpers::sign_download_url,
    replication::replication_handler::ReplicationMessage,
    structs::{
        Endpoint, Object, ObjectLocation, ObjectType, Origin, TypedRelation, ALL_RIGHTS_RESERVED,
    },
    trace_err,
};
use anyhow::{anyhow, Result};
use aruna_file::{
    helpers::footer_parser::FooterParser, streamreadwrite::ArunaStreamReadWriter,
    transformer::ReadWriter,
};
use aruna_rust_api::api::{
    dataproxy::services::v2::{
        dataproxy_replication_service_server::DataproxyReplicationService,
        pull_replication_request::Message, pull_replication_response, DataInfo, DataInfos,
        InitMessage, PullReplicationRequest, PullReplicationResponse, PushReplicationRequest,
        PushReplicationResponse,
    },
    storage::models::v2::{DataClass, Status},
};
use async_channel::Sender;
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use std::collections::{HashMap, HashSet};
use std::{str::FromStr, sync::Arc};
use tokio::pin;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Streaming;
use tracing::{error, trace};

pub struct DataproxyReplicationServiceImpl {
    pub cache: Arc<Cache>,
    pub sender: Sender<ReplicationMessage>,
    pub endpoint_url: String,
    pub backend: Arc<Box<dyn StorageBackend>>,
}

impl DataproxyReplicationServiceImpl {
    #[tracing::instrument(level = "trace", skip(cache))]
    pub fn new(
        cache: Arc<Cache>,
        sender: Sender<ReplicationMessage>,
        endpoint_url: String,
        backend: Arc<Box<dyn StorageBackend>>,
    ) -> Self {
        Self {
            cache,
            sender,
            endpoint_url,
            backend,
        }
    }
}

#[derive(PartialEq, Eq, Hash)]
enum AckSync {
    ObjectInit(DieselUlid),
    ObjectChunk(DieselUlid, i64, i64), // Object id, completed_chunks, chunks_idx
    Finish,
}

#[tonic::async_trait]
impl DataproxyReplicationService for DataproxyReplicationServiceImpl {
    type PullReplicationStream = ReceiverStream<Result<PullReplicationResponse, tonic::Status>>;
    /// PullReplication
    ///
    /// Status: BETA
    ///
    /// Creates a replication request
    #[tracing::instrument(level = "trace", skip(self, request))]
    async fn pull_replication(
        &self,
        request: tonic::Request<Streaming<PullReplicationRequest>>,
    ) -> Result<tonic::Response<Self::PullReplicationStream>, tonic::Status> {
        let (metadata, _, request) = request.into_parts();
        let token = trace_err!(get_token_from_md(&metadata))
            .map_err(|_| tonic::Status::unauthenticated("Token not found"))?;

        let (object_input_send, object_input_rcv) = async_channel::bounded(1);
        let (object_ack_send, object_ack_rcv) = async_channel::bounded(100);
        let (object_output_send, object_output_rcv) = tokio::sync::mpsc::channel(100);

        // Recieving loop
        tokio::spawn(async move {
            while let Ok(message) = request.message().await {
                match message {
                    Some(message) => match message {
                        PullReplicationRequest { message } => match message {
                            Some(message) => match message {
                                Message::InitMessage(init) => {
                                    object_input_send
                                        .send(self.check_permissions(init, token).await);
                                }
                                Message::InfoAckMessage(_) => todo!(),
                                Message::ChunkAckMessage(_) => todo!(),
                                Message::ErrorMessage(_) => todo!(),
                                Message::FinishMessage(_) => todo!(),
                            },
                            _ => todo!(),
                        },
                        _ => todo!(),
                    },
                    None => todo!(),
                };
            }
        });

        // Ack sync loop
        tokio::spawn(async move {
            let mut sync_map: HashSet<AckSync> = HashSet::new();
            while let Ok(ack_msg) = object_ack_rcv.recv().await {
                match ack_msg {
                    init @ AckSync::ObjectInit(_) => {
                        sync_map.insert(init);
                    }
                    chunk @ AckSync::ObjectChunk(id, chunk_id, max_chunk) => {
                        // If completed chunk before found
                        if sync_map
                            .take(&AckSync::ObjectChunk(id, chunk_id - 1, max_chunk))
                            .is_some()
                        {
                            // ... insert ack with ObjectInit
                            sync_map.insert(chunk);
                        }
                        // If None chunk and None ObjectInit found
                        else if let None = sync_map.take(&AckSync::ObjectInit(id)) {
                            // ... send Err
                            object_output_send
                                .send(Err(tonic::Status::not_found(
                                    "Object not found in InitMessage",
                                )))
                                .await;
                        }
                        // If None found, but ObjectInit found
                        else {
                            // ... insert ack with ObjectChunk
                            sync_map.insert(chunk);
                        };
                    }
                    AckSync::Finish => {
                        // Look if any object is not finished
                        if sync_map.iter().all(|ack| match ack {
                            AckSync::ObjectInit(_) => false,
                            AckSync::ObjectChunk(id, completed, max) => completed == max,
                            AckSync::Finish => false,
                        }) {
                            object_output_send
                                .send(Ok(PullReplicationResponse {
                                    message: Some(
                                        pull_replication_response::Message::FinishMessage(
                                            aruna_rust_api::api::dataproxy::services::v2::Empty {},
                                        ),
                                    ),
                                }))
                                .await;
                        } else {
                            object_output_send
                                .send(Err(tonic::Status::data_loss(
                                    "Not all chunks were acknowledged",
                                )))
                                .await;
                        }
                    }
                }
            }
        });

        // Responding loop
        tokio::spawn(async move {
            'outer: while let replication_message = object_input_rcv.recv().await {
                match replication_message {
                    // Errors
                    Err(err) => {
                        error!("{err}");
                        object_output_send
                            .send(Err(tonic::Status::unauthenticated(
                                "Unauthorized to pull objects",
                            )))
                            .await;
                    }
                    Ok(Err(err)) => {
                        error!("{err}");
                        object_output_send
                            .send(Err(tonic::Status::unauthenticated(
                                "Unauthorized to pull objects",
                            )))
                            .await;
                    }

                    // Objects
                    Ok(Ok(objects)) => {
                        for (object, location) in objects {
                            // Get footer
                            let footer = match self.get_footer(location).await {
                                Ok(footer) => footer,
                                Err(err) => {
                                    error!("{err}");
                                    object_output_send
                                        .send(Err(tonic::Status::internal(
                                            "Could not parse footer",
                                        )))
                                        .await;
                                    break 'outer;
                                }
                            };
                            let blocklist = footer.map(|f| f.get_blocklist());
                            let max_blocks = blocklist.map(|l| l.len()).unwrap_or(1);
                            object_output_send
                                .send(Ok(PullReplicationResponse {
                                    message: Some(pull_replication_response::Message::ObjectInfo(
                                        aruna_rust_api::api::dataproxy::services::v2::ObjectInfo {
                                            object_id: object.id.to_string(),
                                            chunks: max_blocks as i64,
                                            extra: None,
                                        },
                                    )),
                                }))
                                .await;
                            // Send chunks

                            // Send all objects into acknowledgement sync handler
                            object_ack_send.send(AckSync::ObjectInit(object.id)).await;
                            todo!()
                        }
                    }
                };
            }
        });

        let grpc_response: tonic::Response<
            ReceiverStream<std::result::Result<PullReplicationResponse, tonic::Status>>,
        > = tonic::Response::new(ReceiverStream::new(object_output_rcv));
        Ok(grpc_response)
    }

    /// PushReplication
    ///
    /// Status: BETA
    ///
    /// Provides the necessary url to init replication
    #[tracing::instrument(level = "trace", skip(self, _request))]
    async fn push_replication(
        &self,
        _request: tonic::Request<PushReplicationRequest>,
    ) -> Result<tonic::Response<PushReplicationResponse>, tonic::Status> {
        // TODO
        // 1. query permissions
        // 2. validate endpoint that tries sending these
        // 3. validate if i need these objects
        // 4. send message to replication handler with DataInfos
        error!("InitReplication not implemented");
        Err(tonic::Status::unimplemented("Currently not implemented"))
    }
}

impl DataproxyReplicationServiceImpl {
    async fn check_permissions(
        &self,
        init: InitMessage,
        token: String,
    ) -> Result<Vec<(Object, ObjectLocation)>, tonic::Status> {
        let ids = trace_err!(init
            .object_ids
            .iter()
            .map(|id| DieselUlid::from_str(id).map_err(|e| {
                trace!("{e}: Invalid id");
                tonic::Status::invalid_argument("Invalid id provided")
            }))
            .collect::<tonic::Result<Vec<DieselUlid>, tonic::Status>>())?;

        // 1. get all objects & endpoints from server
        let mut objects = Vec::new();
        let object_endpoint_map = DashMap::new();
        for id in ids {
            if let Some(o) = self.cache.resources.get(&id) {
                let (_, (object, location)) = o.pair();
                let location = location
                    .as_ref()
                    .ok_or_else(|| tonic::Status::not_found("No location found for object"))?;
                objects.push((object.clone(), location.clone()));
                object_endpoint_map.insert(object.id, object.endpoints.clone());
            }
        }
        trace!("EndpointMap: {:?}", object_endpoint_map);

        // 2. check if proxy has permissions to pull everything
        if let Some(auth) = self.cache.auth.read().await.as_ref() {
            // Returns claims.sub as id -> Can return UserIds or DataproxyIds
            // -> UserIds cannot be found in object.endpoints, so this should be safe
            let (dataproxy_id, _) = trace_err!(auth.check_permissions(&token))
                .map_err(|_| tonic::Status::unauthenticated("DataProxy not authenticated"))?;
            if !object_endpoint_map.iter().all(|map| {
                let (_, eps) = map.pair();
                eps.iter().find(|ep| ep.id == dataproxy_id).is_some()
            }) {
                error!("Unauthorized DataProxy request");
                return Err(tonic::Status::unauthenticated(
                    "DataProxy is not allowed to access requested objects",
                ));
            };
        } else {
            error!("authentication handler not available");
            return Err(tonic::Status::unauthenticated(
                "Unable to authenticate user",
            ));
        };
        Ok(objects)
    }

    async fn get_footer(&self, location: ObjectLocation) -> anyhow::Result<Option<FooterParser>> {
        let content_length = location.raw_content_len;
        let encryption_key = location
            .clone()
            .encryption_key
            .map(|k| k.as_bytes().to_vec());

        // Gets 128 kb chunks (last 2)
        let footer_parser: Option<FooterParser> = if content_length > 5242880 {
            trace!("getting footer");
            // Without encryption block because this is already checked inside
            let (footer_sender, footer_receiver) = async_channel::unbounded();
            pin!(footer_receiver);

            let parser = match encryption_key.clone() {
                Some(key) => {
                    trace_err!(
                        self.backend
                            .get_object(
                                location.clone(),
                                Some(format!("bytes=-{}", (65536 + 28) * 2)),
                                // "-" means last (2) chunks
                                // when encrypted + 28 for encryption information (16 bytes nonce, 12 bytes checksum)
                                // Encrypted chunk = | 16 b nonce | 65536 b data | 12 b checksum |
                                footer_sender,
                            )
                            .await
                    )
                    .map_err(|_| tonic::Status::internal("Unable to get encryption_footer"));
                    let mut output = Vec::with_capacity(130_000);
                    // Stream takes receiver chunks und them into vec
                    let mut arsw =
                        ArunaStreamReadWriter::new_with_writer(footer_receiver, &mut output);

                    // processes chunks and puts them into output
                    trace_err!(arsw.process().await)
                        .map_err(|_| anyhow!("Unable to get footer"))?;
                    drop(arsw);

                    match output.try_into() {
                        Ok(i) => match FooterParser::from_encrypted(&i, &key) {
                            Ok(p) => Some(p),
                            Err(e) => {
                                error!(?e);
                                return Err(anyhow!("Could not parse footer from encrypted"));
                            }
                        },
                        Err(e) => {
                            error!(?e);
                            return Err(anyhow!("Could not parse footer from encrypted"));
                        }
                    }
                }
                None => {
                    trace_err!(
                        self.backend
                            .get_object(
                                location.clone(),
                                Some(format!("bytes=-{}", 65536 * 2)),
                                // when not encrypted without 28
                                footer_sender,
                            )
                            .await
                    )
                    .map_err(|_| anyhow!("Unable to get compression footer"))?;
                    let mut output = Vec::with_capacity(130_000);
                    let mut arsw =
                        ArunaStreamReadWriter::new_with_writer(footer_receiver, &mut output);

                    trace_err!(arsw.process().await)
                        .map_err(|_| anyhow!("Unable to get footer"))?;
                    drop(arsw);

                    match output.try_into() {
                        Ok(i) => Some(FooterParser::new(&i)),
                        Err(_) => {
                            return Err(anyhow!("Could not parse footer from unencrypted"));
                        }
                    }
                }
            };
            parser
        } else {
            None
        };
        Ok(footer_parser)
    }

    async fn send_object(
        &self,
        location: ObjectLocation,
        blocklist: Vec<u8>,
        sender: tokio::sync::mpsc::Sender<Result<PushReplicationResponse, tonic::Status>>,
    ) -> anyhow::Result<()> {
        todo!()
    }
}
