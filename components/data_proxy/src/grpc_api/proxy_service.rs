use crate::{
    caching::{auth::get_token_from_md, cache::Cache},
    data_backends::storage_backend::StorageBackend,
    replication::replication_handler::ReplicationMessage,
    s3_frontend::utils::replication_sink::ReplicationSink,
    structs::{Object, ObjectLocation},
    trace_err,
};
use anyhow::{anyhow, Result};
use aruna_file::{
    helpers::footer_parser::FooterParser, streamreadwrite::ArunaStreamReadWriter,
    transformer::ReadWriter, transformers::decrypt::ChaCha20Dec,
};

use aruna_rust_api::api::dataproxy::services::v2::{
    dataproxy_replication_service_server::DataproxyReplicationService, error_message::Error,
    ErrorMessage, RetryChunkMessage,
};
use aruna_rust_api::api::dataproxy::services::v2::{
    pull_replication_request::Message, pull_replication_response, ChunkAckMessage, InfoAckMessage,
    InitMessage, PullReplicationRequest, PullReplicationResponse, PushReplicationRequest,
    PushReplicationResponse,
};
use async_channel::{Receiver, Sender};
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use std::collections::{HashMap, HashSet};
use std::{str::FromStr, sync::Arc};
use tokio::pin;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Streaming;
use tracing::{error, info_span, trace, Instrument};

#[derive(Clone)]
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

#[derive(PartialEq, Eq, Hash, Clone)]
enum AckSync {
    ObjectInit(DieselUlid),
    ObjectChunk(DieselUlid, i64), // Object id, completed_chunks, chunks_idx
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
        trace!("Received request: {request:?}");
        let (metadata, _, mut request) = request.into_parts();
        let token = trace_err!(get_token_from_md(&metadata))
            .map_err(|_| tonic::Status::unauthenticated("Token not found"))?;

        // Sends initial Vec<(object, location)> to sync/ack/stream handlers
        let (object_input_send, object_input_rcv) = async_channel::bounded(5);
        // Sends ack messages to the ack handler
        let (object_ack_send, object_ack_rcv) = async_channel::bounded(100);
        // Sends the finished ack hash map to the output handler to compare send/ack
        let (object_sync_send, object_sync_rcv) = async_channel::bounded(1);
        // Handles output stream
        let (object_output_send, object_output_rcv) = tokio::sync::mpsc::channel(255);
        // Error and retry handling for ArunaStreamReadWriter
        let (retry_send, retry_rcv) = async_channel::bounded(1);

        // Recieving loop
        let proxy_replication_service = self.clone();
        let output_sender = object_output_send.clone();
        tokio::spawn(async move {
            while let Ok(message) = request.message().await {
                trace!(?message);
                match message {
                    Some(message) => {
                        let PullReplicationRequest { message } = message;
                        match message {
                            Some(message) => match message {
                                Message::InitMessage(init) => {
                                    trace!(?init);
                                    let msg = trace_err!(
                                        proxy_replication_service
                                            .check_permissions(init, token.clone())
                                            .await
                                    );
                                    trace_err!(object_input_send.send(msg).await)?;
                                }
                                Message::InfoAckMessage(InfoAckMessage { object_id }) => {
                                    let object_id = DieselUlid::from_str(&object_id)?;
                                    // Send object init into acknowledgement sync handler
                                    trace_err!(
                                        object_ack_send.send(AckSync::ObjectInit(object_id)).await
                                    )?;
                                }
                                Message::ChunkAckMessage(ChunkAckMessage {
                                    object_id,
                                    chunk_idx,
                                }) => {
                                    let object_id = DieselUlid::from_str(&object_id)?;
                                    // Send object init into acknowledgement sync handler
                                    trace_err!(
                                        object_ack_send
                                            .send(AckSync::ObjectChunk(object_id, chunk_idx))
                                            .await
                                    )?;
                                    trace_err!(retry_send.send(None).await)?;
                                }
                                Message::ErrorMessage(ErrorMessage { error }) => {
                                    if let Some(err) = error {
                                        match err {
                                            Error::RetryChunk(RetryChunkMessage {
                                                object_id,
                                                chunk_idx,
                                            }) => {
                                                let msg = Some((chunk_idx, object_id));
                                                trace_err!(retry_send.send(msg).await)?;
                                            }
                                            Error::Abort(_) => return Err(anyhow!("Aborted sync")),
                                            Error::RetryObjectId(object_id) => {
                                                let ulid =
                                                    trace_err!(DieselUlid::from_str(&object_id))?;
                                                let (object, location) =
                                                    trace_err!(proxy_replication_service
                                                        .cache
                                                        .get_resource(&ulid))?;
                                                // TODO:
                                                // - Check permissions for dataproxy
                                                if let Some(auth) = proxy_replication_service
                                                    .cache
                                                    .auth
                                                    .read()
                                                    .await
                                                    .as_ref()
                                                {
                                                    // Returns claims.sub as id -> Can return UserIds or DataproxyIds
                                                    // -> UserIds cannot be found in object.endpoints, so this should be safe
                                                    let (dataproxy_id, _) =
                                                        trace_err!(auth.check_permissions(&token))
                                                            .map_err(|_| {
                                                                tonic::Status::unauthenticated(
                                                                    "DataProxy not authenticated",
                                                                )
                                                            })?;
                                                    if object
                                                        .endpoints
                                                        .iter()
                                                        .find(|ep| ep.id == dataproxy_id)
                                                        .is_none()
                                                    {
                                                        trace!("Endpoint has no permission to replicate object");
                                                        trace_err!(
                                                            output_sender
                                                                .send(Err(
                                                                    tonic::Status::unauthenticated(
                                                                        "Access denied",
                                                                    )
                                                                ))
                                                                .await
                                                        )?;
                                                    } else {
                                                        trace_err!(
                                                            object_input_send
                                                                .send(Ok(vec![(
                                                                    object,
                                                                    trace_err!(location
                                                                        .ok_or_else(|| anyhow!(
                                                                    "No object location found"
                                                                )))?
                                                                )]))
                                                                .await
                                                        )?;
                                                    };
                                                }
                                            }
                                        }
                                    }
                                }
                                Message::FinishMessage(_) => {
                                    trace_err!(object_ack_send.send(AckSync::Finish).await)?;
                                    return Ok(());
                                }
                            },
                            _ => {
                                return Err(anyhow!(
                                    "No message provided in PullReplicationRequest"
                                ));
                            }
                        }
                    }
                    None => {
                        return Err(anyhow!("No message provided in PullReplicationRequest"));
                    }
                };
            }
            Ok::<(), anyhow::Error>(())
        });

        // Ack sync loop
        tokio::spawn(async move {
            let mut sync_map: HashSet<AckSync> = HashSet::new();
            while let Ok(ref ack_msg) = object_ack_rcv.recv().await {
                match ack_msg {
                    init @ AckSync::ObjectInit(_) => {
                        sync_map.insert(init.clone());
                    }
                    chunk @ AckSync::ObjectChunk(..) => {
                        sync_map.insert(chunk.clone());
                    }
                    AckSync::Finish => {
                        trace_err!(object_sync_send.send(sync_map.clone()).await)?;
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        });

        // Responding loop
        let proxy_replication_service = self.clone();
        tokio::spawn(async move {
            'outer: loop {
                match object_input_rcv.recv().await {
                    // Errors
                    Err(err) => {
                        // Technically no error, because rcv was closed and just has one init msg
                        // therefore closed means finished
                        trace!(?err);
                        return Ok(());
                    }
                    Ok(Err(err)) => {
                        error!("{err}");
                        // Here probably proper error matching is needed to
                        // separate unauthorized from other errors
                        trace_err!(
                            object_output_send
                                .send(Err(tonic::Status::unauthenticated(
                                    "Unauthorized to pull objects",
                                )))
                                .await
                        )?;
                    }

                    // Objects
                    Ok(Ok(objects)) => {
                        // Store access-checked objects
                        let mut stored_objects: HashMap<DieselUlid, usize> = HashMap::default();
                        for (object, location) in objects {
                            trace!(?object, ?location);
                            // Get footer
                            let footer = match proxy_replication_service
                                .get_footer(location.clone())
                                .await
                            {
                                Ok(mut footer) => {
                                    if let Some(ref mut footer) = footer {
                                        trace_err!(footer.parse())?;
                                    };
                                    footer
                                }
                                Err(err) => {
                                    error!("{err}");
                                    trace_err!(
                                        object_output_send
                                            .send(Err(tonic::Status::internal(
                                                "Could not parse footer",
                                            )))
                                            .await
                                    )?;
                                    break 'outer;
                                }
                            };
                            // Get blocklist
                            let blocklist = footer.map(|f| f.get_blocklist());
                            // Need to keep track when to create an object, and when to only update the location
                            // Get chunk size from blocklist
                            let max_blocks = blocklist.as_ref().map(|l| l.len()).unwrap_or(1);
                            stored_objects.insert(object.id, max_blocks);
                            // Send ObjectInfo into stream
                            trace_err!(object_output_send
                                .send(Ok(PullReplicationResponse {
                                    message: Some(pull_replication_response::Message::ObjectInfo(
                                        aruna_rust_api::api::dataproxy::services::v2::ObjectInfo {
                                            object_id: object.id.to_string(),
                                            chunks: max_blocks as i64,
                                            block_list: blocklist.clone().map(|block_list| block_list.iter().map(|block| *block as u32).collect()).unwrap_or(Vec::new()),
                                            raw_size: location.raw_content_len,
                                            extra: None,
                                        },
                                    )),
                                }))
                                .await)?;
                            trace!("Send object info into stream");

                            // Send data into stream
                            trace_err!(
                                proxy_replication_service
                                    .send_object(
                                        object.id.to_string(),
                                        location,
                                        blocklist.unwrap_or(Vec::default()),
                                        object_output_send.clone(),
                                        retry_rcv.clone(),
                                    )
                                    .await
                            )?;
                            trace!("Send data into stream");
                        }
                        // Check if any message was unacknowledged
                        if let Ok(ack_msgs) = object_sync_rcv.recv().await {
                            for (id, max_blocks) in stored_objects.iter() {
                                // TODO!
                                // - Error handling
                                // - Retry logic
                                if ack_msgs.get(&AckSync::ObjectInit(*id)).is_none() {
                                    trace_err!(
                                        object_output_send
                                            .send(Err(tonic::Status::not_found(
                                                "Unacknowledged ObjectInit found",
                                            )))
                                            .await
                                    )?;
                                }
                                let max_blocks = *max_blocks as i64;
                                for chunk in 0..max_blocks {
                                    if ack_msgs.get(&AckSync::ObjectChunk(*id, chunk)).is_none() {
                                        trace_err!(
                                            object_output_send
                                                .send(Err(tonic::Status::not_found(
                                                    "Unacknowledged ObjectChunk found",
                                                )))
                                                .await
                                        )?;
                                    }
                                }
                            }
                        }
                    }
                };
            }
            Ok::<(), anyhow::Error>(())
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

    async fn get_footer(&self, location: ObjectLocation) -> Result<Option<FooterParser>> {
        let content_length = location.raw_content_len;
        let encryption_key = location
            .clone()
            .encryption_key
            .map(|k| k.as_bytes().to_vec());

        // Gets 128 kb chunks (last 2)
        let footer_parser: Option<FooterParser> = if content_length > 5245120 {
            trace!("getting footer");
            // Without encryption block because this is already checked inside
            let (footer_sender, footer_receiver) = async_channel::bounded(100);
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
                    .map_err(|_| tonic::Status::internal("Unable to get encryption_footer"))?;
                    let mut output = Vec::with_capacity(131_128);
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
                    let mut output = Vec::with_capacity(131_128);
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
        object_id: String,
        location: ObjectLocation,
        blocklist: Vec<u8>,
        sender: tokio::sync::mpsc::Sender<Result<PullReplicationResponse, tonic::Status>>,
        error_rcv: Receiver<Option<(i64, String)>>, // contains chunk_idx and object_id
    ) -> Result<()> {
        // Get encryption_key
        let key = location
            .clone()
            .encryption_key
            .map(|k| k.as_bytes().to_vec());
        // Create channel for get_object
        let (object_sender, object_receiver) = async_channel::bounded(255);

        // Spawn get_object
        let backend = self.backend.clone();
        tokio::spawn(
            async move {
                trace_err!(
                    backend
                        .get_object(location.clone(), None, object_sender)
                        .await
                )
            }
            .instrument(info_span!("get_object")),
        );

        // Spawn final part
        let _ = trace_err!(
            tokio::spawn(
                async move {
                    pin!(object_receiver);
                    let asrw = ArunaStreamReadWriter::new_with_sink(
                        // Receive get_object
                        object_receiver,
                        // ReplicationSink sends into stream via sender
                        ReplicationSink::new(object_id, blocklist, sender.clone(), error_rcv),
                    );

                    // Add decryption transformer
                    trace_err!(
                        asrw.add_transformer(trace_err!(ChaCha20Dec::new(key))?)
                            .process()
                            .await
                    )?;

                    Ok::<(), anyhow::Error>(())
                }
                .instrument(info_span!("query_data")),
            )
            .await
        )?;

        Ok(())
    }
}
