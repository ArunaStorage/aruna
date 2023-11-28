use crate::{
    caching::cache::Cache, data_backends::storage_backend::StorageBackend,
    s3_frontend::utils::buffered_s3_sink::BufferedS3Sink, structs::Object, trace_err,
};
use ahash::RandomState;
use anyhow::{anyhow, Result};
use aruna_file::{
    streamreadwrite::ArunaStreamReadWriter,
    transformer::ReadWriter,
    transformers::{
        encrypt::ChaCha20Enc, footer::FooterGenerator, hashing_transformer::HashingTransformer,
        size_probe::SizeProbe, zstd_comp::ZstdEnc,
    },
};
use async_channel::Receiver;
use dashmap::DashSet;
use diesel_ulid::DieselUlid;
use futures_util::TryStreamExt;
use md5::{Digest, Md5};
use sha2::Sha256;
use std::sync::Arc;
use tracing::{error, trace};

pub struct ReplicationMessage {
    pub object_id: DieselUlid,
    pub download_url: String,
    pub encryption_key: String,
    pub is_compressed: bool,
    pub direction: Direction,
}

#[derive(Debug, Hash, Eq, PartialEq)]
pub enum Direction {
    Push,
    Pull,
}

pub struct ReplicationHandler {
    pub receiver: Receiver<ReplicationMessage>,
    pub backend: Arc<Box<dyn StorageBackend>>,
}

impl ReplicationHandler {
    #[tracing::instrument(level = "trace")]
    pub fn new(
        receiver: Receiver<ReplicationMessage>,
        backend: Arc<Box<dyn StorageBackend>>,
    ) -> Self {
        Self { receiver, backend }
    }

    #[tracing::instrument(level = "trace", skip(self, cache))]
    pub async fn run(self, cache: Arc<Cache>) -> Result<()> {
        let queue: Arc<DashSet<(DieselUlid, String, String, bool, Direction), RandomState>> =
            Arc::new(DashSet::default());

        // Push messages into DashMap for deduplication
        let queue_clone = queue.clone();
        let receiver = self.receiver.clone();
        let recieve = tokio::spawn(async move {
            while let Ok(ReplicationMessage {
                object_id,
                download_url,
                encryption_key,
                is_compressed,
                direction,
            }) = receiver.recv().await
            {
                queue_clone.insert((
                    object_id,
                    download_url,
                    encryption_key,
                    is_compressed,
                    direction,
                ));
            }
        });

        // Proccess DashMap entries in separate task
        let backend = self.backend.clone();
        let process = tokio::spawn(async move {
            let client = reqwest::Client::new();
            loop {
                let next = match queue.iter().next() {
                    Some(entry) => entry,
                    None => continue,
                };
                let (id, url, key, compressed, direction) = next.key();
                if let Err(err) = ReplicationHandler::process(
                    *id,
                    url,
                    key,
                    compressed,
                    direction,
                    cache.clone(),
                    backend.clone(),
                    client.clone(),
                )
                .await
                {
                    tracing::error!(error = ?err, msg = err.to_string());
                };
            }
        });
        // Run both tasks simultaneously
        trace_err!(tokio::try_join!(recieve, process))?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(cache))]
    // TODO
    // - Pull/Push logic
    // - write objects into storage backend
    // - write objects into cache/db
    async fn process(
        object_id: DieselUlid,
        url: &str,
        encryption_key: &str,
        compressed: &bool,
        direction: &Direction,
        cache: Arc<Cache>,
        backend: Arc<Box<dyn StorageBackend>>,
        client: reqwest::Client,
    ) -> Result<()> {
        match direction {
            Direction::Push => {
                return trace_err!(Err(anyhow!(
                    "Push replciation is currently not implemented"
                )))
            }
            Direction::Pull => {
                // TODO! MULTIPART!!!

                /* -----------------------------------------
                 * ------------- DOWNLOAD ------------------
                 * -----------------------------------------*/
                let response = client.get(url).send().await?;
                let (object, location) = trace_err!(cache
                    .resources
                    .get(&object_id)
                    .ok_or_else(|| anyhow!("No object found in path")))?
                .value()
                .clone();

                /* -----------------------------------------
                 * ------------- PUT INTO BUCKET -----------
                 * -----------------------------------------*/
                // Initialize data location in the storage backend
                let location = if let Some(location) = location {
                    location
                } else {
                    let loc = trace_err!(
                        backend
                            .initialize_location(
                                &object,
                                response.content_length().map(|s| s as i64),
                                None,
                                false
                            )
                            .await
                    )?;
                    trace!("Initialized data location");
                    loc
                };

                // Initialize hashing transformers
                let (initial_sha_trans, initial_sha_recv) = HashingTransformer::new(Sha256::new());
                let (initial_md5_trans, initial_md5_recv) = HashingTransformer::new(Md5::new());
                let (initial_size_trans, initial_size_recv) = SizeProbe::new();
                let (final_sha_trans, final_sha_recv) = HashingTransformer::new(Sha256::new());
                let (final_size_trans, final_size_recv) = SizeProbe::new();

                let stream = response
                    .bytes_stream()
                    .map_err(|_| anyhow!("Error recieving replication object stream").into());
                //match stream {
                //    Ok(data) => {
                let mut awr = ArunaStreamReadWriter::new_with_sink(
                    stream,
                    BufferedS3Sink::new(
                        backend.clone(),
                        location.clone(),
                        None,
                        None,
                        false,
                        None,
                        false,
                    )
                    .0,
                );

                awr = awr.add_transformer(initial_sha_trans);
                awr = awr.add_transformer(initial_md5_trans);
                awr = awr.add_transformer(initial_size_trans);

                if location.compressed {
                    trace!("adding zstd decompressor");
                    awr = awr.add_transformer(ZstdEnc::new(true));
                    if location.raw_content_len > 5242880 + 80 * 28 {
                        trace!("adding footer generator");
                        awr = awr.add_transformer(FooterGenerator::new(None))
                    }
                }

                if let Some(enc_key) = &location.encryption_key {
                    awr = awr.add_transformer(trace_err!(ChaCha20Enc::new(
                        true,
                        enc_key.to_string().into_bytes()
                    ))?);
                }

                awr = awr.add_transformer(final_sha_trans);
                awr = awr.add_transformer(final_size_trans);

                trace_err!(awr.process().await)?;

                cache.upsert_object(object, Some(location));
            }
        }
        Ok(())
    }
}
