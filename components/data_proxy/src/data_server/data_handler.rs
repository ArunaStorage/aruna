use anyhow::anyhow;
use anyhow::bail;
use anyhow::Result;
use aruna_file::helpers::notifications_helper::parse_compressor_chunks;
use aruna_file::streamreadwrite::ArunaStreamReadWriter;
use aruna_file::transformers::async_sender_sink::AsyncSenderSink;
use aruna_file::transformers::compressor::ZstdEnc;
use aruna_file::transformers::decrypt::ChaCha20Dec;
use aruna_file::transformers::encrypt::ChaCha20Enc;
use aruna_file::transformers::footer::FooterGenerator;
use aruna_rust_api::api::internal::v1::internal_proxy_notifier_service_client::InternalProxyNotifierServiceClient;
use aruna_rust_api::api::internal::v1::FinalizeObjectRequest;
use aruna_rust_api::api::internal::v1::GetOrCreateEncryptionKeyRequest;
use aruna_rust_api::api::internal::v1::Location as ArunaLocation;
use aruna_rust_api::api::internal::v1::PartETag;
use aruna_rust_api::api::storage::models::v1::Hash;
use aruna_rust_api::api::storage::models::v1::Hashalgorithm;
use futures::future;
use futures::StreamExt;
use md5::{Digest, Md5};
use s3s::s3_error;
use s3s::S3Error;
use sha2::Sha256;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tonic::transport::Channel;

use crate::backends::storage_backend::StorageBackend;

use super::s3service::ServiceSettings;
use super::utils::create_ranges;
use super::utils::validate_expected_hashes;

#[derive(Debug)]
pub struct DataHandler {
    pub backend: Arc<Box<dyn StorageBackend>>,
    pub internal_notifier_service: InternalProxyNotifierServiceClient<Channel>,
    pub settings: ServiceSettings,
}

impl DataHandler {
    pub async fn new(
        backend: Arc<Box<dyn StorageBackend>>,
        url: impl Into<String>,
        settings: ServiceSettings,
    ) -> Result<Self> {
        Ok(DataHandler {
            backend,
            internal_notifier_service: InternalProxyNotifierServiceClient::connect(url.into())
                .await
                .map_err(|_| anyhow!("Unable to connect to internal notifiers"))?,
            settings,
        })
    }

    pub async fn move_encode(
        self: Arc<Self>,
        from: ArunaLocation,
        mut to: ArunaLocation,
        object_id: String,
        collection_id: String,
        expected_hashes: Option<Vec<Hash>>,
    ) -> Result<()> {
        if from.is_compressed {
            bail!("Unimplemented move operation (compressed input)")
        }

        let expected_size = self.backend.clone().head_object(from.clone()).await?;

        let hashes = self.get_hashes(from.clone()).await?;

        validate_expected_hashes(expected_hashes, &hashes)?;

        let sha_hash = hashes
            .iter()
            .find(|e| e.alg == Hashalgorithm::Sha256 as i32)
            .ok_or(anyhow!("Sha256 not found"))?;

        to.bucket = sha_hash.hash[0..2].to_string();
        to.path = sha_hash.hash[2..].to_string();

        // Check if this object already exists
        let resp = match self.backend.head_object(to.clone()).await {
            Ok(size) => Some(size),
            Err(_) => None,
        };

        // Check if size == expected size
        match resp {
            Some(size) => {
                if size != expected_size {
                    bail!("Target already exists but content-length does not match")
                }

                // Copy and re-encode data
                let mut ranges = create_ranges(expected_size, from.clone());
                let mut handles: Vec<
                    JoinHandle<
                        Result<(
                            aruna_rust_api::api::internal::v1::PartETag,
                            (usize, Vec<u8>),
                        )>,
                    >,
                > = Vec::new();

                let upload_id = self.backend.init_multipart_upload(to.clone()).await?;
                let range_len = ranges.len();

                let last_range = ranges
                    .pop()
                    .ok_or(anyhow!("At least one range must be specified"))?;

                for (part, range) in ranges.iter().enumerate() {
                    let backend_clone = self.backend.clone();
                    let backend_inner_clone = self.backend.clone();
                    let range = range.clone();
                    let from = from.clone();
                    let upload_id = upload_id.clone();
                    let to = to.clone();
                    let to_clone = to.clone();
                    handles.push(tokio::spawn(async move {
                        let (sender, receiver) = async_channel::bounded(10);

                        let read_handle: JoinHandle<Result<(usize, Vec<u8>)>> =
                            tokio::spawn(async move {
                                let (internal_sender, internal_receiver) =
                                    async_channel::bounded(10);
                                backend_inner_clone
                                    .get_object(from.clone(), Some(range), internal_sender)
                                    .await?;

                                let mut asrw = ArunaStreamReadWriter::new_with_sink(
                                    internal_receiver.map(|e| Ok(e)),
                                    AsyncSenderSink::new(sender),
                                )
                                .add_transformer(ChaCha20Enc::new(
                                    true,
                                    to.encryption_key.as_bytes().to_vec(),
                                )?)
                                .add_transformer(ZstdEnc::new(part, part == range_len))
                                .add_transformer(ChaCha20Dec::new(
                                    from.encryption_key.as_bytes().to_vec(),
                                )?);

                                asrw.process().await?;

                                let notes = asrw.query_notifications().await?;

                                Ok((part, parse_compressor_chunks(notes)?))
                            });

                        let tag = backend_clone
                            .upload_multi_object(
                                receiver,
                                to_clone,
                                upload_id,
                                (range.to - range.from) as i64,
                                (part + 1) as i32,
                            )
                            .await?;
                        Ok((tag, read_handle.await??))
                    }));
                }

                // Process last_range
                let backend_clone = self.backend.clone();
                let from = from.clone();
                let upload_id = upload_id.clone();
                let to = to.clone();
                let to_clone = to.clone();
                let handles_len = handles.len();

                let mut parts_vector = Vec::new();

                for task in handles {
                    let value = task.await??;
                    parts_vector.push((value.1 .0, value.0, value.1 .1));
                }
                parts_vector.sort_by(|a, b| a.0.cmp(&b.0));

                let footer_info = parts_vector.iter().fold(Vec::new(), |mut acc, e| {
                    acc.extend(e.2.clone());
                    acc
                });

                let (sender, receiver) = async_channel::bounded(10);

                let read_handle: JoinHandle<Result<(usize, Vec<u8>)>> = tokio::spawn(async move {
                    let (internal_sender, internal_receiver) = async_channel::bounded(10);
                    backend_clone
                        .get_object(from.clone(), Some(last_range), internal_sender)
                        .await?;

                    let mut asrw = ArunaStreamReadWriter::new_with_sink(
                        internal_receiver.map(|e| Ok(e)),
                        AsyncSenderSink::new(sender),
                    )
                    .add_transformer(ChaCha20Enc::new(
                        true,
                        to.encryption_key.as_bytes().to_vec(),
                    )?);

                    if footer_info.len() > 0 {
                        asrw = asrw.add_transformer(FooterGenerator::new(Some(footer_info), true))
                    }
                    asrw = asrw
                        .add_transformer(ZstdEnc::new(handles_len + 1, true))
                        .add_transformer(ChaCha20Dec::new(
                            from.encryption_key.as_bytes().to_vec(),
                        )?);

                    asrw.process().await?;

                    let notes = asrw.query_notifications().await?;

                    Ok((handles_len + 1, parse_compressor_chunks(notes)?))
                });

                let tag = self
                    .backend
                    .upload_multi_object(
                        receiver,
                        to_clone.clone(),
                        upload_id.clone(),
                        (last_range.to - last_range.from) as i64,
                        (handles_len + 2) as i32,
                    )
                    .await?;

                read_handle.await??;

                // Finish multipart
                let mut parts = parts_vector.into_iter().map(|e| e.1).collect::<Vec<_>>();
                parts.push(tag);

                self.backend
                    .finish_multipart_upload(to_clone, parts, upload_id)
                    .await?;
            }
            _ => {}
        };

        // Finalize the object request
        self.internal_notifier_service
            .clone() // This uses mpsc channel internally and just clones the handle -> Should be ok to clone
            .finalize_object(FinalizeObjectRequest {
                object_id: object_id,
                collection_id: collection_id,
                location: Some(to),
                hashes: hashes,
                content_length: expected_size,
            })
            .await?;

        Ok(())
    }

    pub async fn get_hashes(&self, location: ArunaLocation) -> Result<Vec<Hash>> {
        let (tx_send, tx_receive) = async_channel::unbounded();

        let backend_clone = self.backend.clone();
        tokio::spawn(async move { backend_clone.get_object(location, None, tx_send).await });
        let mut md5_hash = Md5::new();
        let mut sha256_hash = Sha256::new();

        let md5_str = tx_receive.inspect(|bytes| md5_hash.update(bytes.as_ref()));
        let sha_str = md5_str.inspect(|bytes| sha256_hash.update(bytes.as_ref()));
        // iterate the whole stream and do nothing
        sha_str.for_each(|_| future::ready(())).await;

        Ok(vec![
            Hash {
                alg: Hashalgorithm::Md5 as i32,
                hash: format!("{:x}", md5_hash.finalize()),
            },
            Hash {
                alg: Hashalgorithm::Sha256 as i32,
                hash: format!("{:x}", sha256_hash.finalize()),
            },
        ])
    }

    pub async fn finish_multipart(
        self: Arc<Self>,
        etag_parts: Vec<PartETag>,
        object_id: String,
        collection_id: String,
        upload_id: String,
    ) -> Result<(), S3Error> {
        // Get the encryption key from backend
        let enc_key = self
            .internal_notifier_service
            .clone() // This uses mpsc channel internally and just clones the handle -> Should be ok to clone
            .get_or_create_encryption_key(GetOrCreateEncryptionKeyRequest {
                path: "".to_string(),
                endpoint_id: self.settings.endpoint_id.to_string(),
                hash: "".to_string(),
            })
            .await
            .map_err(|e| {
                log::error!("{}", e);
                s3_error!(InternalError, "Internal notifier error")
            })?
            .into_inner()
            .encryption_key;

        self.backend
            .clone()
            .finish_multipart_upload(
                ArunaLocation {
                    bucket: "temp".to_string(),
                    path: format!("{}/{}", collection_id, object_id),
                    ..Default::default()
                },
                etag_parts,
                upload_id,
            )
            .await
            .map_err(|e| {
                log::error!("{}", e);
                s3_error!(InvalidArgument, "Unable to finish multipart")
            })?;

        let mover_clone = self.clone();

        let encrypting = self.settings.encrypting;
        let compressing = self.settings.compressing;

        tokio::spawn(async move {
            mover_clone
                .move_encode(
                    ArunaLocation {
                        bucket: "temp".to_string(),
                        path: format!("{}/{}", collection_id, object_id),
                        is_encrypted: encrypting,
                        encryption_key: enc_key.clone(),
                        ..Default::default()
                    },
                    ArunaLocation {
                        bucket: "temp".to_string(),
                        path: format!("{}/{}", collection_id, object_id),
                        is_encrypted: encrypting,
                        is_compressed: compressing,
                        encryption_key: enc_key,
                        ..Default::default()
                    },
                    object_id,
                    collection_id,
                    None,
                )
                .await
        });

        Ok(())
    }
}
