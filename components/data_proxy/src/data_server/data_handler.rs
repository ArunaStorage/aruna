use crate::backends::storage_backend::StorageBackend;
use crate::data_server::utils::buffered_s3_sink::BufferedS3Sink;
use crate::data_server::utils::utils::validate_expected_hashes;
use crate::ServiceSettings;
use anyhow::anyhow;
use anyhow::bail;
use anyhow::Result;
use aruna_file::helpers::notifications_helper::parse_size_from_notifications;
use aruna_file::streamreadwrite::ArunaStreamReadWriter;
use aruna_file::transformers::async_sender_sink::AsyncSenderSink;
use aruna_file::transformers::compressor::ZstdEnc;
use aruna_file::transformers::decrypt::ChaCha20Dec;
use aruna_file::transformers::encrypt::ChaCha20Enc;
use aruna_file::transformers::footer::FooterGenerator;
use aruna_file::transformers::size_probe::SizeProbe;
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
use tonic::transport::Channel;

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
        aruna_path: String,
    ) -> Result<()> {
        if from.is_compressed {
            bail!("Unimplemented move operation (compressed input)")
        }

        let temp_size = self.backend.clone().head_object(from.clone()).await?;

        let (hashes, raw_file_size) = self.get_hashes(from.clone()).await?;

        validate_expected_hashes(expected_hashes, &hashes)?;

        let sha_hash = hashes
            .iter()
            .find(|e| e.alg == Hashalgorithm::Sha256 as i32)
            .ok_or_else(|| anyhow!("Sha256 not found"))?;

        to.bucket = format!("b{}", &sha_hash.hash[0..2]);
        to.path = sha_hash.hash[2..].to_string();

        // Get the correct encryption key based on the actual hash of the object
        to.encryption_key = self
            .internal_notifier_service
            .clone() // This uses mpsc channel internally and just clones the handle -> Should be ok to clone
            .get_or_create_encryption_key(GetOrCreateEncryptionKeyRequest {
                path: aruna_path,
                endpoint_id: self.settings.endpoint_id.to_string(),
                hash: sha_hash.hash.clone(),
            })
            .await
            .map_err(|e| {
                log::error!("{}", e);
                s3_error!(InternalError, "Internal notifier error")
            })?
            .into_inner()
            .encryption_key;

        // Check if this object already exists
        let existing = self.backend.head_object(to.clone()).await.is_ok();
        let mut to_clone = to.clone();

        if !existing {
            let (tx_send, tx_receive) = async_channel::bounded(10);

            let backend_clone = self.backend.clone();
            let settings_clone = self.settings.clone();
            let from_clone = from.clone();

            let awr_handle = tokio::spawn(async move {
                let mut awr = ArunaStreamReadWriter::new_with_sink(
                    tx_receive.map(Ok),
                    BufferedS3Sink::new(backend_clone, to.clone(), None, None, false, None),
                );

                if settings_clone.encrypting {
                    awr = awr.add_transformer(
                        ChaCha20Enc::new(false, to.encryption_key.as_bytes().to_vec()).map_err(
                            |e| {
                                log::error!("{}", e);
                                s3_error!(
                                    InternalError,
                                    "Internal data transformer encryption error"
                                )
                            },
                        )?,
                    );
                }

                if settings_clone.compressing {
                    if temp_size > 5242880 + 80 * 28 {
                        log::debug!("Added footer !");
                        awr = awr.add_transformer(FooterGenerator::new(None, true))
                    }
                    awr = awr.add_transformer(ZstdEnc::new(0, true));
                }

                awr = awr.add_transformer(ChaCha20Dec::new(to.encryption_key.as_bytes().to_vec())?);

                let result = awr.process().await;

                result
            });

            self.backend.get_object(from_clone, None, tx_send).await?;

            awr_handle.await??;
        }
        to_clone.endpoint_id = self.settings.endpoint_id.to_string();

        // Finalize the object request
        self.internal_notifier_service
            .clone() // This uses mpsc channel internally and just clones the handle -> Should be ok to clone
            .finalize_object(FinalizeObjectRequest {
                object_id,
                collection_id,
                location: Some(to_clone),
                hashes,
                content_length: raw_file_size,
            })
            .await?;

        self.backend.delete_object(from).await?;

        Ok(())
    }

    pub async fn get_hashes(&self, location: ArunaLocation) -> Result<(Vec<Hash>, i64)> {
        let locstring = format!("{}/{}", location.clone().bucket, location.clone().path);
        log::debug!("Calculating hashes for {:?}", locstring.clone());

        let (tx_send, tx_receive) = async_channel::bounded(10);

        let clone_key = location.encryption_key.as_bytes().to_vec();
        let mut md5_hash = Md5::new();
        let mut sha256_hash = Sha256::new();

        let (transform_send, transform_receive) = async_channel::bounded(10);

        let aswr_handle = tokio::spawn(async move {
            // Bind to variable to extend the lifetime of arsw to the end of the function
            let mut asr = ArunaStreamReadWriter::new_with_sink(
                tx_receive.clone().map(Ok),
                AsyncSenderSink::new(transform_send),
            );

            asr = asr.add_transformer(SizeProbe::new(1));
            asr = asr.add_transformer(ChaCha20Dec::new(clone_key)?);

            asr.process().await?;

            let notes = asr.query_notifications().await?;

            match 1 {
                1 => Ok(parse_size_from_notifications(notes, 1)?),
                _ => Err(anyhow!("Will not occur")),
            }
        });

        let hashing_handle = tokio::spawn(async move {
            let md5_str = transform_receive.inspect(|res_bytes| {
                if let Ok(bytes) = res_bytes {
                    md5_hash.update(bytes)
                }
            });
            let sha_str = md5_str.inspect(|res_bytes| {
                if let Ok(bytes) = res_bytes {
                    sha256_hash.update(bytes)
                }
            });
            sha_str.for_each(|_| future::ready(())).await;
            (
                format!("{:x}", sha256_hash.finalize()),
                format!("{:x}", md5_hash.finalize()),
            )
        });

        self.backend.get_object(location, None, tx_send).await?;
        let size = aswr_handle.await??;
        let (sha, md5) = hashing_handle.await?;
        // iterate the whole stream and do nothing

        log::debug!("Finished calculating hashes for {:?}", locstring);

        Ok((
            vec![
                Hash {
                    alg: Hashalgorithm::Md5 as i32,
                    hash: md5,
                },
                Hash {
                    alg: Hashalgorithm::Sha256 as i32,
                    hash: sha,
                },
            ],
            size as i64,
        ))
    }

    pub async fn finish_multipart(
        self: Arc<Self>,
        etag_parts: Vec<PartETag>,
        object_id: String,
        collection_id: String,
        upload_id: String,
        aruna_path: String,
    ) -> Result<(), S3Error> {
        // Get the encryption key from backend
        let enc_key = self
            .internal_notifier_service
            .clone() // This uses mpsc channel internally and just clones the handle -> Should be ok to clone
            .get_or_create_encryption_key(GetOrCreateEncryptionKeyRequest {
                path: aruna_path.clone(),
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
                    aruna_path,
                )
                .await
        });

        Ok(())
    }
}
