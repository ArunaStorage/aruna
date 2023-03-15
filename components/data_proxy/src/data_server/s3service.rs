use anyhow::anyhow;
use anyhow::Result;
use aruna_file::streamreadwrite::ArunaStreamReadWriter;
use aruna_file::transformers::async_sender_sink::AsyncSenderSink;
use aruna_file::transformers::compressor::ZstdEnc;
use aruna_file::transformers::encrypt::ChaCha20Enc;
use aruna_rust_api::api::internal::v1::internal_proxy_notifier_service_client::InternalProxyNotifierServiceClient;
use aruna_rust_api::api::internal::v1::GetEncryptionKeyRequest;
use aruna_rust_api::api::internal::v1::GetOrCreateObjectByPathRequest;
use aruna_rust_api::api::internal::v1::Location as ArunaLocation;
use futures::TryStreamExt;
use md5::{Digest, Md5};
use rand::distributions::Alphanumeric;
use rand::thread_rng;
use rand::Rng;
use s3s::dto::*;
use s3s::s3_error;
use s3s::S3Request;
use s3s::S3Result;
use s3s::S3;
use sha2::Sha256;
use std::sync::Arc;
use tonic::transport::Channel;

use crate::backends::storage_backend::StorageBackend;

use super::utils::construct_path;
use super::utils::create_location_from_hash;
use super::utils::create_stage_object;
use super::utils::validate_and_check_hashes;

#[derive(Debug)]
pub struct ServiceSettings {
    pub endpoint_id: uuid::Uuid,
    pub encrypting: bool,
    pub compressing: bool,
}

impl Default for ServiceSettings {
    fn default() -> Self {
        Self {
            endpoint_id: Default::default(),
            encrypting: true,
            compressing: true,
        }
    }
}

#[derive(Debug)]
pub struct S3ServiceServer {
    backend: Arc<Box<dyn StorageBackend>>,
    internal_notifier_service: InternalProxyNotifierServiceClient<Channel>,
    settings: ServiceSettings,
}

impl S3ServiceServer {
    pub async fn new(
        backend: Arc<Box<dyn StorageBackend>>,
        url: impl Into<String>,
        settings: ServiceSettings,
    ) -> Result<Self> {
        Ok(S3ServiceServer {
            backend,
            internal_notifier_service: InternalProxyNotifierServiceClient::connect(url.into())
                .await
                .map_err(|_| anyhow!("Unable to connect to internal notifiers"))?,
            settings,
        })
    }

    async fn _move_encode(&self, from: ArunaLocation, _to: ArunaLocation) -> Result<()> {
        if from.is_compressed {
            if from.is_encrypted {}
        }

        if self.settings.compressing {
            if self.settings.encrypting {
            } else {
            }
        } else {
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl S3 for S3ServiceServer {
    #[tracing::instrument]
    async fn put_object(&self, req: S3Request<PutObjectInput>) -> S3Result<PutObjectOutput> {
        let creds = match req.credentials {
            Some(cred) => cred,
            None => {
                log::error!("{}", "Not identified PutObjectRequest");
                return Err(s3_error!(NotSignedUp, "Your account is not signed up"));
            }
        };

        let get_obj_req = GetOrCreateObjectByPathRequest {
            path: construct_path(&req.input.bucket, &req.input.key),
            access_key: creds.access_key,
            object: Some(create_stage_object(
                &req.input.key,
                req.input.content_length,
            )),
        };

        let response = self
            .internal_notifier_service
            .clone() // This uses mpsc channel internally and just clones the handle -> Should be ok to clone
            .get_or_create_object_by_path(get_obj_req)
            .await
            .map_err(|e| {
                log::error!("{}", e);
                s3_error!(InternalError, "Internal notifier error")
            })?
            .into_inner();

        let (valid_md5, valid_sha256) = validate_and_check_hashes(
            req.input.content_md5,
            req.input.checksum_sha256,
            response.hashes,
        )?;

        let get_key_resp = self
            .internal_notifier_service
            .clone() // This uses mpsc channel internally and just clones the handle -> Should be ok to clone
            .get_encryption_key(GetEncryptionKeyRequest {
                path: "".to_string(),
                endpoint_id: self.settings.endpoint_id.to_string(),
                hash: valid_sha256.clone(),
            })
            .await
            .map_err(|e| {
                log::error!("{}", e);
                s3_error!(InternalError, "Internal notifier error")
            })?
            .into_inner();

        let enc_key = if get_key_resp.encryption_key.is_empty() {
            thread_rng().sample_iter(&Alphanumeric).take(32).collect()
        } else {
            get_key_resp.encryption_key.as_bytes().to_vec()
        };

        let mut md5_hash = Md5::new();
        let mut sha256_hash = Sha256::new();

        match req.input.body {
            Some(data) => {
                let (location, is_temp) = create_location_from_hash(
                    &valid_sha256,
                    &response.object_id,
                    &response.collection_id,
                );

                let (sender, recv) = async_channel::bounded(10);
                let backend_handle = self.backend.clone();
                tokio::spawn(async move {
                    backend_handle
                        .put_object(recv, location, req.input.content_length)
                        .await
                });

                // MD5 Stream
                let md5ed_stream = data.inspect_ok(|bytes| md5_hash.update(bytes.as_ref()));
                // Sha256 stream
                let shaed_stream =
                    md5ed_stream.inspect_ok(|bytes| sha256_hash.update(bytes.as_ref()));

                let mut awr = ArunaStreamReadWriter::new_with_sink(
                    shaed_stream,
                    AsyncSenderSink::new(sender),
                );

                if self.settings.encrypting {
                    awr = awr.add_transformer(ChaCha20Enc::new(true, enc_key).map_err(|e| {
                        log::error!("{}", e);
                        s3_error!(InternalError, "Internal data transformer encryption error")
                    })?);
                }

                if self.settings.compressing && !is_temp {
                    awr = awr.add_transformer(ZstdEnc::new(0, true));
                }

                awr.process().await.map_err(|e| {
                    log::error!("{}", e);
                    s3_error!(InternalError, "Internal data transformer processing error")
                })?;
            }
            None => {
                return Err(s3_error!(
                    InvalidObjectState,
                    "Request body / data is required, use ArunaAPI for empty objects"
                ))
            }
        }

        let final_md5 = format!("{:x}", md5_hash.finalize());
        let final_sha256 = format!("{:x}", sha256_hash.finalize());

        if !valid_md5.is_empty() && final_md5 != valid_md5 {
            return Err(s3_error!(
                InvalidDigest,
                "Invalid or inconsistent MD5 digest"
            ));
        }

        if !final_sha256.is_empty() && final_sha256 != valid_sha256 {
            return Err(s3_error!(
                InvalidDigest,
                "Invalid or inconsistent SHA256 digest"
            ));
        }

        // TODO: MOVER !

        let output = PutObjectOutput {
            e_tag: Some(response.object_id),
            checksum_sha256: Some("".to_string()),
            ..Default::default()
        };
        Ok(output)
    }
}
