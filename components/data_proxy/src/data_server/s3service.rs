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
use aruna_rust_api::api::internal::v1::LocationType;
use s3s::dto::*;
use s3s::s3_error;
use s3s::S3Request;
use s3s::S3Result;
use s3s::S3;
use std::sync::Arc;
use tonic::transport::Channel;

use crate::backends::storage_backend::StorageBackend;

use super::utils::construct_path;
use super::utils::create_stage_object;

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
    async fn create_bucket(
        &self,
        _req: S3Request<CreateBucketInput>,
    ) -> S3Result<CreateBucketOutput> {
        let output = CreateBucketOutput::default();
        Ok(output)
    }

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

        //let hash_md5 = response.hash;
        let _hash_sha256 = match response.hash {
            Some(_h) => {}
            None => {}
        };

        let get_key_resp = self
            .internal_notifier_service
            .clone() // This uses mpsc channel internally and just clones the handle -> Should be ok to clone
            .get_encryption_key(GetEncryptionKeyRequest {
                path: "".to_string(),
                endpoint_id: self.settings.endpoint_id.to_string(),
                hash: "".to_string(),
            })
            .await
            .map_err(|e| {
                log::error!("{}", e);
                s3_error!(InternalError, "Internal notifier error")
            })?
            .into_inner();

        match req.input.body {
            Some(data) => {
                let (sender, recv) = async_channel::bounded(10);
                let backend_handle = self.backend.clone();
                tokio::spawn(async move {
                    backend_handle
                        .put_object(
                            recv,
                            ArunaLocation {
                                r#type: LocationType::S3 as i32,
                                bucket: "a".to_string(),
                                path: "b".to_string(),
                                ..Default::default()
                            },
                            req.input.content_length,
                        )
                        .await
                });
                let mut awr =
                    ArunaStreamReadWriter::new_with_sink(data, AsyncSenderSink::new(sender));

                if self.settings.encrypting {
                    awr = awr.add_transformer(
                        ChaCha20Enc::new(true, get_key_resp.encryption_key.into_bytes()).map_err(
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

                if self.settings.compressing {
                    awr = awr.add_transformer(ZstdEnc::new(0, true));
                }

                awr.process().await.map_err(|e| {
                    log::error!("{}", e);
                    s3_error!(InternalError, "Internal data transformer processing error")
                })?
            }
            None => {}
        }

        let output = PutObjectOutput {
            checksum_sha256: Some("".to_string()),
            ..Default::default()
        };
        Ok(output)
    }
}
