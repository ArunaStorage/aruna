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
use aruna_rust_api::api::internal::v1::GetOrCreateObjectByPathRequest;
use aruna_rust_api::api::internal::v1::Location as ArunaLocation;
use aruna_rust_api::api::internal::v1::PartETag;
use aruna_rust_api::api::storage::models::v1::Hash;
use aruna_rust_api::api::storage::models::v1::Hashalgorithm;
use futures::future;
use futures::StreamExt;
use futures::TryStreamExt;
use md5::{Digest, Md5};
use s3s::dto::*;
use s3s::s3_error;
use s3s::S3Error;
use s3s::S3Request;
use s3s::S3Result;
use s3s::S3;
use sha2::Sha256;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tonic::transport::Channel;

use crate::backends::storage_backend::StorageBackend;

use super::utils::construct_path;
use super::utils::create_location_from_hash;
use super::utils::create_ranges;
use super::utils::create_stage_object;
use super::utils::validate_and_check_hashes;
use super::utils::validate_expected_hashes;

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
    data_handler: Arc<DataHandler>,
    settings: ServiceSettings,
}

#[derive(Debug)]
pub struct DataHandler {
    backend: Arc<Box<dyn StorageBackend>>,
    internal_notifier_service: InternalProxyNotifierServiceClient<Channel>,
}

impl S3ServiceServer {
    pub async fn new(
        backend: Arc<Box<dyn StorageBackend>>,
        url: impl Into<String>,
        settings: ServiceSettings,
    ) -> Result<Self> {
        Ok(S3ServiceServer {
            backend: backend.clone(),
            data_handler: Arc::new(DataHandler::new(backend, url).await?),
            settings,
        })
    }
}
impl DataHandler {
    pub async fn new(
        backend: Arc<Box<dyn StorageBackend>>,
        url: impl Into<String>,
    ) -> Result<Self> {
        Ok(DataHandler {
            backend,
            internal_notifier_service: InternalProxyNotifierServiceClient::connect(url.into())
                .await
                .map_err(|_| anyhow!("Unable to connect to internal notifiers"))?,
        })
    }

    async fn move_encode(
        &self,
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
            })
            .await?;

        Ok(())
    }

    async fn get_hashes(&self, location: ArunaLocation) -> Result<Vec<Hash>> {
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
}

#[async_trait::async_trait]
impl S3 for S3ServiceServer {
    #[tracing::instrument]
    async fn put_object(&self, req: S3Request<PutObjectInput>) -> S3Result<PutObjectOutput> {
        // Get the credentials
        let creds = match req.credentials {
            Some(cred) => cred,
            None => {
                log::error!("{}", "Not identified PutObjectRequest");
                return Err(s3_error!(NotSignedUp, "Your account is not signed up"));
            }
        };

        // Get the object from backend
        let get_obj_req = GetOrCreateObjectByPathRequest {
            path: construct_path(&req.input.bucket, &req.input.key),
            access_key: creds.access_key,
            object: Some(create_stage_object(
                &req.input.key,
                req.input.content_length,
            )),
        };

        // Get or create object by path
        let response = self
            .data_handler
            .internal_notifier_service
            .clone() // This uses mpsc channel internally and just clones the handle -> Should be ok to clone
            .get_or_create_object_by_path(get_obj_req)
            .await
            .map_err(|e| {
                log::error!("{}", e);
                s3_error!(InternalError, "Internal notifier error")
            })?
            .into_inner();

        // Check / get hashes
        let (valid_md5, valid_sha256) = validate_and_check_hashes(
            req.input.content_md5,
            req.input.checksum_sha256,
            response.hashes,
        )?;

        // Get the encryption key from backend
        let enc_key = self
            .data_handler
            .internal_notifier_service
            .clone() // This uses mpsc channel internally and just clones the handle -> Should be ok to clone
            .get_or_create_encryption_key(GetOrCreateEncryptionKeyRequest {
                path: "".to_string(),
                endpoint_id: self.settings.endpoint_id.to_string(),
                hash: valid_sha256.clone(),
            })
            .await
            .map_err(|e| {
                log::error!("{}", e);
                s3_error!(InternalError, "Internal notifier error")
            })?
            .into_inner()
            .encryption_key
            .as_bytes()
            .to_vec();

        let mut md5_hash = Md5::new();
        let mut sha256_hash = Sha256::new();

        // Create a target location (may be a temp location)
        let (location, is_temp) = create_location_from_hash(
            &valid_sha256,
            &response.object_id,
            &response.collection_id,
            self.settings.encrypting,
            self.settings.compressing,
            String::from_utf8_lossy(&enc_key).into(),
        );

        // Check if this object already exists
        let resp = match self.backend.head_object(location.clone()).await {
            Ok(size) => {
                if size == req.input.content_length {
                    Some(size)
                } else {
                    return Err(s3_error!(
                        InvalidArgument,
                        "Illegal content-length for hash"
                    ));
                }
            }
            Err(_) => None,
        };
        let mut final_md5 = String::new();
        let mut final_sha256 = String::new();

        // If the object exists and the signatures match -> Skip the download
        if resp.is_some() {
            if !valid_md5.is_empty() && !valid_sha256.is_empty() {
                final_md5 = valid_md5.clone();
                final_sha256 = valid_sha256.clone();
            } else {
                return Err(s3_error!(SignatureDoesNotMatch, "Invalid hash"));
            }
        }
        if resp.is_none() {
            match req.input.body {
                Some(data) => {
                    let (sender, recv) = async_channel::bounded(10);
                    let backend_handle = self.backend.clone();
                    let cloned_loc = location.clone();
                    let handle = tokio::spawn(async move {
                        backend_handle
                            .put_object(recv, cloned_loc, req.input.content_length)
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
                        awr =
                            awr.add_transformer(ChaCha20Enc::new(true, enc_key).map_err(|e| {
                                log::error!("{}", e);
                                s3_error!(
                                    InternalError,
                                    "Internal data transformer encryption error"
                                )
                            })?);
                    }

                    if self.settings.compressing && !is_temp {
                        if req.input.content_length > 5242880 + 80 * 28 {
                            awr = awr.add_transformer(FooterGenerator::new(None, true))
                        }
                        awr = awr.add_transformer(ZstdEnc::new(0, true));
                    }

                    awr.process().await.map_err(|e| {
                        log::error!("{}", e);
                        s3_error!(InternalError, "Internal data transformer processing error")
                    })?;

                    handle
                        .await
                        .map_err(|e| {
                            log::error!("{}", e);
                            s3_error!(
                                InternalError,
                                "Internal data transformer processing error (TokioJoinHandle)"
                            )
                        })?
                        .map_err(|e| {
                            log::error!("{}", e);
                            s3_error!(
                                InternalError,
                                "Internal data transformer processing error (Backend)"
                            )
                        })?;
                }
                None => {
                    return Err(s3_error!(
                        InvalidObjectState,
                        "Request body / data is required, use ArunaAPI for empty objects"
                    ))
                }
            }

            final_md5 = format!("{:x}", md5_hash.finalize());
            final_sha256 = format!("{:x}", sha256_hash.finalize());

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
            if is_temp {
                self.data_handler
                    .move_encode(
                        location.clone(),
                        create_location_from_hash(
                            &final_sha256,
                            &response.object_id,
                            &response.collection_id,
                            self.settings.encrypting,
                            self.settings.compressing,
                            location.encryption_key.clone(),
                        )
                        .0,
                        response.object_id.clone(),
                        response.collection_id.clone(),
                        Some(vec![
                            Hash {
                                alg: Hashalgorithm::Md5 as i32,
                                hash: final_md5.clone(),
                            },
                            Hash {
                                alg: Hashalgorithm::Sha256 as i32,
                                hash: final_sha256.clone(),
                            },
                        ]),
                    )
                    .await
                    .map_err(|e| {
                        log::error!("{}", e);
                        s3_error!(InternalError, "Internal data mover error")
                    })?
            }
        }

        if !is_temp {
            self.data_handler
                .internal_notifier_service
                .clone() // This uses mpsc channel internally and just clones the handle -> Should be ok to clone
                .finalize_object(FinalizeObjectRequest {
                    object_id: response.object_id.clone(),
                    collection_id: response.collection_id.clone(),
                    location: Some(location),
                    hashes: vec![
                        Hash {
                            alg: Hashalgorithm::Md5 as i32,
                            hash: final_md5,
                        },
                        Hash {
                            alg: Hashalgorithm::Sha256 as i32,
                            hash: final_sha256,
                        },
                    ],
                })
                .await
                .map_err(|e| {
                    log::error!("{}", e);
                    s3_error!(InternalError, "Internal aruna error")
                })?;
        }

        let output = PutObjectOutput {
            e_tag: Some(response.object_id),
            checksum_sha256: Some("".to_string()),
            ..Default::default()
        };
        Ok(output)
    }

    #[tracing::instrument]
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<CreateMultipartUploadOutput> {
        // Get the credentials
        let creds = match req.credentials {
            Some(cred) => cred,
            None => {
                log::error!("{}", "Not identified PutObjectRequest");
                return Err(s3_error!(NotSignedUp, "Your account is not signed up"));
            }
        };

        // Get the object from backend
        let get_obj_req = GetOrCreateObjectByPathRequest {
            path: construct_path(&req.input.bucket, &req.input.key),
            access_key: creds.access_key,
            object: Some(create_stage_object(&req.input.key, 0)),
        };

        // Get or create object by path
        let response = self
            .data_handler
            .internal_notifier_service
            .clone() // This uses mpsc channel internally and just clones the handle -> Should be ok to clone
            .get_or_create_object_by_path(get_obj_req)
            .await
            .map_err(|e| {
                log::error!("{}", e);
                s3_error!(InternalError, "Internal notifier error")
            })?
            .into_inner();

        let init_response = self
            .backend
            .clone()
            .init_multipart_upload(ArunaLocation {
                bucket: "temp".to_string(),
                path: format!("{}/{}", response.collection_id, response.object_id),
                ..Default::default()
            })
            .await
            .map_err(|e| {
                log::error!("{}", e);
                s3_error!(InvalidArgument, "Unable to initialize multi-part")
            })?;

        Ok(CreateMultipartUploadOutput {
            key: Some(req.input.key),
            bucket: Some(req.input.bucket),
            upload_id: Some(init_response),
            ..Default::default()
        })
    }

    #[tracing::instrument]
    async fn upload_part(&self, req: S3Request<UploadPartInput>) -> S3Result<UploadPartOutput> {
        // Get the credentials
        let creds = match req.credentials {
            Some(cred) => cred,
            None => {
                log::error!("{}", "Not identified PutObjectRequest");
                return Err(s3_error!(NotSignedUp, "Your account is not signed up"));
            }
        };

        // Get the object from backend
        let get_obj_req = GetOrCreateObjectByPathRequest {
            path: construct_path(&req.input.bucket, &req.input.key),
            access_key: creds.access_key,
            object: Some(create_stage_object(&req.input.key, 0)),
        };

        // Get or create object by path
        let response = self
            .data_handler
            .internal_notifier_service
            .clone() // This uses mpsc channel internally and just clones the handle -> Should be ok to clone
            .get_or_create_object_by_path(get_obj_req)
            .await
            .map_err(|e| {
                log::error!("{}", e);
                s3_error!(InternalError, "Internal notifier error")
            })?
            .into_inner();

        // Get the encryption key from backend
        let enc_key = self
            .data_handler
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
            .encryption_key
            .as_bytes()
            .to_vec();

        let (sender, recv) = async_channel::bounded(10);
        let backend_handle = self.backend.clone();
        let cloned_loc = ArunaLocation {
            ..Default::default()
        };
        let handle = tokio::spawn(async move {
            backend_handle
                .upload_multi_object(
                    recv,
                    cloned_loc,
                    req.input.upload_id,
                    req.input.content_length,
                    req.input.part_number,
                )
                .await
        });

        let etag = match req.input.body {
            Some(data) => {
                let mut awr =
                    ArunaStreamReadWriter::new_with_sink(data, AsyncSenderSink::new(sender));

                if self.settings.encrypting {
                    awr = awr.add_transformer(ChaCha20Enc::new(true, enc_key).map_err(|e| {
                        log::error!("{}", e);
                        s3_error!(InternalError, "Internal data transformer encryption error")
                    })?);
                }

                awr.process().await.map_err(|e| {
                    log::error!("{}", e);
                    s3_error!(InternalError, "Internal data transformer processing error")
                })?;

                handle
                    .await
                    .map_err(|e| {
                        log::error!("{}", e);
                        s3_error!(
                            InternalError,
                            "Internal data transformer processing error (TokioJoinHandle)"
                        )
                    })?
                    .map_err(|e| {
                        log::error!("{}", e);
                        s3_error!(
                            InternalError,
                            "Internal data transformer processing error (Backend)"
                        )
                    })?
            }
            _ => return Err(s3_error!(InvalidPart, "MultiPart cannot be empty")),
        };
        Ok(UploadPartOutput {
            e_tag: Some(etag.etag),
            ..Default::default()
        })
    }

    #[tracing::instrument]
    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<CompleteMultipartUploadOutput> {
        // Get the credentials
        let creds = match req.credentials {
            Some(cred) => cred,
            None => {
                log::error!("{}", "Not identified PutObjectRequest");
                return Err(s3_error!(NotSignedUp, "Your account is not signed up"));
            }
        };

        // Get the object from backend
        let get_obj_req = GetOrCreateObjectByPathRequest {
            path: construct_path(&req.input.bucket, &req.input.key),
            access_key: creds.access_key,
            object: Some(create_stage_object(&req.input.key, 0)),
        };

        // Get or create object by path
        let response = self
            .data_handler
            .internal_notifier_service
            .clone() // This uses mpsc channel internally and just clones the handle -> Should be ok to clone
            .get_or_create_object_by_path(get_obj_req)
            .await
            .map_err(|e| {
                log::error!("{}", e);
                s3_error!(InternalError, "Internal notifier error")
            })?
            .into_inner();

        // Does this object exists (including object id etc)

        // Get the encryption key from backend
        let enc_key = self
            .data_handler
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

        let parts = match req.input.multipart_upload {
            Some(parts) => parts
                .parts
                .ok_or(s3_error!(InvalidPart, "Parts must be specified")),
            None => return Err(s3_error!(InvalidPart, "Parts must be specified")),
        }?;

        let etag_parts = parts
            .into_iter()
            .map(|a| {
                Ok(PartETag {
                    part_number: a.part_number as i64,
                    etag: a
                        .e_tag
                        .ok_or(s3_error!(InvalidPart, "etag must be specified"))?,
                })
            })
            .collect::<Result<Vec<PartETag>, S3Error>>()?;

        self.backend
            .clone()
            .finish_multipart_upload(
                ArunaLocation {
                    bucket: "temp".to_string(),
                    path: format!("{}/{}", response.collection_id, response.object_id),
                    ..Default::default()
                },
                etag_parts,
                req.input.upload_id,
            )
            .await
            .map_err(|e| {
                log::error!("{}", e);
                s3_error!(InvalidArgument, "Unable to finish multipart")
            })?;

        let mover_clone = self.data_handler.clone();

        let encrypting = self.settings.encrypting;
        let compressing = self.settings.compressing;

        tokio::spawn(async move {
            mover_clone
                .move_encode(
                    ArunaLocation {
                        bucket: "temp".to_string(),
                        path: format!("{}/{}", response.collection_id, response.object_id),
                        is_encrypted: encrypting,
                        encryption_key: enc_key.clone(),
                        ..Default::default()
                    },
                    ArunaLocation {
                        bucket: "temp".to_string(),
                        path: format!("{}/{}", response.collection_id, response.object_id),
                        is_encrypted: encrypting,
                        is_compressed: compressing,
                        encryption_key: enc_key,
                        ..Default::default()
                    },
                    response.object_id,
                    response.collection_id,
                    None,
                )
                .await
        });

        Ok(CompleteMultipartUploadOutput {
            ..Default::default()
        })
    }
}
