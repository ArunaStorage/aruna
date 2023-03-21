use anyhow::Result;
use aruna_file::streamreadwrite::ArunaStreamReadWriter;
use aruna_file::transformers::async_sender_sink::AsyncSenderSink;
use aruna_file::transformers::compressor::ZstdEnc;
use aruna_file::transformers::encrypt::ChaCha20Enc;
use aruna_file::transformers::footer::FooterGenerator;
use aruna_rust_api::api::internal::v1::FinalizeObjectRequest;
use aruna_rust_api::api::internal::v1::GetObjectLocationRequest;
use aruna_rust_api::api::internal::v1::GetOrCreateEncryptionKeyRequest;
use aruna_rust_api::api::internal::v1::GetOrCreateObjectByPathRequest;
use aruna_rust_api::api::internal::v1::Location as ArunaLocation;
use aruna_rust_api::api::internal::v1::PartETag;
use aruna_rust_api::api::storage::models::v1::Hash;
use aruna_rust_api::api::storage::models::v1::Hashalgorithm;
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

use crate::backends::storage_backend::StorageBackend;
use crate::data_server::buffered_s3_sink::BufferedS3Sink;

use super::data_handler::DataHandler;
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
    data_handler: Arc<DataHandler>,
}

impl S3ServiceServer {
    pub async fn new(
        backend: Arc<Box<dyn StorageBackend>>,
        data_handler: Arc<DataHandler>,
    ) -> Result<Self> {
        Ok(S3ServiceServer {
            backend: backend.clone(),
            data_handler,
        })
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
            get_only: false,
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
                if e.message() == "Record not found" {
                    s3_error!(NoSuchBucket, "Bucket not found")
                } else {
                    s3_error!(InternalError, "Internal notifier error")
                }
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
                path: format!("s3://{}/{}", &req.input.bucket, &req.input.key),
                endpoint_id: self.data_handler.settings.endpoint_id.to_string(),
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
            self.data_handler.settings.encrypting,
            self.data_handler.settings.compressing,
            String::from_utf8_lossy(&enc_key).into(),
        );

        let resp = if !is_temp {
            // Check if this object already exists
            match self.backend.head_object(location.clone()).await {
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
            }
        } else {
            None
        };

        let mut final_md5 = String::new();
        let mut final_sha256 = String::new();

        // If the object exists and the signatures match -> Skip the download

        if resp.is_some() {
            if !valid_md5.is_empty() && !valid_sha256.is_empty() {
                final_md5 = valid_md5.clone();
                final_sha256 = valid_sha256.clone();
            } else {
                println!("war hier !");
                return Err(s3_error!(SignatureDoesNotMatch, "Invalid hash"));
            }
        }
        if resp.is_none() {
            match req.input.body {
                Some(data) => {
                    // MD5 Stream
                    let md5ed_stream = data.inspect_ok(|bytes| md5_hash.update(bytes.as_ref()));
                    // Sha256 stream
                    let shaed_stream =
                        md5ed_stream.inspect_ok(|bytes| sha256_hash.update(bytes.as_ref()));

                    let mut awr = ArunaStreamReadWriter::new_with_sink(
                        shaed_stream,
                        BufferedS3Sink::new(
                            self.backend.clone(),
                            location.clone(),
                            None,
                            None,
                            None,
                        ),
                    );

                    if self.data_handler.settings.encrypting {
                        awr =
                            awr.add_transformer(ChaCha20Enc::new(true, enc_key).map_err(|e| {
                                log::error!("{}", e);
                                s3_error!(
                                    InternalError,
                                    "Internal data transformer encryption error"
                                )
                            })?);
                    }

                    if self.data_handler.settings.compressing && !is_temp {
                        if req.input.content_length > 5242880 + 80 * 28 {
                            awr = awr.add_transformer(FooterGenerator::new(None, true))
                        }
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

            final_md5 = format!("{:x}", md5_hash.finalize());
            final_sha256 = format!("{:x}", sha256_hash.finalize());

            if !valid_md5.is_empty() && !valid_md5.is_empty() && final_md5 != valid_md5 {
                return Err(s3_error!(
                    InvalidDigest,
                    "Invalid or inconsistent MD5 digest"
                ));
            }

            if !final_sha256.is_empty() && !valid_sha256.is_empty() && final_sha256 != valid_sha256
            {
                return Err(s3_error!(
                    InvalidDigest,
                    "Invalid or inconsistent SHA256 digest"
                ));
            }
            if is_temp {
                self.data_handler
                    .clone()
                    .move_encode(
                        location.clone(),
                        create_location_from_hash(
                            &final_sha256,
                            &response.object_id,
                            &response.collection_id,
                            self.data_handler.settings.encrypting,
                            self.data_handler.settings.compressing,
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
                        log::error!("InternalError: {}", e);
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
                    content_length: req.input.content_length,
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
            get_only: false,
        };

        // Get or create object by path
        // Check if exists
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
            get_only: true,
        };

        // Get or create object by path
        self.data_handler
            .internal_notifier_service
            .clone() // This uses mpsc channel internally and just clones the handle -> Should be ok to clone
            .get_or_create_object_by_path(get_obj_req)
            .await
            .map_err(|e| {
                log::error!("{}", e);
                s3_error!(InternalError, "Internal notifier error")
            })?;

        // Get the encryption key from backend
        let enc_key = self
            .data_handler
            .internal_notifier_service
            .clone() // This uses mpsc channel internally and just clones the handle -> Should be ok to clone
            .get_or_create_encryption_key(GetOrCreateEncryptionKeyRequest {
                path: "".to_string(),
                endpoint_id: self.data_handler.settings.endpoint_id.to_string(),
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

                if self.data_handler.settings.encrypting {
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
            get_only: true,
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

        let parts = match req.input.multipart_upload {
            Some(parts) => parts
                .parts
                .ok_or_else(|| s3_error!(InvalidPart, "Parts must be specified")),
            None => return Err(s3_error!(InvalidPart, "Parts must be specified")),
        }?;

        let etag_parts = parts
            .into_iter()
            .map(|a| {
                Ok(PartETag {
                    part_number: a.part_number as i64,
                    etag: a
                        .e_tag
                        .ok_or_else(|| s3_error!(InvalidPart, "etag must be specified"))?,
                })
            })
            .collect::<Result<Vec<PartETag>, S3Error>>()?;

        // Does this object exists (including object id etc)
        //req.input.multipart_upload.unwrap().
        self.data_handler
            .clone()
            .finish_multipart(
                etag_parts,
                response.object_id.to_string(),
                response.collection_id,
                req.input.upload_id,
            )
            .await?;

        Ok(CompleteMultipartUploadOutput {
            e_tag: Some(response.object_id),
            version_id: Some(response.revision_number.to_string()),
            ..Default::default()
        })
    }

    async fn get_object(&self, req: S3Request<GetObjectInput>) -> S3Result<GetObjectOutput> {
        // Get the credentials
        let creds = match req.credentials {
            Some(cred) => cred,
            None => {
                log::error!("{}", "Not identified PutObjectRequest");
                return Err(s3_error!(NotSignedUp, "Your account is not signed up"));
            }
        };

        let rev_id = match req.input.version_id {
            Some(a) => a,
            None => String::new(),
        };

        let get_location_response = self
            .data_handler
            .internal_notifier_service
            .clone()
            .get_object_location(GetObjectLocationRequest {
                path: format!("s3://{}/{}", req.input.bucket, req.input.key),
                revision_id: rev_id,
                access_key: creds.access_key,
                endpoint_id: self.data_handler.settings.endpoint_id.to_string(),
            })
            .await
            .map_err(|_| s3_error!(NoSuchKey, "Key not found"))?
            .into_inner();

        let _location = get_location_response
            .location
            .ok_or_else(|| s3_error!(NoSuchKey, "Key not found"))?;

        let object = get_location_response
            .object
            .ok_or_else(|| s3_error!(NoSuchKey, "Key not found"))?;

        let sha256_hash = object
            .hashes
            .iter()
            .find(|a| a.alg == Hashalgorithm::Sha256 as i32)
            .cloned()
            .ok_or_else(|| s3_error!(NoSuchKey, "Key not found"))?;

        let (internal_sender, internal_receiver) = async_channel::bounded(10);

        let processor_clone = self.backend.clone();

        tokio::spawn(async move {
            processor_clone
                .get_object(
                    ArunaLocation {
                        bucket: sha256_hash.hash[0..2].to_string(),
                        path: sha256_hash.hash[2..].to_string(),
                        ..Default::default()
                    },
                    None,
                    internal_sender,
                )
                .await
        });

        let (final_sender, final_receiver) = async_channel::bounded(10);

        tokio::spawn(async move {
            ArunaStreamReadWriter::new_with_sink(
                internal_receiver.map(Ok),
                AsyncSenderSink::new(final_sender),
            )
            .process()
            .await
        });

        let timestamp = object
            .created
            .map(|e| {
                Timestamp::parse(
                    TimestampFormat::EpochSeconds,
                    format!("{}", e.seconds).as_str(),
                )
            })
            .ok_or_else(|| s3_error!(InternalError, "intenal processing error"))?
            .map_err(|_| s3_error!(InternalError, "intenal processing error"))?;

        Ok(GetObjectOutput {
            body: Some(StreamingBlob::wrap(
                final_receiver.map_err(|_| s3_error!(InternalError, "intenal processing error")),
            )),
            content_length: object.content_len,
            last_modified: Some(timestamp),
            e_tag: Some(object.id),
            version_id: Some(format!("{}", object.rev_number)),
            ..Default::default()
        })
    }

    async fn head_object(&self, _req: S3Request<HeadObjectInput>) -> S3Result<HeadObjectOutput> {
        Err(s3_error!(
            NotImplemented,
            "HeadObject is not implemented yet"
        ))
    }

    async fn list_objects(&self, _req: S3Request<ListObjectsInput>) -> S3Result<ListObjectsOutput> {
        Err(s3_error!(
            NotImplemented,
            "ListObjects is not implemented yet"
        ))
    }

    async fn create_bucket(
        &self,
        _req: S3Request<CreateBucketInput>,
    ) -> S3Result<CreateBucketOutput> {
        Err(s3_error!(
            NotImplemented,
            "CreateBucket is not implemented yet"
        ))
    }
}
