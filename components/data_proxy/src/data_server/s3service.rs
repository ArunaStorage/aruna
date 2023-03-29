use anyhow::Result;
use aruna_file::streamreadwrite::ArunaStreamReadWriter;
use aruna_file::transformers::async_sender_sink::AsyncSenderSink;
use aruna_file::transformers::compressor::ZstdEnc;
use aruna_file::transformers::decompressor::ZstdDec;
use aruna_file::transformers::decrypt::ChaCha20Dec;
use aruna_file::transformers::encrypt::ChaCha20Enc;
use aruna_file::transformers::footer::FooterGenerator;
use aruna_rust_api::api::internal::v1::FinalizeObjectRequest;
use aruna_rust_api::api::internal::v1::GetObjectLocationRequest;
use aruna_rust_api::api::internal::v1::GetOrCreateEncryptionKeyRequest;
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
use crate::data_server::utils::buffered_s3_sink::BufferedS3Sink;

use super::data_handler::DataHandler;
use super::utils::aruna_notifier::ArunaNotifier;
use super::utils::buffered_s3_sink::parse_notes_get_etag;
use crate::data_server::utils::utils::create_location_from_hash;

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
        let mut anotif = ArunaNotifier::new(
            self.data_handler.internal_notifier_service.clone(),
            self.data_handler.settings.clone(),
        );
        anotif.set_credentials(req.credentials)?;
        anotif
            .get_object(&req.input.bucket, &req.input.key, req.input.content_length)
            .await?;
        anotif.validate_hashes(req.input.content_md5, req.input.checksum_sha256)?;
        anotif.get_encryption_key().await?;

        let (location, is_temp) = anotif.get_location()?;

        let mut md5_hash = Md5::new();
        let mut sha256_hash = Sha256::new();

        let resp = if !is_temp {
            // Check if this object already exists
            match self.backend.head_object(location.clone()).await {
                Ok(_) => Some(()),
                Err(_) => None,
            }
        } else {
            None
        };

        let mut final_md5 = String::new();
        let mut final_sha256 = String::new();

        // If the object exists and the signatures match -> Skip the download

        match resp {
            Some(_) => {}
            None => {
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
                                false,
                                None,
                            ),
                        );

                        if self.data_handler.settings.encrypting {
                            awr = awr.add_transformer(
                                ChaCha20Enc::new(true, anotif.retrieve_enc_key()?).map_err(
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

                anotif.test_final_hashes(&final_md5, &final_sha256)?;

                if is_temp {
                    let (object_id, collection_id) = anotif.get_col_obj()?;
                    self.data_handler
                        .clone()
                        .move_encode(
                            location.clone(),
                            create_location_from_hash(
                                &final_sha256,
                                &object_id,
                                &collection_id,
                                self.data_handler.settings.encrypting,
                                self.data_handler.settings.compressing,
                                location.encryption_key.clone(),
                            )
                            .0,
                            object_id,
                            collection_id,
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
                            format!("s3://{}/{}", &req.input.bucket, &req.input.key),
                        )
                        .await
                        .map_err(|e| {
                            log::error!("InternalError: {}", e);
                            s3_error!(InternalError, "Internal data mover error")
                        })?
                }
            }
        }

        if !is_temp {
            let (object_id, collection_id) = anotif.get_col_obj()?;
            self.data_handler
                .internal_notifier_service
                .clone() // This uses mpsc channel internally and just clones the handle -> Should be ok to clone
                .finalize_object(FinalizeObjectRequest {
                    object_id: object_id,
                    collection_id: collection_id,
                    location: Some(location),
                    content_length: req.input.content_length,
                    hashes: vec![
                        Hash {
                            alg: Hashalgorithm::Md5 as i32,
                            hash: final_md5,
                        },
                        Hash {
                            alg: Hashalgorithm::Sha256 as i32,
                            hash: final_sha256.to_string(),
                        },
                    ],
                })
                .await
                .map_err(|e| {
                    log::error!("{}", e);
                    s3_error!(InternalError, "Internal aruna error")
                })?;
        }

        let (object_id, _) = anotif.get_col_obj()?;
        let output = PutObjectOutput {
            e_tag: Some(object_id),
            checksum_sha256: Some(final_sha256),
            ..Default::default()
        };
        Ok(output)
    }

    #[tracing::instrument]
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<CreateMultipartUploadOutput> {
        let mut anotif = ArunaNotifier::new(
            self.data_handler.internal_notifier_service.clone(),
            self.data_handler.settings.clone(),
        );
        anotif.set_credentials(req.credentials)?;
        anotif
            .get_object(&req.input.bucket, &req.input.key, 0)
            .await?;

        let (object_id, collection_id) = anotif.get_col_obj()?;

        let init_response = self
            .backend
            .clone()
            .init_multipart_upload(ArunaLocation {
                bucket: "temp".to_string(),
                path: format!("{}/{}", collection_id, object_id),
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
        let mut anotif = ArunaNotifier::new(
            self.data_handler.internal_notifier_service.clone(),
            self.data_handler.settings.clone(),
        );
        anotif.set_credentials(req.credentials)?;
        anotif
            .get_object(&req.input.bucket, &req.input.key, 0)
            .await?;

        anotif.get_encryption_key().await?;

        let (object_id, collection_id) = anotif.get_col_obj()?;
        let etag;

        match req.input.body {
            Some(data) => {
                let mut awr = ArunaStreamReadWriter::new_with_sink(
                    data.into_stream(),
                    BufferedS3Sink::new(
                        self.backend.clone(),
                        ArunaLocation {
                            bucket: "temp".to_string(),
                            path: format!("{}/{}", collection_id, object_id),
                            ..Default::default()
                        },
                        Some(req.input.upload_id),
                        Some(req.input.part_number),
                        true,
                        None,
                    ),
                );

                if self.data_handler.settings.encrypting {
                    awr = awr.add_transformer(
                        ChaCha20Enc::new(true, anotif.retrieve_enc_key()?).map_err(|e| {
                            log::error!("{}", e);
                            s3_error!(InternalError, "Internal data transformer encryption error")
                        })?,
                    );
                }

                awr.process().await.map_err(|e| {
                    log::error!("Processing error: {}", e);
                    s3_error!(InternalError, "Internal data transformer processing error")
                })?;

                etag = parse_notes_get_etag(awr.query_notifications().await.map_err(|e| {
                    log::error!("Processing error: {}", e);
                    s3_error!(InternalError, "ETagError")
                })?)
                .map_err(|e| {
                    log::error!("Processing error: {}", e);
                    s3_error!(InternalError, "ETagError")
                })?;
            }
            _ => return Err(s3_error!(InvalidPart, "MultiPart cannot be empty")),
        };

        Ok(UploadPartOutput {
            e_tag: Some(format!("-{}", etag)),
            ..Default::default()
        })
    }

    #[tracing::instrument]
    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<CompleteMultipartUploadOutput> {
        let mut anotif = ArunaNotifier::new(
            self.data_handler.internal_notifier_service.clone(),
            self.data_handler.settings.clone(),
        );
        anotif.set_credentials(req.credentials)?;
        anotif
            .get_object(&req.input.bucket, &req.input.key, 0)
            .await?;

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

        let (object_id, collection_id) = anotif.get_col_obj()?;
        // Does this object exists (including object id etc)
        //req.input.multipart_upload.unwrap().
        self.data_handler
            .clone()
            .finish_multipart(
                etag_parts,
                object_id.to_string(),
                collection_id,
                req.input.upload_id,
                anotif.get_path()?,
            )
            .await?;

        Ok(CompleteMultipartUploadOutput {
            e_tag: Some(object_id),
            version_id: Some(anotif.get_revision_string()?),
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
            .map_err(|_| s3_error!(NoSuchKey, "Key not found, getlocation"))?
            .into_inner();

        let _location = get_location_response
            .location
            .ok_or_else(|| s3_error!(NoSuchKey, "Key not found, location"))?;

        let object = get_location_response
            .object
            .ok_or_else(|| s3_error!(NoSuchKey, "Key not found, object"))?;

        let sha256_hash = object
            .hashes
            .iter()
            .find(|a| a.alg == Hashalgorithm::Sha256 as i32)
            .cloned()
            .ok_or_else(|| s3_error!(NoSuchKey, "Key not found"))?;

        if sha256_hash.hash.is_empty() {
            return Err(s3_error!(InternalError, "Aruna returned empty signature"));
        }

        let (internal_sender, internal_receiver) = async_channel::bounded(10);

        let processor_clone = self.backend.clone();

        let sha_clone = sha256_hash.hash.clone();

        let get_clone = tokio::spawn(async move {
            processor_clone
                .get_object(
                    ArunaLocation {
                        bucket: format!("b{}", sha256_hash.hash[0..2].to_string()),
                        path: sha256_hash.hash[2..].to_string(),
                        ..Default::default()
                    },
                    None,
                    internal_sender,
                )
                .await
        });

        let (final_sender, final_receiver) = async_channel::unbounded();

        let mut dhandler_service = self.data_handler.internal_notifier_service.clone();
        let setting = self.data_handler.settings.clone();

        let path = format!("s3://{}/{}", req.input.bucket, req.input.key);

        tokio::spawn(async move {
            ArunaStreamReadWriter::new_with_sink(
                internal_receiver.map(Ok),
                AsyncSenderSink::new(final_sender),
            )
            .add_transformer(ZstdDec::new())
            .add_transformer(
                ChaCha20Dec::new(
                    dhandler_service // This uses mpsc channel internally and just clones the handle -> Should be ok to clone
                        .get_or_create_encryption_key(GetOrCreateEncryptionKeyRequest {
                            path,
                            endpoint_id: setting.endpoint_id.to_string(),
                            hash: sha_clone,
                        })
                        .await
                        .map_err(|e| {
                            log::error!("{}", e);
                            s3_error!(InternalError, "Internal notifier error")
                        })?
                        .into_inner()
                        .encryption_key
                        .as_bytes()
                        .to_vec(),
                )
                .map_err(|e| {
                    log::error!("{}", e);
                    s3_error!(InternalError, "Internal notifier error")
                })?,
            )
            .process()
            .await
        })
        .await
        .map_err(|e| {
            log::error!("{}", e);
            s3_error!(InternalError, "Internal notifier error")
        })?
        .map_err(|e| {
            log::error!("{}", e);
            s3_error!(InternalError, "Internal notifier error")
        })?;

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

        get_clone
            .await
            .map_err(|e| {
                log::error!("{}", e);
                s3_error!(InternalError, "Internal notifier error")
            })?
            .map_err(|e| {
                log::error!("{}", e);
                s3_error!(InternalError, "Internal notifier error")
            })?;

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

    async fn head_object(&self, req: S3Request<HeadObjectInput>) -> S3Result<HeadObjectOutput> {
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
            .map_err(|_| s3_error!(NoSuchKey, "Key not found, tag: head_get_loc"))?
            .into_inner();

        let _location = get_location_response
            .location
            .ok_or_else(|| s3_error!(NoSuchKey, "Key not found, tag: head_loc"))?;

        let object = get_location_response
            .object
            .ok_or_else(|| s3_error!(NoSuchKey, "Key not found, tag: head_obj"))?;

        let sha256_hash = object
            .hashes
            .iter()
            .find(|a| a.alg == Hashalgorithm::Sha256 as i32)
            .cloned()
            .ok_or_else(|| s3_error!(NoSuchKey, "Key not found, tag: head_sha"))?;

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

        Ok(HeadObjectOutput {
            content_length: object.content_len,
            last_modified: Some(timestamp),
            checksum_sha256: Some(sha256_hash.hash),
            e_tag: Some(object.id),
            version_id: Some(format!("{}", object.rev_number)),
            ..Default::default()
        })
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
