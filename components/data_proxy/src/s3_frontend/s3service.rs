use crate::caching::cache::Cache;
use crate::data_backends::storage_backend::StorageBackend;
use crate::structs::Object as ProxyObject;
use crate::structs::ResourceIds;
use anyhow::Result;
use s3s::dto::*;
use s3s::s3_error;
use s3s::S3Request;
use s3s::S3Response;
use s3s::S3Result;
use s3s::S3;
use std::fmt::Debug;
use std::sync::Arc;

pub struct ArunaS3Service {
    backend: Arc<Box<dyn StorageBackend>>,
    cache: Arc<Cache>,
}

impl Debug for ArunaS3Service {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArunaS3Service").finish()
    }
}

impl ArunaS3Service {
    pub async fn new(backend: Arc<Box<dyn StorageBackend>>, cache: Arc<Cache>) -> Result<Self> {
        Ok(ArunaS3Service {
            backend: backend.clone(),
            cache,
        })
    }
}

#[async_trait::async_trait]
impl S3 for ArunaS3Service {
    #[tracing::instrument]
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let data = req
            .extensions
            .get::<Option<(ResourceIds, String, Option<String>)>>()
            .map(|e| e.clone())
            .flatten();

        let mut new_object = ProxyObject::from(req.input);

        if let Some(client) = self.cache.aruna_client.read().await.as_ref() {
            let (_, user, token) = data.unwrap();

            let token = self
                .cache
                .auth
                .read()
                .await
                .as_ref()
                .unwrap()
                .sign_impersonating_token(&user, token)
                .map_err(|e| {
                    dbg!(e);
                    s3_error!(NotSignedUp, "Unauthorized")
                })?;

            new_object = client
                .create_project(new_object, &token)
                .await
                .map_err(|e| {
                    dbg!(e);
                    s3_error!(InternalError, "[BACKEND] Unable to create project")
                })?;
        }
        let output = CreateBucketOutput {
            location: Some(new_object.name.to_string()),
            ..Default::default()
        };

        self.cache
            .upsert_object(new_object, None)
            .await
            .map_err(|e| {
                dbg!(e);
                s3_error!(InternalError, "Unable to cache new bucket")
            })?;

        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        return Err(s3_error!(NotImplemented, "Not implemented yet"));
        // if req.input.content_length == 0 {
        //     return Err(s3_error!(
        //         MissingContentLength,
        //         "Missing or invalid (0) content-length"
        //     ));
        // }

        // let mut anotif = ArunaNotifier::new(
        //     self.data_handler.internal_notifier_service.clone(),
        //     self.data_handler.settings.clone(),
        // );
        // anotif.set_credentials(req.credentials)?;
        // anotif
        //     .get_or_create_object(&req.input.bucket, &req.input.key, req.input.content_length)
        //     .await?;
        // anotif.validate_hashes(req.input.content_md5, req.input.checksum_sha256)?;
        // anotif.get_encryption_key().await?;

        // let hash = anotif.get_sha256();

        // let exists = match hash {
        //     Some(h) => {
        //         if !h.is_empty() && h.len() == 32 {
        //             self.backend
        //                 .head_object(ArunaLocation {
        //                     bucket: format!("{}-{}", &self.endpoint_id.to_lowercase(), &h[0..2]),
        //                     path: h[2..].to_string(),
        //                     ..Default::default()
        //                 })
        //                 .await
        //                 .is_ok()
        //         } else {
        //             false
        //         }
        //     }
        //     None => false,
        // };

        // let (location, is_temp) = anotif.get_location(exists)?;

        // let mut md5_hash = Md5::new();
        // let mut sha256_hash = Sha256::new();
        // let mut final_md5 = String::new();
        // let mut final_sha256 = String::new();
        // let mut size_counter = 0;
        // // If the object exists and the signatures match -> Skip the download

        // if !exists {
        //     match req.input.body {
        //         Some(data) => {
        //             // MD5 Stream
        //             let md5ed_stream = data.inspect_ok(|bytes| md5_hash.update(bytes.as_ref()));
        //             // Sha256 stream
        //             let shaed_stream =
        //                 md5ed_stream.inspect_ok(|bytes| sha256_hash.update(bytes.as_ref()));

        //             let sized_stream = shaed_stream.inspect_ok(|by| size_counter += by.len());

        //             let mut awr = ArunaStreamReadWriter::new_with_sink(
        //                 sized_stream,
        //                 BufferedS3Sink::new(
        //                     self.backend.clone(),
        //                     location.clone(),
        //                     None,
        //                     None,
        //                     false,
        //                     None,
        //                 )
        //                 .0,
        //             );

        //             if location.is_compressed {
        //                 awr = awr.add_transformer(ZstdEnc::new(true));
        //                 if req.input.content_length > 5242880 + 80 * 28 {
        //                     awr = awr.add_transformer(FooterGenerator::new(None))
        //                 }
        //             }

        //             if location.is_encrypted {
        //                 awr = awr.add_transformer(
        //                     ChaCha20Enc::new(true, anotif.retrieve_enc_key()?).map_err(|e| {
        //                         log::error!("{}", e);
        //                         s3_error!(
        //                             InternalError,
        //                             "Internal data transformer encryption error"
        //                         )
        //                     })?,
        //                 );
        //             }

        //             awr.process().await.map_err(|e| {
        //                 log::error!("{}", e);
        //                 s3_error!(InternalError, "Internal data transformer processing error")
        //             })?;

        //             if size_counter as i64 != req.input.content_length {
        //                 self.backend.delete_object(location).await.map_err(|e| {
        //                     log::error!(
        //                         "PUT: Unable to delete object, after wrong content_len: {}",
        //                         e
        //                     );
        //                     s3_error!(InternalError, "PUT: Unable to delete object")
        //                 })?;
        //                 return Err(s3_error!(
        //                     UnexpectedContent,
        //                     "Content length does not match"
        //                 ));
        //             }
        //         }
        //         None => {
        //             return Err(s3_error!(
        //                 InvalidObjectState,
        //                 "Request body / data is required, use ArunaAPI for empty objects"
        //             ))
        //         }
        //     }

        //     final_md5 = format!("{:x}", md5_hash.finalize());
        //     final_sha256 = format!("{:x}", sha256_hash.finalize());

        //     let hashes_is_ok = anotif.test_final_hashes(&final_md5, &final_sha256)?;

        //     if !hashes_is_ok {
        //         self.backend.delete_object(location).await.map_err(|e| {
        //             log::error!("PUT: Unable to delete object, after wrong hash: {}", e);
        //             s3_error!(InternalError, "PUT: Unable to delete object")
        //         })?;
        //         return Err(s3_error!(InvalidDigest, "Invalid hash digest"));
        //     };
        //     if is_temp {
        //         let (object_id, collection_id) = anotif.get_col_obj()?;
        //         self.data_handler
        //             .clone()
        //             .move_encode(
        //                 location.clone(),
        //                 create_location_from_hash(
        //                     &final_sha256,
        //                     &object_id,
        //                     &collection_id,
        //                     self.data_handler.settings.encrypting,
        //                     self.data_handler.settings.compressing,
        //                     location.encryption_key.clone(),
        //                     self.data_handler.settings.endpoint_id.to_string(),
        //                     exists,
        //                 )
        //                 .0,
        //                 object_id,
        //                 collection_id,
        //                 Some(vec![
        //                     Hash {
        //                         alg: Hashalgorithm::Md5 as i32,
        //                         hash: final_md5.clone(),
        //                     },
        //                     Hash {
        //                         alg: Hashalgorithm::Sha256 as i32,
        //                         hash: final_sha256.clone(),
        //                     },
        //                 ]),
        //                 format!("s3://{}/{}", &req.input.bucket, &req.input.key),
        //             )
        //             .await
        //             .map_err(|e| {
        //                 log::error!("InternalError: {}", e);
        //                 s3_error!(InternalError, "Internal data mover error")
        //             })?
        //     }
        // }

        // if !is_temp {
        //     let (object_id, collection_id) = anotif.get_col_obj()?;
        //     self.data_handler
        //         .internal_notifier_service
        //         .clone() // This uses mpsc channel internally and just clones the handle -> Should be ok to clone
        //         .finalize_object(FinalizeObjectRequest {
        //             object_id,
        //             collection_id,
        //             location: Some(location),
        //             content_length: req.input.content_length,
        //             hashes: vec![
        //                 Hash {
        //                     alg: Hashalgorithm::Md5 as i32,
        //                     hash: final_md5,
        //                 },
        //                 Hash {
        //                     alg: Hashalgorithm::Sha256 as i32,
        //                     hash: final_sha256.to_string(),
        //                 },
        //             ],
        //         })
        //         .await
        //         .map_err(|e| {
        //             log::error!("{}", e);
        //             s3_error!(InternalError, "Internal aruna error")
        //         })?;
        // }

        // let (object_id, _) = anotif.get_col_obj()?;
        // let output = PutObjectOutput {
        //     e_tag: Some(format!("-{}", object_id)),
        //     checksum_sha256: Some(final_sha256),
        //     ..Default::default()
        // };
        // Ok(output)
    }

    #[tracing::instrument]
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        return Err(s3_error!(NotImplemented, "Not implemented yet"));
        // let mut anotif = ArunaNotifier::new(
        //     self.data_handler.internal_notifier_service.clone(),
        //     self.data_handler.settings.clone(),
        // );
        // anotif.set_credentials(req.credentials)?;
        // anotif
        //     .get_or_create_object(&req.input.bucket, &req.input.key, 0)
        //     .await?;

        // let (object_id, collection_id) = anotif.get_col_obj()?;

        // let init_response = self
        //     .backend
        //     .clone()
        //     .init_multipart_upload(ArunaLocation {
        //         bucket: format!("{}-temp", self.endpoint_id.to_lowercase()),
        //         path: format!("{}/{}", collection_id, object_id),
        //         ..Default::default()
        //     })
        //     .await
        //     .map_err(|e| {
        //         log::error!("{}", e);
        //         s3_error!(InvalidArgument, "Unable to initialize multi-part")
        //     })?;

        // Ok(CreateMultipartUploadOutput {
        //     key: Some(req.input.key),
        //     bucket: Some(req.input.bucket),
        //     upload_id: Some(init_response),
        //     ..Default::default()
        // })
    }

    #[tracing::instrument]
    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        return Err(s3_error!(NotImplemented, "Not implemented yet"));
        // if req.input.content_length == 0 {
        //     return Err(s3_error!(
        //         MissingContentLength,
        //         "Missing or invalid (0) content-length"
        //     ));
        // }
        // let mut anotif = ArunaNotifier::new(
        //     self.data_handler.internal_notifier_service.clone(),
        //     self.data_handler.settings.clone(),
        // );
        // anotif.set_credentials(req.credentials)?;
        // anotif
        //     .get_or_create_object(&req.input.bucket, &req.input.key, 0)
        //     .await?;

        // anotif.get_encryption_key().await?;

        // let (object_id, collection_id) = anotif.get_col_obj()?;
        // let etag;

        // match req.input.body {
        //     Some(data) => {
        //         let (sink, recv) = BufferedS3Sink::new(
        //             self.backend.clone(),
        //             ArunaLocation {
        //                 bucket: format!("{}-temp", &self.endpoint_id.to_lowercase()),
        //                 path: format!("{}/{}", collection_id, object_id),
        //                 ..Default::default()
        //             },
        //             Some(req.input.upload_id),
        //             Some(req.input.part_number),
        //             true,
        //             None,
        //         );
        //         let mut awr = ArunaStreamReadWriter::new_with_sink(data.into_stream(), sink);

        //         if self.data_handler.settings.encrypting {
        //             awr = awr.add_transformer(
        //                 ChaCha20Enc::new(true, anotif.retrieve_enc_key()?).map_err(|e| {
        //                     log::error!("{}", e);
        //                     s3_error!(InternalError, "Internal data transformer encryption error")
        //                 })?,
        //             );
        //         }

        //         awr.process().await.map_err(|e| {
        //             log::error!("Processing error: {}", e);
        //             s3_error!(InternalError, "Internal data transformer processing error")
        //         })?;

        //         etag = recv
        //             .try_recv()
        //             .map_err(|_| s3_error!(InternalError, "Unable to get etag"))?;
        //     }
        //     _ => return Err(s3_error!(InvalidPart, "MultiPart cannot be empty")),
        // };

        // Ok(UploadPartOutput {
        //     e_tag: Some(format!("-{}", etag)),
        //     ..Default::default()
        // })
    }

    #[tracing::instrument]
    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        return Err(s3_error!(NotImplemented, "Not implemented yet"));
        // let mut anotif = ArunaNotifier::new(
        //     self.data_handler.internal_notifier_service.clone(),
        //     self.data_handler.settings.clone(),
        // );
        // anotif.set_credentials(req.credentials)?;
        // anotif
        //     .get_or_create_object(&req.input.bucket, &req.input.key, 0)
        //     .await?;

        // let parts = match req.input.multipart_upload {
        //     Some(parts) => parts
        //         .parts
        //         .ok_or_else(|| s3_error!(InvalidPart, "Parts must be specified")),
        //     None => return Err(s3_error!(InvalidPart, "Parts must be specified")),
        // }?;

        // let etag_parts = parts
        //     .into_iter()
        //     .map(|a| {
        //         Ok(PartETag {
        //             part_number: a.part_number as i64,
        //             etag: a
        //                 .e_tag
        //                 .ok_or_else(|| s3_error!(InvalidPart, "etag must be specified"))?,
        //         })
        //     })
        //     .collect::<Result<Vec<PartETag>, S3Error>>()?;

        // let (object_id, collection_id) = anotif.get_col_obj()?;
        // // Does this object exists (including object id etc)
        // //req.input.multipart_upload.unwrap().
        // self.data_handler
        //     .clone()
        //     .finish_multipart(
        //         etag_parts,
        //         object_id.to_string(),
        //         collection_id,
        //         req.input.upload_id,
        //         anotif.get_path()?,
        //     )
        //     .await?;

        // Ok(CompleteMultipartUploadOutput {
        //     e_tag: Some(object_id),
        //     version_id: Some(anotif.get_revision_string()?),
        //     ..Default::default()
        // })
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        return Err(s3_error!(NotImplemented, "Not implemented yet"));

        // // Get the credentials
        // dbg!(req.credentials.clone());
        // let creds = match req.credentials {
        //     Some(cred) => cred,
        //     None => {
        //         log::error!("{}", "Not identified PutObjectRequest");
        //         return Err(s3_error!(NotSignedUp, "Your account is not signed up"));
        //     }
        // };

        // let rev_id = match req.input.version_id {
        //     Some(a) => a,
        //     None => String::new(),
        // };

        // let get_location_response = self
        //     .data_handler
        //     .internal_notifier_service
        //     .clone()
        //     .get_object_location(GetObjectLocationRequest {
        //         path: format!("s3://{}/{}", req.input.bucket, req.input.key),
        //         revision_id: rev_id,
        //         access_key: creds.access_key,
        //         endpoint_id: self.data_handler.settings.endpoint_id.to_string(),
        //     })
        //     .await
        //     .map_err(|_| s3_error!(NoSuchKey, "Key not found, getlocation"))?
        //     .into_inner();

        // let _location = get_location_response
        //     .location
        //     .ok_or_else(|| s3_error!(NoSuchKey, "Key not found, location"))?;

        // let object = get_location_response
        //     .object
        //     .clone()
        //     .ok_or_else(|| s3_error!(NoSuchKey, "Key not found, object"))?;

        // let sha256_hash = object
        //     .hashes
        //     .iter()
        //     .find(|a| a.alg == Hashalgorithm::Sha256 as i32)
        //     .cloned()
        //     .ok_or_else(|| s3_error!(NoSuchKey, "Key not found"))?;

        // if sha256_hash.hash.is_empty() {
        //     return Err(s3_error!(InternalError, "Aruna returned empty signature"));
        // }

        // let (internal_sender, internal_receiver) = async_channel::bounded(10);

        // let processor_clone = self.backend.clone();

        // let sha_clone = sha256_hash.hash.clone();

        // let content_length = get_location_response
        //     .object
        //     .clone()
        //     .ok_or_else(|| s3_error!(NoSuchKey, "Key not found"))?
        //     .content_len;

        // let get_location = ArunaLocation {
        //     bucket: format!(
        //         "{}-{}",
        //         &self.endpoint_id.to_lowercase(),
        //         &sha256_hash.hash[0..2]
        //     ),
        //     path: sha256_hash.hash[2..].to_string(),
        //     ..Default::default()
        // };

        // let setting = self.data_handler.settings.clone();

        // let path = format!("s3://{}/{}", req.input.bucket, req.input.key);

        // let encryption_key = self
        //     .data_handler
        //     .internal_notifier_service // This uses mpsc channel internally and just clones the handle -> Should be ok to clone
        //     .clone()
        //     .get_or_create_encryption_key(GetOrCreateEncryptionKeyRequest {
        //         path,
        //         endpoint_id: setting.endpoint_id.to_string(),
        //         hash: sha_clone,
        //     })
        //     .await
        //     .map_err(|e| {
        //         log::error!("{}", e);
        //         s3_error!(InternalError, "Internal notifier error")
        //     })?
        //     .into_inner()
        //     .encryption_key
        //     .as_bytes()
        //     .to_vec();

        // let footer_parser: Option<FooterParser> = if content_length > 5242880 + 80 * 28 {
        //     let (footer_sender, footer_receiver) = async_channel::unbounded();
        //     self.backend
        //         .get_object(
        //             get_location.clone(),
        //             Some(format!("bytes=-{}", (65536 + 28) * 2)),
        //             footer_sender,
        //         )
        //         .await
        //         .map_err(|e| {
        //             log::error!("{}", e);
        //             s3_error!(InternalError, "Unable to get encryption_key")
        //         })?;

        //     let mut output = Vec::with_capacity(130_000);

        //     let mut arsw = ArunaStreamReadWriter::new_with_writer(footer_receiver, &mut output);

        //     arsw.process().await.map_err(|e| {
        //         log::error!("{}", e);
        //         s3_error!(InternalError, "Unable to get footer")
        //     })?;
        //     drop(arsw);

        //     match output.try_into() {
        //         Ok(i) => match FooterParser::from_encrypted(&i, &encryption_key) {
        //             Ok(p) => Some(p),
        //             Err(_) => None,
        //         },
        //         Err(_) => None,
        //     }
        // } else {
        //     None
        // };

        // let (query_range, filter_ranges) =
        //     calculate_ranges(req.input.range, content_length as u64, footer_parser).map_err(
        //         |e| {
        //             log::error!("{}", e);
        //             s3_error!(InternalError, "Unable to build FooterParser")
        //         },
        //     )?;

        // let calc_content_len = match filter_ranges {
        //     Some(r) => calculate_content_length_from_range(r),
        //     None => object.content_len,
        // };

        // tokio::spawn(async move {
        //     processor_clone
        //         .get_object(get_location, query_range, internal_sender)
        //         .await
        // });

        // let (final_sender, final_receiver) = async_channel::bounded(10);

        // tokio::spawn(async move {
        //     let mut asrw = ArunaStreamReadWriter::new_with_sink(
        //         internal_receiver,
        //         AsyncSenderSink::new(final_sender),
        //     );

        //     if let Some(r) = filter_ranges {
        //         asrw = asrw.add_transformer(Filter::new(r));
        //     };

        //     asrw.add_transformer(ChaCha20Dec::new(Some(encryption_key)).map_err(|e| {
        //         log::error!("{}", e);
        //         s3_error!(InternalError, "Internal notifier error")
        //     })?)
        //     .add_transformer(ZstdDec::new())
        //     .process()
        //     .await
        //     .map_err(|e| {
        //         log::error!("{}", e);
        //         s3_error!(InternalError, "Internal notifier error")
        //     })?;

        //     match 1 {
        //         1 => Ok(()),
        //         _ => Err(s3_error!(InternalError, "Internal notifier error")),
        //     }
        // });

        // let timestamp = object
        //     .created
        //     .map(|e| {
        //         Timestamp::parse(
        //             TimestampFormat::EpochSeconds,
        //             format!("{}", e.seconds).as_str(),
        //         )
        //     })
        //     .ok_or_else(|| s3_error!(InternalError, "intenal processing error"))?
        //     .map_err(|_| s3_error!(InternalError, "intenal processing error"))?;

        // let body =
        //     Some(StreamingBlob::wrap(final_receiver.map_err(|_| {
        //         s3_error!(InternalError, "intenal processing error")
        //     })));

        // Ok(GetObjectOutput {
        //     body,
        //     content_length: calc_content_len,
        //     last_modified: Some(timestamp),
        //     e_tag: Some(format!("-{}", object.id)),
        //     version_id: Some(format!("{}", object.rev_number)),
        //     ..Default::default()
        // })
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        return Err(s3_error!(NotImplemented, "Not implemented yet"));
        // Get the credentials

        // let creds = match req.credentials {
        //     Some(cred) => cred,
        //     None => {
        //         log::error!("{}", "Not identified PutObjectRequest");
        //         return Err(s3_error!(NotSignedUp, "Your account is not signed up"));
        //     }
        // };

        // let rev_id = match req.input.version_id {
        //     Some(a) => a,
        //     None => String::new(),
        // };

        // let get_location_response = self
        //     .data_handler
        //     .internal_notifier_service
        //     .clone()
        //     .get_object_location(GetObjectLocationRequest {
        //         path: format!("s3://{}/{}", req.input.bucket, req.input.key),
        //         revision_id: rev_id,
        //         access_key: creds.access_key,
        //         endpoint_id: self.data_handler.settings.endpoint_id.to_string(),
        //     })
        //     .await
        //     .map_err(|_| s3_error!(NoSuchKey, "Key not found, tag: head_get_loc"))?
        //     .into_inner();

        // let _location = get_location_response
        //     .location
        //     .ok_or_else(|| s3_error!(NoSuchKey, "Key not found, tag: head_loc"))?;

        // let object = get_location_response
        //     .object
        //     .ok_or_else(|| s3_error!(NoSuchKey, "Key not found, tag: head_obj"))?;

        // let sha256_hash = object
        //     .hashes
        //     .iter()
        //     .find(|a| a.alg == Hashalgorithm::Sha256 as i32)
        //     .cloned()
        //     .ok_or_else(|| s3_error!(NoSuchKey, "Key not found, tag: head_sha"))?;

        // let timestamp = object
        //     .created
        //     .map(|e| {
        //         Timestamp::parse(
        //             TimestampFormat::EpochSeconds,
        //             format!("{}", e.seconds).as_str(),
        //         )
        //     })
        //     .ok_or_else(|| s3_error!(InternalError, "intenal processing error"))?
        //     .map_err(|_| s3_error!(InternalError, "intenal processing error"))?;

        // Ok(HeadObjectOutput {
        //     content_length: object.content_len,
        //     last_modified: Some(timestamp),
        //     checksum_sha256: Some(sha256_hash.hash),
        //     e_tag: Some(object.id),
        //     version_id: Some(format!("{}", object.rev_number)),
        //     ..Default::default()
        // })
    }

    async fn list_objects(
        &self,
        _req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        Err(s3_error!(
            NotImplemented,
            "ListObjects is not implemented yet"
        ))
    }
}
