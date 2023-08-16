use std::collections::BTreeMap;
use super::impersonating_client::ImpersonatingClient;
use crate::caching::cache::{Cache, ResourceString};
use crate::data_backends::storage_backend::StorageBackend;
use anyhow::Result;
use s3s::dto::*;
use s3s::s3_error;
use s3s::S3Request;
use s3s::S3Response;
use s3s::S3Result;
use s3s::S3;
use std::fmt::Debug;
use std::sync::Arc;
use ahash::HashSet;
use aruna_rust_api::api::storage::models::v2::DataClass;
use base64::Engine;
use base64::engine::general_purpose;
use diesel_ulid::DieselUlid;
use crate::structs::ObjectLocation;
use crate::structs::Object;

pub struct ArunaS3Service {
    backend: Arc<Box<dyn StorageBackend>>,
    client: Arc<ImpersonatingClient>,
    cache: Arc<Cache>,
}

impl Debug for ArunaS3Service {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArunaS3Service").finish()
    }
}

impl ArunaS3Service {
    pub async fn new(
        backend: Arc<Box<dyn StorageBackend>>,
        client: Arc<ImpersonatingClient>,
        cache: Arc<Cache>,
    ) -> Result<Self> {
        Ok(ArunaS3Service {
            backend: backend.clone(),
            client,
            cache,
        })
    }
}

#[async_trait::async_trait]
impl S3 for ArunaS3Service {
    #[tracing::instrument]
    async fn complete_multipart_upload(
        &self,
        _req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        return Err(s3_error!(NotImplemented, "Not implemented yet"));
        // let mut anotif = ArunaNotifier::new(
        //     self.data_handler.internal_notifier_service.clone(),
        //     self.data_handler.settings.clone(),
        // );
        // anotif.set_credentials(_req.credentials)?;
        // anotif
        //     .get_or_create_object(&_req.input.bucket, &_req.input.key, 0)
        //     .await?;

        // let parts = match _req.input.multipart_upload {
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
        // //_req.input.multipart_upload.unwrap().
        // self.data_handler
        //     .clone()
        //     .finish_multipart(
        //         etag_parts,
        //         object_id.to_string(),
        //         collection_id,
        //         _req.input.upload_id,
        //         anotif.get_path()?,
        //     )
        //     .await?;

        // Ok(CompleteMultipartUploadOutput {
        //     e_tag: Some(object_id),
        //     version_id: Some(anotif.get_revision_string()?),
        //     ..Default::default()
        // })
    }

    async fn create_bucket(
        &self,
        _req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        Err(s3_error!(
            NotImplemented,
            "CreateBucket is not implemented yet"
        ))
    }

    #[tracing::instrument]
    async fn create_multipart_upload(
        &self,
        _req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        return Err(s3_error!(NotImplemented, "Not implemented yet"));
        // let mut anotif = ArunaNotifier::new(
        //     self.data_handler.internal_notifier_service.clone(),
        //     self.data_handler.settings.clone(),
        // );
        // anotif.set_credentials(_req.credentials)?;
        // anotif
        //     .get_or_create_object(&_req.input.bucket, &_req.input.key, 0)
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
        //     key: Some(_req.input.key),
        //     bucket: Some(_req.input.bucket),
        //     upload_id: Some(init_response),
        //     ..Default::default()
        // })
    }

    async fn get_object(
        &self,
        _req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        return Err(s3_error!(NotImplemented, "Not implemented yet"));

        // // Get the credentials
        // dbg!(_req.credentials.clone());
        // let creds = match _req.credentials {
        //     Some(cred) => cred,
        //     None => {
        //         log::error!("{}", "Not identified PutObjectRequest");
        //         return Err(s3_error!(NotSignedUp, "Your account is not signed up"));
        //     }
        // };

        // let rev_id = match _req.input.version_id {
        //     Some(a) => a,
        //     None => String::new(),
        // };

        // let get_location_response = self
        //     .data_handler
        //     .internal_notifier_service
        //     .clone()
        //     .get_object_location(GetObjectLocationRequest {
        //         path: format!("s3://{}/{}", _req.input.bucket, _req.input.key),
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

        // let path = format!("s3://{}/{}", _req.input.bucket, _req.input.key);

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
        //     calculate_ranges(_req.input.range, content_length as u64, footer_parser).map_err(
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
        _req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        return Err(s3_error!(NotImplemented, "Not implemented yet"));
        // Get the credentials

        // let creds = match _req.credentials {
        //     Some(cred) => cred,
        //     None => {
        //         log::error!("{}", "Not identified PutObjectRequest");
        //         return Err(s3_error!(NotSignedUp, "Your account is not signed up"));
        //     }
        // };

        // let rev_id = match _req.input.version_id {
        //     Some(a) => a,
        //     None => String::new(),
        // };

        // let get_location_response = self
        //     .data_handler
        //     .internal_notifier_service
        //     .clone()
        //     .get_object_location(GetObjectLocationRequest {
        //         path: format!("s3://{}/{}", _req.input.bucket, _req.input.key),
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

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        log::debug!("{:?}", &req);
        let project_name = ResourceString::Project(req.input.bucket.clone());
        match self.cache.paths.get(&project_name) {
            Some(_) => {},
            None => return Err(s3_error!(NoSuchBucket, "No bucket found"))
        };
        let root = &req.input.bucket;

        let continuation_token = match req.input.continuation_token {
            Some(t) => {
                let decoded_token =
                //TODO ERROR HANDLING
                    general_purpose::STANDARD_NO_PAD.decode(t).map_err(|_| s3_error!(NoSuchBucket, "TODO"))?;
                //TODO ERROR HANDLING
                let decoded_token = std::str::from_utf8(&decoded_token).map_err(|_| s3_error!(NoSuchBucket, "TODO"))?
                    .to_string();
                Some(decoded_token)
            }
            None => None,
        };

        let delimiter = req.input.delimiter;
        let prefix = req.input.prefix;

        let sorted  = self.cache.paths.iter().filter_map(|e| match e.key().clone() {
            ResourceString::Collection(temp_root, collection)
            if &temp_root == root => {
                Some(([temp_root, collection].join("/"), e.value().into()))
            },
            ResourceString::Dataset(temp_root, collection, dataset )
            if &temp_root == root =>
                Some(([temp_root, collection.unwrap_or("".to_string()), dataset].join("/"), e.value().into())),
            ResourceString::Object(temp_root, collection, dataset , object)
            if &temp_root == root =>
                Some(([temp_root, collection.unwrap_or("".to_string()), dataset.unwrap_or("".to_string()), object].join("/"), e.value().into())),
            ResourceString::Project(temp_root) if &temp_root == root => Some((temp_root, e.value().into())),
            _ => None,
        }).collect::<BTreeMap<String, DieselUlid>>();
        let start_after = match (req.input.start_after, continuation_token.clone()) {
            (Some(_), Some(ct)) => ct,
            (None, Some(ct)) => ct,
            (Some(s), None) => s,
            _ =>{ let (path,_ ) = sorted.first_key_value().ok_or_else(|| s3_error!(NoSuchKey, "No project in tree"))?;
                path.clone()
            }
            ,
        };
        let max_keys = match req.input.max_keys {
            Some(k) if k < 1000 => k as usize,
            _ => 1000usize,
        };

        let mut keys: HashSet<Contents> = HashSet::default();
        let mut common_prefixes: HashSet<String> = HashSet::default();
        let mut new_continuation_token: Option<String> = None;

        match (delimiter.clone(), prefix.clone()) {
            (Some(delimiter), Some(prefix)) => {
                for (idx, (path, id)) in sorted.range(start_after.clone()..).enumerate() {
                    if let Some(split) = path.strip_prefix(&prefix) {
                        if let Some((sub_prefix, _)) = split.split_once(&delimiter) {
                        // If Some split -> common prefixes
                        common_prefixes.insert([prefix.to_string(),sub_prefix.to_string()].join(""));
                        if idx == max_keys {
                            if idx == max_keys + 1{
                                new_continuation_token = Some(general_purpose::STANDARD_NO_PAD.encode(path));
                            }
                            break
                        }
                    } else {
                        let entry: Contents = (path, self.cache.resources.get(id).ok_or_else(|| s3_error!(NoSuchKey, "No key found for path"))?.value()).into();
                        keys.insert(entry);
                        if idx == max_keys {
                            if idx == max_keys + 1{
                                new_continuation_token = Some(general_purpose::STANDARD_NO_PAD.encode(path));
                            }
                            break
                        }
                    };
                    } else {
                        continue
                    };
                    if idx == max_keys {
                        if idx == max_keys + 1{
                            new_continuation_token = Some(general_purpose::STANDARD_NO_PAD.encode(path));
                        }
                        break
                    }
                }
            }
            (Some(delimiter), None) => {
                for (idx, (path, id)) in sorted.range(start_after.clone()..).enumerate() {
                    if let Some((pre, _)) = path.split_once(&delimiter) {
                        // If Some split -> common prefixes
                        common_prefixes.insert(pre.to_string());
                        if idx == max_keys {
                            if idx == max_keys + 1{
                                new_continuation_token = Some(general_purpose::STANDARD_NO_PAD.encode(path));
                            }
                            break
                        }
                    } else {
                        // If None split -> Entry
                        let entry: Contents = (path, self.cache.resources.get(id).ok_or_else(|| s3_error!(NoSuchKey, "No key found for path"))?.value()).into();
                        keys.insert(entry);
                        if idx == max_keys {
                            if idx == max_keys + 1{
                                new_continuation_token = Some(general_purpose::STANDARD_NO_PAD.encode(path));
                            }
                            break
                        }
                    };
                }
            },
            (None, Some(prefix)) => {
                for (idx, (path, id)) in sorted.range(start_after.clone()..).enumerate() {
                    let entry: Contents = if path.strip_prefix(&prefix).is_some() {
                        (path, self.cache.resources.get(id).ok_or_else(|| s3_error!(NoSuchKey, "No key found for path"))?.value()).into()
                    } else {
                        continue
                    };
                    keys.insert(entry.clone());
                    if idx == max_keys {
                        if idx == max_keys + 1{
                            new_continuation_token = Some(general_purpose::STANDARD_NO_PAD.encode(path));
                        }
                        break
                    }
                }
            },

            (None, None) => {
                for (idx, (path, id)) in sorted.range(start_after.clone()..).enumerate() {
                    let entry: Contents = (path, self.cache.resources.get(id).ok_or_else(|| s3_error!(NoSuchKey, "No key found for path"))?.value()).into();
                    keys.insert(entry.clone());
                    if idx == max_keys {
                        if idx == max_keys + 1{
                            new_continuation_token = Some(general_purpose::STANDARD_NO_PAD.encode(entry.key));
                        }
                        break
                    }
                }
            }
        }
        let key_count = keys.len() as i32;
        let common_prefixes = Some(common_prefixes.into_iter().map(|e| CommonPrefix{ prefix: Some(e)}).collect());
        let contents = Some(keys.into_iter().map(|e| s3s::dto::Object{
            checksum_algorithm: None,
            e_tag: Some(e.etag.to_string()),
            key: Some(e.key),
            last_modified: None,
            owner: None,
            size: e.size,
            storage_class: None, // TODO: Use dataclass here
        }).collect());

        let result = S3Response::new(ListObjectsV2Output{
            common_prefixes,
            contents,
            continuation_token,
            delimiter,
            encoding_type: None,
            is_truncated: new_continuation_token.is_some(),
            key_count,
            max_keys: 0,
            name: Some(root.clone()),
            next_continuation_token: new_continuation_token,
            prefix,
            start_after: Some(start_after),
        });
        Ok(result)
    }
    #[tracing::instrument]
    async fn put_object(
        &self,
        _req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        return Err(s3_error!(NotImplemented, "Not implemented yet"));
        // if _req.input.content_length == 0 {
        //     return Err(s3_error!(
        //         MissingContentLength,
        //         "Missing or invalid (0) content-length"
        //     ));
        // }

        // let mut anotif = ArunaNotifier::new(
        //     self.data_handler.internal_notifier_service.clone(),
        //     self.data_handler.settings.clone(),
        // );
        // anotif.set_credentials(_req.credentials)?;
        // anotif
        //     .get_or_create_object(&_req.input.bucket, &_req.input.key, _req.input.content_length)
        //     .await?;
        // anotif.validate_hashes(_req.input.content_md5, _req.input.checksum_sha256)?;
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
        //     match _req.input.body {
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
        //                 if _req.input.content_length > 5242880 + 80 * 28 {
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

        //             if size_counter as i64 != _req.input.content_length {
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
        //                 format!("s3://{}/{}", &_req.input.bucket, &_req.input.key),
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
        //             content_length: _req.input.content_length,
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
    async fn upload_part(
        &self,
        _req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        return Err(s3_error!(NotImplemented, "Not implemented yet"));
        // if _req.input.content_length == 0 {
        //     return Err(s3_error!(
        //         MissingContentLength,
        //         "Missing or invalid (0) content-length"
        //     ));
        // }
        // let mut anotif = ArunaNotifier::new(
        //     self.data_handler.internal_notifier_service.clone(),
        //     self.data_handler.settings.clone(),
        // );
        // anotif.set_credentials(_req.credentials)?;
        // anotif
        //     .get_or_create_object(&_req.input.bucket, &_req.input.key, 0)
        //     .await?;

        // anotif.get_encryption_key().await?;

        // let (object_id, collection_id) = anotif.get_col_obj()?;
        // let etag;

        // match _req.input.body {
        //     Some(data) => {
        //         let (sink, recv) = BufferedS3Sink::new(
        //             self.backend.clone(),
        //             ArunaLocation {
        //                 bucket: format!("{}-temp", &self.endpoint_id.to_lowercase()),
        //                 path: format!("{}/{}", collection_id, object_id),
        //                 ..Default::default()
        //             },
        //             Some(_req.input.upload_id),
        //             Some(_req.input.part_number),
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
}

// TODO: Move into utils
#[derive(Eq, PartialEq, Hash, Clone)]
struct Contents {
    pub key: String,
    pub etag: DieselUlid,
    pub size: i64,
    pub storage_class: DataClass,
}
impl From<(&String, &(Object, Option<ObjectLocation>))> for Contents {
    fn from(value: (&String, &(Object, Option<ObjectLocation>))) -> Self {
        Contents {
            key: value.0.clone(),
            etag: value.1.0.id,
            size: match &value.1.1 {
                Some(s) => s.raw_content_len,
                None => 0,
            },
            storage_class: value.1.0.data_class,
        }
    }
}
