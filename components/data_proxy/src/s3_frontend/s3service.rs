use super::data_handler::DataHandler;
use super::utils::buffered_s3_sink::BufferedS3Sink;
use super::utils::ranges::calculate_content_length_from_range;
use super::utils::ranges::calculate_ranges;
use crate::bundler::bundle_helper::get_bundle;
use crate::caching::cache::Cache;
use crate::data_backends::storage_backend::StorageBackend;
use crate::log_received;
use crate::s3_frontend::utils::list_objects::filter_list_objects;
use crate::s3_frontend::utils::list_objects::list_response;
use crate::structs::CheckAccessResult;
use crate::structs::Object as ProxyObject;
use crate::structs::PartETag;
use crate::structs::ResourceString;
use crate::structs::TypedRelation;
use anyhow::Result;
use aruna_file::helpers::footer_parser::FooterParser;
use aruna_file::streamreadwrite::ArunaStreamReadWriter;
use aruna_file::transformer::ReadWriter;
use aruna_file::transformers::async_sender_sink::AsyncSenderSink;
use aruna_file::transformers::decrypt::ChaCha20Dec;
use aruna_file::transformers::encrypt::ChaCha20Enc;
use aruna_file::transformers::filter::Filter;
use aruna_file::transformers::footer::FooterGenerator;
use aruna_file::transformers::hashing_transformer::HashingTransformer;
use aruna_file::transformers::size_probe::SizeProbe;
use aruna_file::transformers::zstd_comp::ZstdEnc;
use aruna_file::transformers::zstd_decomp::ZstdDec;
use aruna_rust_api::api::storage::models::v2::{DataClass, Status};
use base64::engine::general_purpose;
use base64::Engine;
use chrono::Utc;
use diesel_ulid::DieselUlid;
use futures_util::TryStreamExt;
use http::HeaderValue;
use md5::{Digest, Md5};
use s3s::dto::*;
use s3s::s3_error;
use s3s::S3Error;
use s3s::S3Request;
use s3s::S3Response;
use s3s::S3Result;
use s3s::S3;
use sha2::Sha256;
use std::collections::HashMap;
use std::collections::HashSet;
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
        let data = req.extensions.get::<CheckAccessResult>().cloned();

        let mut new_object = ProxyObject::from(req.input);

        if let Some(client) = self.cache.aruna_client.read().await.as_ref() {
            let CheckAccessResult {
                user_id, token_id, ..
            } = data.ok_or_else(|| s3_error!(InternalError, "Internal Error"))?;

            let token = self
                .cache
                .auth
                .read()
                .await
                .as_ref()
                .ok_or_else(|| s3_error!(InternalError, "Missing auth handler"))?
                .sign_impersonating_token(
                    user_id.ok_or_else(|| {
                        s3_error!(NotSignedUp, "Unauthorized: Impersonating user error")
                    })?,
                    token_id,
                )
                .map_err(|e| {
                    dbg!(e);
                    s3_error!(NotSignedUp, "Unauthorized: Impersonating error")
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
        match req.input.content_length {
            Some(0) | None => {
                return Err(s3_error!(
                    MissingContentLength,
                    "Missing or invalid (0) content-length"
                ));
            }
            _ => {}
        };

        let CheckAccessResult {
            user_id,
            token_id,
            resource_ids,
            missing_resources,
            object,
            ..
        } = req
            .extensions
            .get::<CheckAccessResult>()
            .cloned()
            .ok_or_else(|| s3_error!(UnexpectedContent, "Missing data context"))?;

        dbg!(missing_resources.clone());

        let impersonating_token = if let Some(auth_handler) = self.cache.auth.read().await.as_ref()
        {
            user_id.and_then(|u| auth_handler.sign_impersonating_token(u, token_id).ok())
        } else {
            None
        };

        let res_ids =
            resource_ids.ok_or_else(|| s3_error!(InvalidArgument, "Unknown object path"))?;

        let mut object = if let Some((o, _loc)) = object {
            if o.object_type == crate::structs::ObjectType::Object {
                if o.object_status == Status::Initializing {
                    o
                } else {
                    todo!();
                    // TODO: Update object!
                }
            } else {
                let missing_object_name = missing_resources
                    .clone()
                    .ok_or_else(|| s3_error!(InvalidArgument, "Invalid object path"))?
                    .o
                    .ok_or_else(|| s3_error!(InvalidArgument, "Invalid object path"))?;

                ProxyObject {
                    id: DieselUlid::generate(),
                    name: missing_object_name.to_string(),
                    key_values: vec![],
                    object_status: Status::Initializing,
                    data_class: DataClass::Private,
                    object_type: crate::structs::ObjectType::Object,
                    hashes: HashMap::default(),
                    dynamic: false,
                    children: None,
                    parents: None,
                    synced: false,
                }
            }
        } else {
            let missing_object_name = missing_resources
                .clone()
                .ok_or_else(|| s3_error!(InvalidArgument, "Invalid object path"))?
                .o
                .ok_or_else(|| s3_error!(InvalidArgument, "Invalid object path"))?;

            ProxyObject {
                id: DieselUlid::generate(),
                name: missing_object_name.to_string(),
                key_values: vec![],
                object_status: Status::Initializing,
                data_class: DataClass::Private,
                object_type: crate::structs::ObjectType::Object,
                hashes: HashMap::default(),
                dynamic: false,
                children: None,
                parents: None,
                synced: false,
            }
        };
        let mut location = self
            .backend
            .initialize_location(&object, req.input.content_length, None, false)
            .await
            .map_err(|_| s3_error!(InternalError, "Unable to create object_location"))?;

        let (initial_sha_trans, initial_sha_recv) = HashingTransformer::new(Sha256::new());
        let (initial_md5_trans, initial_md5_recv) = HashingTransformer::new(Md5::new());
        let (initial_size_trans, initial_size_recv) = SizeProbe::new();
        let (final_sha_trans, final_sha_recv) = HashingTransformer::new(Sha256::new());
        let (final_size_trans, final_size_recv) = SizeProbe::new();

        let mut md5_initial: Option<String> = None;
        let mut sha_initial: Option<String> = None;
        let sha_final: String;
        let initial_size: u64;
        let final_size: u64;

        match req.input.body {
            Some(data) => {
                let mut awr = ArunaStreamReadWriter::new_with_sink(
                    data,
                    BufferedS3Sink::new(
                        self.backend.clone(),
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
                    awr = awr.add_transformer(ZstdEnc::new(true));
                    if location.raw_content_len > 5242880 + 80 * 28 {
                        awr = awr.add_transformer(FooterGenerator::new(None))
                    }
                }

                if let Some(enc_key) = &location.encryption_key {
                    awr = awr.add_transformer(
                        ChaCha20Enc::new(true, enc_key.to_string().into_bytes()).map_err(|e| {
                            log::error!("{}", e);
                            s3_error!(InternalError, "Internal data transformer encryption error")
                        })?,
                    );
                }

                awr = awr.add_transformer(final_sha_trans);
                awr = awr.add_transformer(final_size_trans);

                awr.process().await.map_err(|e| {
                    log::error!("{}", e);
                    s3_error!(InternalError, "Internal data transformer processing error")
                })?;
            }
            None => return Err(s3_error!(InvalidRequest, "Empty body is not allowed")),
        }

        if let Some(missing) = missing_resources {
            let collection_id = if let Some(collection) = missing.c {
                let col = ProxyObject {
                    id: DieselUlid::default(),
                    name: collection,
                    key_values: vec![],
                    object_status: Status::Available,
                    data_class: DataClass::Private,
                    object_type: crate::structs::ObjectType::Collection,
                    hashes: HashMap::default(),
                    dynamic: true,
                    children: None,
                    parents: Some(HashSet::from([TypedRelation::Project(
                        res_ids.get_project(),
                    )])),
                    synced: false,
                };

                if let Some(handler) = self.cache.aruna_client.read().await.as_ref() {
                    if let Some(token) = &impersonating_token {
                        let col = handler
                            .create_collection(col, token)
                            .await
                            .map_err(|_| s3_error!(InternalError, "Unable to create collection"))?;
                        Some(col.id)
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                res_ids.get_collection()
            };

            let dataset_id = if let Some(dataset) = missing.d {
                let parent = if let Some(collection_id) = collection_id {
                    TypedRelation::Collection(collection_id)
                } else {
                    TypedRelation::Project(res_ids.get_project())
                };

                let dataset = ProxyObject {
                    id: DieselUlid::generate(),
                    name: dataset,
                    key_values: vec![],
                    object_status: Status::Available,
                    data_class: DataClass::Private,
                    object_type: crate::structs::ObjectType::Dataset,
                    hashes: HashMap::default(),
                    dynamic: true,
                    children: None,
                    parents: Some(HashSet::from([parent])),
                    synced: false,
                };
                if let Some(handler) = self.cache.aruna_client.read().await.as_ref() {
                    if let Some(token) = &impersonating_token {
                        let dataset = handler
                            .create_dataset(dataset, token)
                            .await
                            .map_err(|_| s3_error!(InternalError, "Unable to create dataset"))?;

                        Some(dataset.id)
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                res_ids.get_dataset()
            };

            if let Some(ds_id) = dataset_id {
                object.parents = Some(HashSet::from([TypedRelation::Dataset(ds_id)]));
            } else if let Some(col_id) = collection_id {
                object.parents = Some(HashSet::from([TypedRelation::Collection(col_id)]));
            } else {
                object.parents = Some(HashSet::from([TypedRelation::Project(
                    res_ids.get_project(),
                )]));
            }

            md5_initial = Some(
                initial_md5_recv
                    .try_recv()
                    .map_err(|_| s3_error!(InternalError, "Unable to md5 hash initial data"))?,
            );
            sha_initial = Some(
                initial_sha_recv
                    .try_recv()
                    .map_err(|_| s3_error!(InternalError, "Unable to sha hash initial data"))?,
            );
            sha_final = final_sha_recv
                .try_recv()
                .map_err(|_| s3_error!(InternalError, "Unable to md5 hash final data"))?;
            initial_size = initial_size_recv
                .try_recv()
                .map_err(|_| s3_error!(InternalError, "Unable to get size"))?;
            final_size = final_size_recv
                .try_recv()
                .map_err(|_| s3_error!(InternalError, "Unable to get size"))?;

            object.hashes = vec![
                (
                    "MD5".to_string(),
                    md5_initial.clone().ok_or_else(|| {
                        s3_error!(InternalError, "Unable to get md5 hash initial data")
                    })?,
                ),
                (
                    "SHA256".to_string(),
                    sha_initial.clone().ok_or_else(|| {
                        s3_error!(InternalError, "Unable to get sha hash initial data")
                    })?,
                ),
            ]
            .into_iter()
            .collect::<HashMap<String, String>>();
            location.raw_content_len = initial_size as i64;
            location.disk_content_len = final_size as i64;
            location.disk_hash = Some(sha_final.clone());

            if let Some(handler) = self.cache.aruna_client.read().await.as_ref() {
                if let Some(token) = &impersonating_token {
                    handler
                        .create_and_finish(object.clone(), location, token)
                        .await
                        .map_err(|e| {
                            log::error!("{}", e);
                            s3_error!(InternalError, "Unable to create object (finish)")
                        })?;
                }
            }
        }
        let output = PutObjectOutput {
            e_tag: md5_initial,
            checksum_sha256: sha_initial,
            version_id: Some(object.id.to_string()),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        log_received!(&req);

        let CheckAccessResult {
            user_id,
            token_id,
            resource_ids,
            missing_resources,
            ..
        } = req
            .extensions
            .get::<CheckAccessResult>()
            .cloned()
            .ok_or_else(|| s3_error!(UnexpectedContent, "Missing data context"))?;

        let impersonating_token = if let Some(auth_handler) = self.cache.auth.read().await.as_ref()
        {
            user_id.and_then(|u| auth_handler.sign_impersonating_token(u, token_id).ok())
        } else {
            None
        };

        let res_ids =
            resource_ids.ok_or_else(|| s3_error!(InvalidArgument, "Unknown object path"))?;

        let missing_object_name = missing_resources
            .clone()
            .ok_or_else(|| s3_error!(InvalidArgument, "Invalid object path"))?
            .o
            .ok_or_else(|| s3_error!(InvalidArgument, "Invalid object path"))?;

        let mut new_object = ProxyObject {
            id: DieselUlid::generate(),
            name: missing_object_name,
            key_values: vec![],
            object_status: Status::Initializing,
            data_class: DataClass::Private,
            object_type: crate::structs::ObjectType::Object,
            hashes: HashMap::default(),
            dynamic: false,
            children: None,
            parents: None,
            synced: false,
        };

        let mut location = self
            .backend
            .initialize_location(&new_object, None, None, true)
            .await
            .map_err(|_| s3_error!(InternalError, "Unable to create object_location"))?;

        let init_response = self
            .backend
            .clone()
            .init_multipart_upload(location.clone())
            .await
            .map_err(|e| {
                log::error!("{}", e);
                s3_error!(InvalidArgument, "Unable to initialize multi-part")
            })?;

        location.upload_id = Some(init_response.to_string());

        if let Some(missing) = missing_resources {
            let collection_id = if let Some(collection) = missing.c {
                let col = ProxyObject {
                    id: DieselUlid::generate(),
                    name: collection,
                    key_values: vec![],
                    object_status: Status::Available,
                    data_class: DataClass::Private,
                    object_type: crate::structs::ObjectType::Collection,
                    hashes: HashMap::default(),
                    dynamic: true,
                    children: None,
                    parents: Some(HashSet::from([TypedRelation::Project(
                        res_ids.get_project(),
                    )])),
                    synced: false,
                };

                if let Some(handler) = self.cache.aruna_client.read().await.as_ref() {
                    if let Some(token) = &impersonating_token {
                        let col = handler
                            .create_collection(col, token)
                            .await
                            .map_err(|_| s3_error!(InternalError, "Unable to create collection"))?;
                        Some(col.id)
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                res_ids.get_collection()
            };

            let dataset_id = if let Some(dataset) = missing.d {
                let parent = if let Some(collection_id) = collection_id {
                    TypedRelation::Collection(collection_id)
                } else {
                    TypedRelation::Project(res_ids.get_project())
                };

                let dataset = ProxyObject {
                    id: DieselUlid::generate(),
                    name: dataset,
                    key_values: vec![],
                    object_status: Status::Available,
                    data_class: DataClass::Private,
                    object_type: crate::structs::ObjectType::Dataset,
                    hashes: HashMap::default(),
                    dynamic: true,
                    children: None,
                    parents: Some(HashSet::from([parent])),
                    synced: false,
                };
                
                if let Some(handler) = self.cache.aruna_client.read().await.as_ref() {
                    if let Some(token) = &impersonating_token {
                        let dataset = handler
                            .create_dataset(dataset, token)
                            .await
                            .map_err(|_| s3_error!(InternalError, "Unable to create dataset"))?;

                        Some(dataset.id)
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                res_ids.get_dataset()
            };

            if let Some(ds_id) = dataset_id {
                new_object.parents = Some(HashSet::from([TypedRelation::Dataset(ds_id)]));
            } else if let Some(col_id) = collection_id {
                new_object.parents = Some(HashSet::from([TypedRelation::Collection(col_id)]));
            } else {
                new_object.parents = Some(HashSet::from([TypedRelation::Project(
                    res_ids.get_project(),
                )]));
            }

            if let Some(handler) = self.cache.aruna_client.read().await.as_ref() {
                if let Some(token) = &impersonating_token {
                    handler
                        .create_object(new_object.clone(), Some(location), token)
                        .await
                        .map_err(|err| {
                            log::error!("Unable to create object: {:?}", err);
                            s3_error!(InternalError, "Unable to create object")
                    })?;
                }
            }
        }
        let output = CreateMultipartUploadOutput {
            key: Some(req.input.key),
            bucket: Some(req.input.bucket),
            upload_id: Some(init_response),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        log_received!(&req);

        match req.input.content_length {
            Some(0) | None => {
                return Err(s3_error!(
                    MissingContentLength,
                    "Missing or invalid (0) content-length"
                ));
            }
            _ => {}
        };

        let CheckAccessResult { object, .. } =
            req.extensions
                .get::<CheckAccessResult>()
                .cloned()
                .ok_or_else(|| s3_error!(UnexpectedContent, "Missing data context"))?;

        // If the object exists and the signatures match -> Skip the download

        let location = if let Some((_, Some(loc))) = object {
            loc
        } else {
            return Err(s3_error!(InvalidArgument, "Object not initialized"));
        };

        let etag = match req.input.body {
            Some(data) => {
                let (sink, receiver) = BufferedS3Sink::new(
                    self.backend.clone(),
                    location.clone(),
                    location.upload_id,
                    Some(req.input.part_number),
                    true,
                    None,
                    true,
                );

                let mut awr = ArunaStreamReadWriter::new_with_sink(data, sink);

                if location.compressed {
                    awr = awr.add_transformer(ZstdEnc::new(false));
                }

                if let Some(enc_key) = &location.encryption_key {
                    awr = awr.add_transformer(
                        ChaCha20Enc::new(true, enc_key.to_string().into_bytes()).map_err(|e| {
                            log::error!("{}", e);
                            s3_error!(InternalError, "Internal data transformer encryption error")
                        })?,
                    );
                }

                awr.process().await.map_err(|e| {
                    log::error!("{}", e);
                    s3_error!(InternalError, "Internal data transformer processing error")
                })?;

                if let Some(r) = receiver {
                    r.recv().await.map_err(|e| {
                        log::error!("{}", e);
                        s3_error!(InternalError, "Unable to query etag")
                    })?
                } else {
                    return Err(s3_error!(InternalError, "Unable to query etag"));
                }
            }
            None => return Err(s3_error!(InvalidRequest, "Empty body is not allowed")),
        };

        let output = UploadPartOutput {
            e_tag: Some(format!("-{}", etag)),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let CheckAccessResult {
            user_id,
            token_id,
            object,
            ..
        } = req
            .extensions
            .get::<CheckAccessResult>()
            .cloned()
            .ok_or_else(|| s3_error!(UnexpectedContent, "Missing data context"))?;

        let impersonating_token = if let Some(auth_handler) = self.cache.auth.read().await.as_ref()
        {
            user_id.and_then(|u| auth_handler.sign_impersonating_token(u, token_id).ok())
        } else {
            None
        };

        // If the object exists and the signatures match -> Skip the download

        let (object, old_location) = if let Some((object, Some(loc))) = object {
            (object, loc)
        } else {
            return Err(s3_error!(InvalidArgument, "Object not initialized"));
        };

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
                    part_number: a.part_number,
                    etag: a
                        .e_tag
                        .ok_or_else(|| s3_error!(InvalidPart, "etag must be specified"))?,
                })
            })
            .collect::<Result<Vec<PartETag>, S3Error>>()?;

        // Does this object exists (including object id etc)
        //req.input.multipart_upload.unwrap().
        self.backend
            .clone()
            .finish_multipart_upload(
                old_location.clone(),
                etag_parts,
                old_location
                    .upload_id
                    .as_ref()
                    .ok_or_else(|| s3_error!(InvalidPart, "Upload id must be specified"))?
                    .to_string(),
            )
            .await
            .map_err(|e| {
                log::error!("{}", e);
                s3_error!(InternalError, "Unable to finish upload")
            })?;

        let response = CompleteMultipartUploadOutput {
            e_tag: Some(object.id.to_string()),
            ..Default::default()
        };

        let mut new_location = self
            .backend
            .initialize_location(&object, None, None, false)
            .await
            .map_err(|_| s3_error!(InternalError, "Unable to create object_location"))?;

        let hashes =
            DataHandler::finalize_location(self.backend.clone(), &old_location, &mut new_location)
                .await
                .map_err(|e| {
                    log::error!("{}", e);
                    s3_error!(InternalError, "Unable to finalize location")
                })?;

        if let Some(handler) = self.cache.aruna_client.read().await.as_ref() {
            if let Some(token) = &impersonating_token {
                let object = handler
                    .finish_object(object.id, new_location.raw_content_len, hashes, token)
                    .await
                    .map_err(|_| s3_error!(InternalError, "Unable to create object"))?;

                // Set id of new location to object id to satisfy FK constraint
                new_location.id = object.id;

                self.cache
                    .upsert_object(object, Some(new_location))
                    .await
                    .map_err(|e| {
                        log::error! {"Upsert object error: {}", e};
                        s3_error!(InternalError, "Unable to cache object after finish")
                    })?;

                self.backend
                    .delete_object(old_location)
                    .await
                    .map_err(|e| {
                        log::error!("{}", e);
                        s3_error!(InternalError, "Unable to delete old object")
                    })?;
            }
        }

        Ok(S3Response::new(response))
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let CheckAccessResult { object, bundle, .. } = req
            .extensions
            .get::<CheckAccessResult>()
            .cloned()
            .ok_or_else(|| s3_error!(InternalError, "No context found"))?;

        if let Some(_bundle) = bundle {
            let id = match object {
                Some((obj, _)) => obj.id,
                None => return Err(s3_error!(NoSuchKey, "Object not found")),
            };
            let levels = self.cache.get_path_levels(id).map_err(|e| {
                log::error!("{}", e);
                s3_error!(InternalError, "Unable to get path levels")
            })?;

            let body = get_bundle(levels, self.backend.clone()).await;

            let mut resp = S3Response::new(GetObjectOutput {
                body,
                last_modified: None,
                e_tag: Some(format!("-{}", id)),
                ..Default::default()
            });

            resp.headers.insert(
                hyper::header::TRANSFER_ENCODING,
                HeaderValue::from_static("chunked"),
            );

            resp.headers.insert(
                hyper::header::CONTENT_TYPE,
                HeaderValue::from_static("application/octet-stream"),
            );

            return Ok(resp);
        }

        let id = match object {
            Some((obj, _)) => obj.id,
            None => return Err(s3_error!(NoSuchKey, "Object not found")),
        };

        let cache_result = self
            .cache
            .resources
            .get(&id)
            .ok_or_else(|| s3_error!(NoSuchKey, "Object not found"))?;
        let (_, location) = cache_result.value();
        let location = location
            .as_ref()
            .ok_or_else(|| s3_error!(InternalError, "Object location not found"))?
            .clone();
        let content_length = location.raw_content_len;
        let encryption_key = location
            .clone()
            .encryption_key
            .map(|k| k.as_bytes().to_vec());

        let (sender, receiver) = async_channel::bounded(10);

        // Gets 128 kb chunks (last 2)
        let footer_parser: Option<FooterParser> = if content_length > 5242880 * 28 {
            // Without encryption block because this is already checked inside
            let (footer_sender, footer_receiver) = async_channel::unbounded();
            let parser = match encryption_key.clone() {
                Some(key) => {
                    self.backend
                        .get_object(
                            location.clone(),
                            Some(format!("bytes=-{}", (65536 + 28) * 2)),
                            // "-" means last (2) chunks
                            // when encrypted + 28 for encryption information (16 bytes nonce, 12 bytes checksum)
                            // Encrypted chunk = | 16 b nonce | 65536 b data | 12 b checksum |
                            footer_sender,
                        )
                        .await
                        .map_err(|e| {
                            log::error!("{}", e);
                            s3_error!(InternalError, "Unable to get encryption_footer")
                        })?;
                    let mut output = Vec::with_capacity(130_000);
                    // Stream takes receiver chunks und them into vec
                    let mut arsw =
                        ArunaStreamReadWriter::new_with_writer(footer_receiver, &mut output);

                    // processes chunks and puts them into output
                    arsw.process().await.map_err(|e| {
                        log::error!("{}", e);
                        s3_error!(InternalError, "Unable to get footer")
                    })?;
                    drop(arsw);

                    match output.try_into() {
                        Ok(i) => match FooterParser::from_encrypted(&i, &key) {
                            Ok(p) => Some(p),
                            Err(_) => None,
                        },
                        Err(_) => None,
                    }
                }
                None => {
                    self.backend
                        .get_object(
                            location.clone(),
                            Some(format!("bytes=-{}", 65536 * 2)),
                            // when not encrypted without 28
                            footer_sender,
                        )
                        .await
                        .map_err(|e| {
                            log::error!("{}", e);
                            s3_error!(InternalError, "Unable to get compression footer")
                        })?;
                    let mut output = Vec::with_capacity(130_000);
                    let mut arsw =
                        ArunaStreamReadWriter::new_with_writer(footer_receiver, &mut output);

                    arsw.process().await.map_err(|e| {
                        log::error!("{}", e);
                        s3_error!(InternalError, "Unable to get footer")
                    })?;
                    drop(arsw);

                    match output.try_into() {
                        Ok(i) => Some(FooterParser::new(&i)),
                        Err(_) => None,
                    }
                }
            };
            parser
        } else {
            None
        };

        // Needed for final part
        let (query_ranges, filter_ranges) =
            calculate_ranges(req.input.range, content_length as u64, footer_parser)
                .map_err(|_| s3_error!(InternalError, "Error while parsing ranges"))?;
        let calc_content_len = match filter_ranges {
            Some(r) => calculate_content_length_from_range(r),
            None => content_length,
        };
        let cloned_key = encryption_key.clone();

        // Spawn get_object
        let backend = self.backend.clone();
        tokio::spawn(async move {
            backend
                .get_object(location.clone(), query_ranges, sender)
                .await
        });
        let (final_send, final_rcv) = async_channel::bounded(10);

        // Spawn final part
        tokio::spawn(async move {
            let mut asrw =
                ArunaStreamReadWriter::new_with_sink(receiver, AsyncSenderSink::new(final_send));

            if let Some(r) = filter_ranges {
                asrw = asrw.add_transformer(Filter::new(r));
            };

            asrw.add_transformer(ChaCha20Dec::new(cloned_key).map_err(|e| {
                log::error!("{}", e);
                s3_error!(InternalError, "Internal notifier error")
            })?)
            .add_transformer(ZstdDec::new())
            .process()
            .await
            .map_err(|e| {
                log::error!("{}", e);
                s3_error!(InternalError, "Internal notifier error")
            })?;

            match 1 {
                1 => Ok(()),
                _ => Err(s3_error!(InternalError, "Internal notifier error")),
            }
        });

        let body =
            Some(StreamingBlob::wrap(final_rcv.map_err(|_| {
                s3_error!(InternalError, "Internal processing error")
            })));

        Ok(S3Response::new(GetObjectOutput {
            body,
            content_length: calc_content_len,
            last_modified: None,
            e_tag: Some(format!("-{}", id)),
            version_id: None,
            ..Default::default()
        }))
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let CheckAccessResult { object, bundle, .. } = req
            .extensions
            .get::<CheckAccessResult>()
            .cloned()
            .ok_or_else(|| s3_error!(InternalError, "No context found"))?;

        dbg!((&object, &bundle));

        if let Some(_bundle) = bundle {
            let id = match object {
                Some((obj, _)) => obj.id,
                None => return Err(s3_error!(NoSuchKey, "Object not found")),
            };
            let _levels = self.cache.get_path_levels(id).map_err(|e| {
                log::error!("{}", e);
                s3_error!(InternalError, "Unable to get path levels")
            })?;

            return Ok(S3Response::new(HeadObjectOutput {
                content_length: -1,
                last_modified: Some(
                    // FIXME: Real time ...
                    time::OffsetDateTime::from_unix_timestamp(Utc::now().timestamp())
                        .unwrap()
                        .into(),
                ),
                e_tag: Some(format!("-{}", id)),
                ..Default::default()
            }));
        }

        let (object, location) = object.ok_or_else(|| s3_error!(NoSuchKey, "No object found"))?;

        let content_len = location.map(|l| l.raw_content_len).unwrap_or_default();

        Ok(S3Response::new(HeadObjectOutput {
            content_length: content_len,
            last_modified: Some(
                // FIXME: Real time ...
                time::OffsetDateTime::from_unix_timestamp(Utc::now().timestamp())
                    .unwrap()
                    .into(),
            ),
            e_tag: Some(object.id.to_string()),
            ..Default::default()
        }))
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
            Some(_) => {}
            None => return Err(s3_error!(NoSuchBucket, "No bucket found")),
        };
        let root = &req.input.bucket;

        let continuation_token = match req.input.continuation_token {
            Some(t) => {
                let decoded_token = general_purpose::STANDARD_NO_PAD
                    .decode(t)
                    .map_err(|_| s3_error!(InvalidToken, "Invalid continuation token"))?;
                let decoded_token = std::str::from_utf8(&decoded_token)
                    .map_err(|_| s3_error!(InvalidToken, "Invalid continuation token"))?
                    .to_string();
                Some(decoded_token)
            }
            None => None,
        };

        let delimiter = req.input.delimiter;
        let prefix = req.input.prefix;

        let sorted = filter_list_objects(&self.cache.paths, root);
        let start_after = match (req.input.start_after, continuation_token.clone()) {
            (Some(_), Some(ct)) => ct,
            (None, Some(ct)) => ct,
            (Some(s), None) => s,
            _ => {
                let (path, _) = sorted
                    .first_key_value()
                    .ok_or_else(|| s3_error!(NoSuchKey, "No project in tree"))?;
                path.clone()
            }
        };
        let max_keys = match req.input.max_keys {
            Some(k) if k < 1000 => k as usize,
            _ => 1000usize,
        };

        let (keys, common_prefixes, new_continuation_token) = list_response(
            sorted,
            &self.cache,
            &delimiter,
            &prefix,
            &start_after,
            max_keys,
        )
        .map_err(|_| s3_error!(NoSuchKey, "Keys not found in ListObjectsV2"))?;

        let key_count = keys.len() as i32;
        let common_prefixes = Some(
            common_prefixes
                .into_iter()
                .map(|e| CommonPrefix { prefix: Some(e) })
                .collect(),
        );
        let contents = Some(
            keys.into_iter()
                .map(|e| Object {
                    checksum_algorithm: None,
                    e_tag: Some(e.etag.to_string()),
                    key: Some(e.key),
                    last_modified: None,
                    owner: None,
                    size: e.size,
                    ..Default::default()
                })
                .collect(),
        );

        let result = ListObjectsV2Output {
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
            ..Default::default()
        };
        log::debug!("{:?}", &result);
        Ok(S3Response::new(result))
    }
}
