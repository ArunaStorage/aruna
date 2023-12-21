use super::data_handler::DataHandler;
use super::utils::buffered_s3_sink::BufferedS3Sink;
use super::utils::ranges::calculate_content_length_from_range;
use super::utils::ranges::calculate_ranges;
use crate::bundler::bundle_helper::get_bundle;
use crate::caching::cache::Cache;
use crate::data_backends::storage_backend::StorageBackend;
use crate::s3_frontend::utils::list_objects::filter_list_objects;
use crate::s3_frontend::utils::list_objects::list_response;
use crate::s3_frontend::utils::ranges::aruna_range_from_s3range;
use crate::structs::CheckAccessResult;
use crate::structs::Object as ProxyObject;
use crate::structs::PartETag;
use crate::structs::ResourceString;
use crate::structs::TypedRelation;
use crate::structs::ALL_RIGHTS_RESERVED;
use crate::trace_err;
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
use aruna_rust_api::api::storage::models::v2::Hash;
use aruna_rust_api::api::storage::models::v2::Hashalgorithm;
use aruna_rust_api::api::storage::models::v2::{DataClass, Status};
use base64::engine::general_purpose;
use base64::Engine;
use chrono::Utc;
use diesel_ulid::DieselUlid;
use futures_util::TryStreamExt;
use http::HeaderName;
use http::HeaderValue;
use md5::{Digest, Md5};
use s3s::dto::*;
use s3s::s3_error;
use s3s::stream::ByteStream;
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
use tokio::pin;
use tracing::debug;
use tracing::error;
use tracing::info_span;
use tracing::trace;
use tracing::warn;
use tracing::Instrument;

pub struct ArunaS3Service {
    backend: Arc<Box<dyn StorageBackend>>,
    cache: Arc<Cache>,
}

impl Debug for ArunaS3Service {
    #[tracing::instrument(level = "trace", skip(self, f))]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArunaS3Service").finish()
    }
}

impl ArunaS3Service {
    #[tracing::instrument(level = "trace", skip(backend, cache))]
    pub async fn new(backend: Arc<Box<dyn StorageBackend>>, cache: Arc<Cache>) -> Result<Self> {
        Ok(ArunaS3Service {
            backend: backend.clone(),
            cache,
        })
    }
}

#[async_trait::async_trait]
impl S3 for ArunaS3Service {
    #[tracing::instrument(err)]
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let data = req.extensions.get::<CheckAccessResult>().cloned();

        let mut new_object = ProxyObject::from(req.input);

        if let Some(client) = self.cache.aruna_client.read().await.as_ref() {
            let CheckAccessResult {
                user_id, token_id, ..
            } = trace_err!(data.ok_or_else(|| s3_error!(InternalError, "Internal Error")))?;
            trace!(?user_id, ?token_id, "impersonating user");
            let token = trace_err!(self
                .cache
                .auth
                .read()
                .await
                .as_ref()
                .ok_or_else(|| s3_error!(InternalError, "Missing auth handler")))?
            .sign_impersonating_token(
                trace_err!(user_id.ok_or_else(|| {
                    s3_error!(NotSignedUp, "Unauthorized: Impersonating user error")
                }))?,
                token_id,
            )
            .map_err(|_| s3_error!(NotSignedUp, "Unauthorized: Impersonating error"))?;

            new_object = trace_err!(client.create_project(new_object, &token).await)
                .map_err(|_| s3_error!(InternalError, "[BACKEND] Unable to create project"))?;
        }
        let output = CreateBucketOutput {
            location: Some(new_object.name.to_string()),
        };

        trace_err!(self.cache.upsert_object(new_object, None).await)
            .map_err(|_| s3_error!(InternalError, "Unable to cache new bucket"))?;

        debug!(?output);
        Ok(S3Response::new(output))
    }

    #[tracing::instrument(err, skip(self, req))]
    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let expected_len = match req.input.content_length {
            Some(0) | None => {
                match req
                    .input
                    .body
                    .as_ref()
                    .map(|b| b.remaining_length().exact())
                    .flatten()
                {
                    Some(0) | None => {
                        error!("Missing or invalid (0) content-length");
                        return Err(s3_error!(
                            MissingContentLength,
                            "Missing or invalid (0) content-length"
                        ));
                    }
                    Some(a) => a as i64,
                }
            }
            Some(a) => a,
        };

        let CheckAccessResult {
            user_id,
            token_id,
            resource_ids,
            missing_resources,
            object,
            ..
        } = trace_err!(req
            .extensions
            .get::<CheckAccessResult>()
            .cloned()
            .ok_or_else(|| s3_error!(UnexpectedContent, "Missing data context")))?;

        trace!(?user_id, ?token_id, "impersonating user");
        let impersonating_token = if let Some(auth_handler) = self.cache.auth.read().await.as_ref()
        {
            user_id.and_then(|u| auth_handler.sign_impersonating_token(u, token_id).ok())
        } else {
            None
        };

        let res_ids =
            resource_ids.ok_or_else(|| s3_error!(InvalidArgument, "Unknown object path"))?;

        let mut object = if let Some((o, _)) = object {
            if o.object_type == crate::structs::ObjectType::Object {
                if o.object_status == Status::Initializing {
                    trace!("Object is initializing");
                    o
                } else {
                    // Force Object update with new revision in ArunaServer
                    trace!("Object is not initializing, force update");
                    let new_revision = if let Some(handler) =
                        self.cache.aruna_client.read().await.as_ref()
                    {
                        if let Some(token) = &impersonating_token {
                            trace_err!(handler.init_object_update(o, token, true).await)
                                .map_err(|_| s3_error!(InternalError, "Object update failed"))?
                        } else {
                            error!("missing impersonating token");
                            return Err(s3_error!(InternalError, "Token creation failed"));
                        }
                    } else {
                        return Err(s3_error!(InternalError, "ArunaServer client not available"));
                    };

                    trace!("Created dummy object");
                    ProxyObject {
                        id: new_revision.id,
                        name: new_revision.name,
                        key_values: new_revision.key_values,
                        object_status: Status::Initializing,
                        data_class: new_revision.data_class,
                        object_type: crate::structs::ObjectType::Object,
                        hashes: HashMap::default(),
                        metadata_license: new_revision.metadata_license,
                        data_license: new_revision.data_license,
                        dynamic: false,
                        children: None,
                        parents: new_revision.parents,
                        synced: false,
                        created_at: new_revision.created_at,
                    }
                }
            } else {
                trace!("ObjectType is not object");
                let missing_object_name = trace_err!(trace_err!(missing_resources
                    .clone()
                    .ok_or_else(|| s3_error!(InvalidArgument, "Invalid object path")))?
                .o
                .ok_or_else(|| s3_error!(InvalidArgument, "Invalid object path")))?;

                ProxyObject {
                    id: DieselUlid::generate(),
                    name: missing_object_name.to_string(),
                    key_values: vec![],
                    object_status: Status::Initializing,
                    data_class: DataClass::Private,
                    object_type: crate::structs::ObjectType::Object,
                    hashes: HashMap::default(),
                    metadata_license: ALL_RIGHTS_RESERVED.to_string(),
                    data_license: ALL_RIGHTS_RESERVED.to_string(),
                    dynamic: false,
                    children: None,
                    parents: None,
                    synced: false,
                    created_at: Some(chrono::Utc::now().naive_utc()),
                }
            }
        } else {
            let missing_object_name = trace_err!(trace_err!(missing_resources
                .clone()
                .ok_or_else(|| s3_error!(InvalidArgument, "Invalid object path")))?
            .o
            .ok_or_else(|| s3_error!(InvalidArgument, "Invalid object path")))?;

            ProxyObject {
                id: DieselUlid::generate(),
                name: missing_object_name.to_string(),
                key_values: vec![],
                object_status: Status::Initializing,
                data_class: DataClass::Private,
                object_type: crate::structs::ObjectType::Object,
                hashes: HashMap::default(),
                metadata_license: ALL_RIGHTS_RESERVED.to_string(),
                data_license: ALL_RIGHTS_RESERVED.to_string(),
                dynamic: false,
                children: None,
                parents: None,
                synced: false,
                created_at: Some(chrono::Utc::now().naive_utc()),
            }
        };

        // Initialize data location in the storage backend
        let mut location = trace_err!(
            self.backend
                .initialize_location(&object, Some(expected_len), None, false)
                .await
        )
        .map_err(|_| s3_error!(InternalError, "Unable to create object_location"))?;

        trace!("Initialized data location");

        // Initialize hashing transformers
        let (initial_sha_trans, initial_sha_recv) = HashingTransformer::new(Sha256::new());
        let (initial_md5_trans, initial_md5_recv) = HashingTransformer::new(Md5::new());
        let (initial_size_trans, initial_size_recv) = SizeProbe::new();
        let (final_sha_trans, final_sha_recv) = HashingTransformer::new(Sha256::new());
        let (final_size_trans, final_size_recv) = SizeProbe::new();

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
                    trace!("adding zstd decompressor");
                    awr = awr.add_transformer(ZstdEnc::new(true));
                    if location.raw_content_len > 5242880 + 80 * 28 {
                        trace!("adding footer generator");
                        awr = awr.add_transformer(FooterGenerator::new(None))
                    }
                }

                if let Some(enc_key) = &location.encryption_key {
                    awr = awr.add_transformer(
                        trace_err!(ChaCha20Enc::new(true, enc_key.to_string().into_bytes()))
                            .map_err(|_| {
                                s3_error!(
                                    InternalError,
                                    "Internal data transformer encryption error"
                                )
                            })?,
                    );
                }

                awr = awr.add_transformer(final_sha_trans);
                awr = awr.add_transformer(final_size_trans);

                trace_err!(awr.process().await).map_err(|_| {
                    s3_error!(InternalError, "Internal data transformer processing error")
                })?;
            }
            None => {
                error!("Empty body is not allowed");
                return Err(s3_error!(InvalidRequest, "Empty body is not allowed"));
            }
        }

        // Create missing parent resources
        if let Some(missing) = &missing_resources {
            trace!(?missing, "parent_resources");
            let collection_id = if let Some(collection) = &missing.c {
                let col = ProxyObject {
                    id: DieselUlid::default(),
                    name: collection.to_string(),
                    key_values: vec![],
                    object_status: Status::Available,
                    data_class: DataClass::Private,
                    object_type: crate::structs::ObjectType::Collection,
                    hashes: HashMap::default(),
                    metadata_license: ALL_RIGHTS_RESERVED.to_string(),
                    data_license: ALL_RIGHTS_RESERVED.to_string(),
                    dynamic: true,
                    children: None,
                    parents: Some(HashSet::from([TypedRelation::Project(
                        res_ids.get_project(),
                    )])),
                    synced: false,
                    created_at: Some(chrono::Utc::now().naive_utc()),
                };

                if let Some(handler) = self.cache.aruna_client.read().await.as_ref() {
                    if let Some(token) = &impersonating_token {
                        let col = trace_err!(handler.create_collection(col, token).await)
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

            let dataset_id = if let Some(dataset) = &missing.d {
                let parent = if let Some(collection_id) = collection_id {
                    TypedRelation::Collection(collection_id)
                } else {
                    TypedRelation::Project(res_ids.get_project())
                };

                let dataset = ProxyObject {
                    id: DieselUlid::generate(),
                    name: dataset.to_string(),
                    key_values: vec![],
                    object_status: Status::Available,
                    data_class: DataClass::Private,
                    object_type: crate::structs::ObjectType::Dataset,
                    hashes: HashMap::default(),
                    metadata_license: ALL_RIGHTS_RESERVED.to_string(),
                    data_license: ALL_RIGHTS_RESERVED.to_string(),
                    dynamic: true,
                    children: None,
                    parents: Some(HashSet::from([parent])),
                    synced: false,
                    created_at: Some(chrono::Utc::now().naive_utc()),
                };
                if let Some(handler) = self.cache.aruna_client.read().await.as_ref() {
                    if let Some(token) = &impersonating_token {
                        let dataset = trace_err!(handler.create_dataset(dataset, token).await)
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

            trace!("updating parents");
            if let Some(ds_id) = dataset_id {
                object.parents = Some(HashSet::from([TypedRelation::Dataset(ds_id)]));
            } else if let Some(col_id) = collection_id {
                object.parents = Some(HashSet::from([TypedRelation::Collection(col_id)]));
            } else {
                object.parents = Some(HashSet::from([TypedRelation::Project(
                    res_ids.get_project(),
                )]));
            }
        }

        // Fetch calculated hashes
        trace!("fetching hashes");
        let md5_initial = Some(
            trace_err!(initial_md5_recv.try_recv())
                .map_err(|_| s3_error!(InternalError, "Unable to md5 hash initial data"))?,
        );
        let sha_initial = Some(
            trace_err!(initial_sha_recv.try_recv())
                .map_err(|_| s3_error!(InternalError, "Unable to sha hash initial data"))?,
        );
        let sha_final: String = trace_err!(final_sha_recv.try_recv())
            .map_err(|_| s3_error!(InternalError, "Unable to sha hash final data"))?;
        let initial_size: u64 = trace_err!(initial_size_recv.try_recv())
            .map_err(|_| s3_error!(InternalError, "Unable to get size"))?;
        let final_size: u64 = trace_err!(final_size_recv.try_recv())
            .map_err(|_| s3_error!(InternalError, "Unable to get size"))?;

        object.hashes = vec![
            (
                "MD5".to_string(),
                trace_err!(md5_initial.clone().ok_or_else(|| {
                    s3_error!(InternalError, "Unable to get md5 hash initial data")
                }))?,
            ),
            (
                "SHA256".to_string(),
                trace_err!(sha_initial.clone().ok_or_else(|| {
                    s3_error!(InternalError, "Unable to get sha hash initial data")
                }))?,
            ),
        ]
        .into_iter()
        .collect::<HashMap<String, String>>();

        let hashes = vec![
            Hash {
                alg: Hashalgorithm::Sha256.into(),
                hash: trace_err!(sha_initial.clone().ok_or_else(|| {
                    s3_error!(InternalError, "Unable to get sha hash initial data")
                }))?,
            },
            Hash {
                alg: Hashalgorithm::Md5.into(),
                hash: trace_err!(md5_initial.clone().ok_or_else(|| {
                    s3_error!(InternalError, "Unable to get md5 hash initial data")
                }))?,
            },
        ];

        location.raw_content_len = initial_size as i64;
        location.disk_content_len = final_size as i64;
        location.disk_hash = Some(sha_final.clone());

        trace!("finishing object");
        if let Some(handler) = self.cache.aruna_client.read().await.as_ref() {
            if let Some(token) = &impersonating_token {
                if let Some(missing) = missing_resources {
                    if missing.o.is_some() {
                        trace_err!(
                            handler
                                .create_and_finish(object.clone(), location, token)
                                .await
                        )
                        .map_err(|_| {
                            s3_error!(InternalError, "Unable to create and/or finish object")
                        })?;
                    } else {
                        trace_err!(
                            handler
                                .finish_object(
                                    object.id,
                                    location.raw_content_len,
                                    hashes,
                                    Some(location),
                                    token,
                                )
                                .await
                        )
                        .map_err(|_| s3_error!(InternalError, "Unable to finish object"))?;
                    }
                } else {
                    trace_err!(
                        handler
                            .finish_object(
                                object.id,
                                location.raw_content_len,
                                hashes,
                                Some(location),
                                token,
                            )
                            .await
                    )
                    .map_err(|_| s3_error!(InternalError, "Unable to finish object"))?;
                }
            }
        }

        let output = PutObjectOutput {
            e_tag: md5_initial,
            checksum_sha256: sha_initial,
            version_id: Some(object.id.to_string()),
            ..Default::default()
        };
        debug!(?output);
        Ok(S3Response::new(output))
    }

    #[tracing::instrument(err)]
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let CheckAccessResult {
            user_id,
            token_id,
            resource_ids,
            missing_resources,
            object,
            ..
        } = trace_err!(req
            .extensions
            .get::<CheckAccessResult>()
            .cloned()
            .ok_or_else(|| s3_error!(UnexpectedContent, "Missing data context")))?;

        let impersonating_token = if let Some(auth_handler) = self.cache.auth.read().await.as_ref()
        {
            user_id.and_then(|u| auth_handler.sign_impersonating_token(u, token_id).ok())
        } else {
            None
        };

        let res_ids = trace_err!(
            resource_ids.ok_or_else(|| s3_error!(InvalidArgument, "Unknown object path"))
        )?;

        let mut new_object = if let Some((o, _)) = object {
            if o.object_type == crate::structs::ObjectType::Object {
                if o.object_status == Status::Initializing {
                    trace!("Object is initializing");
                    o
                } else {
                    trace!("object is not initializing, force update");
                    // Force Object update with new revision in ArunaServer
                    let new_revision = if let Some(handler) =
                        self.cache.aruna_client.read().await.as_ref()
                    {
                        if let Some(token) = &impersonating_token {
                            trace_err!(handler.init_object_update(o, token, true).await)
                                .map_err(|_| s3_error!(InternalError, "Object update failed"))?
                        } else {
                            error!("missing impersonating token");
                            return Err(s3_error!(InternalError, "Token creation failed"));
                        }
                    } else {
                        //TODO: Enable offline mode
                        error!("ArunaServer client not available");
                        return Err(s3_error!(InternalError, "ArunaServer client not available"));
                    };

                    ProxyObject {
                        id: new_revision.id,
                        name: new_revision.name,
                        key_values: new_revision.key_values,
                        object_status: Status::Initializing,
                        data_class: new_revision.data_class,
                        object_type: crate::structs::ObjectType::Object,
                        hashes: HashMap::default(),
                        metadata_license: new_revision.metadata_license,
                        data_license: new_revision.data_license,
                        dynamic: false,
                        children: None,
                        parents: new_revision.parents,
                        synced: false,
                        created_at: new_revision.created_at,
                    }
                }
            } else {
                let missing_object_name = trace_err!(trace_err!(missing_resources
                    .clone()
                    .ok_or_else(|| s3_error!(InvalidArgument, "Invalid object path")))?
                .o
                .ok_or_else(|| s3_error!(InvalidArgument, "Invalid object path")))?;

                trace!(?missing_object_name, "missing_object_name");

                ProxyObject {
                    id: DieselUlid::generate(),
                    name: missing_object_name.to_string(),
                    key_values: vec![],
                    object_status: Status::Initializing,
                    data_class: DataClass::Private,
                    object_type: crate::structs::ObjectType::Object,
                    hashes: HashMap::default(),
                    metadata_license: ALL_RIGHTS_RESERVED.to_string(),
                    data_license: ALL_RIGHTS_RESERVED.to_string(),
                    dynamic: false,
                    children: None,
                    parents: None,
                    synced: false,
                    created_at: Some(chrono::Utc::now().naive_utc()),
                }
            }
        } else {
            let missing_object_name = trace_err!(trace_err!(missing_resources
                .clone()
                .ok_or_else(|| s3_error!(InvalidArgument, "Invalid object path")))?
            .o
            .ok_or_else(|| s3_error!(InvalidArgument, "Invalid object path")))?;

            ProxyObject {
                id: DieselUlid::generate(),
                name: missing_object_name.to_string(),
                key_values: vec![],
                object_status: Status::Initializing,
                data_class: DataClass::Private,
                object_type: crate::structs::ObjectType::Object,
                hashes: HashMap::default(),
                metadata_license: ALL_RIGHTS_RESERVED.to_string(),
                data_license: ALL_RIGHTS_RESERVED.to_string(),
                dynamic: false,
                children: None,
                parents: None,
                synced: false,
                created_at: Some(chrono::Utc::now().naive_utc()),
            }
        };

        let mut location = trace_err!(
            self.backend
                .initialize_location(&new_object, None, None, true)
                .await
        )
        .map_err(|_| s3_error!(InternalError, "Unable to create object_location"))?;

        let init_response = trace_err!(
            self.backend
                .clone()
                .init_multipart_upload(location.clone())
                .await
        )
        .map_err(|_| s3_error!(InvalidArgument, "Unable to initialize multi-part"))?;

        location.upload_id = Some(init_response.to_string());

        if let Some(missing) = missing_resources {
            trace!(?missing, "missing parent resources");
            let collection_id = if let Some(collection) = missing.c {
                let col = ProxyObject {
                    id: DieselUlid::generate(),
                    name: collection,
                    key_values: vec![],
                    object_status: Status::Available,
                    data_class: DataClass::Private,
                    object_type: crate::structs::ObjectType::Collection,
                    hashes: HashMap::default(),
                    metadata_license: ALL_RIGHTS_RESERVED.to_string(),
                    data_license: ALL_RIGHTS_RESERVED.to_string(),
                    dynamic: true,
                    children: None,
                    parents: Some(HashSet::from([TypedRelation::Project(
                        res_ids.get_project(),
                    )])),
                    synced: false,
                    created_at: Some(chrono::Utc::now().naive_utc()),
                };

                if let Some(handler) = self.cache.aruna_client.read().await.as_ref() {
                    if let Some(token) = &impersonating_token {
                        let col = trace_err!(handler.create_collection(col, token).await)
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
                    metadata_license: ALL_RIGHTS_RESERVED.to_string(),
                    data_license: ALL_RIGHTS_RESERVED.to_string(),
                    dynamic: true,
                    children: None,
                    parents: Some(HashSet::from([parent])),
                    synced: false,
                    created_at: Some(chrono::Utc::now().naive_utc()),
                };

                if let Some(handler) = self.cache.aruna_client.read().await.as_ref() {
                    if let Some(token) = &impersonating_token {
                        let dataset = trace_err!(handler.create_dataset(dataset, token).await)
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

            trace!("updating parents");
            if let Some(ds_id) = dataset_id {
                new_object.parents = Some(HashSet::from([TypedRelation::Dataset(ds_id)]));
            } else if let Some(col_id) = collection_id {
                new_object.parents = Some(HashSet::from([TypedRelation::Collection(col_id)]));
            } else {
                new_object.parents = Some(HashSet::from([TypedRelation::Project(
                    res_ids.get_project(),
                )]));
            }

            trace!("finishing object");
            if let Some(handler) = self.cache.aruna_client.read().await.as_ref() {
                if let Some(token) = &impersonating_token {
                    trace_err!(
                        handler
                            .create_object(new_object.clone(), Some(location), token)
                            .await
                    )
                    .map_err(|_| s3_error!(InternalError, "Unable to create object"))?;
                }
            }
        } else {
            // Just update cache
            trace_err!(self.cache.upsert_object(new_object, Some(location)).await)
                .map_err(|_| s3_error!(InternalError, "Cache update failed"))?
        }

        let output = CreateMultipartUploadOutput {
            key: Some(req.input.key),
            bucket: Some(req.input.bucket),
            upload_id: Some(init_response),
            ..Default::default()
        };
        debug!(?output);
        Ok(S3Response::new(output))
    }

    #[tracing::instrument(err)]
    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        match req.input.content_length {
            Some(0) | None => {
                match req
                    .input
                    .body
                    .as_ref()
                    .map(|b| b.remaining_length().exact())
                    .flatten()
                {
                    Some(0) | None => {
                        error!("Missing or invalid (0) content-length");
                        return Err(s3_error!(
                            MissingContentLength,
                            "Missing or invalid (0) content-length"
                        ));
                    }
                    _ => {}
                }
            }
            _ => {}
        };

        let CheckAccessResult { object, .. } = trace_err!(req
            .extensions
            .get::<CheckAccessResult>()
            .cloned()
            .ok_or_else(|| s3_error!(UnexpectedContent, "Missing data context")))?;

        // If the object exists and the signatures match -> Skip the download

        let location = if let Some((_, Some(loc))) = object {
            loc
        } else {
            error!("object not initialized");
            return Err(s3_error!(InvalidArgument, "Object not initialized"));
        };

        let etag = match req.input.body {
            Some(data) => {
                trace!("streaming data to backend");

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
                    trace!("adding zstd compressor");
                    awr = awr.add_transformer(ZstdEnc::new(false));
                }

                if let Some(enc_key) = &location.encryption_key {
                    trace!("adding chacha20 encryption");
                    awr = awr.add_transformer(
                        trace_err!(ChaCha20Enc::new(true, enc_key.to_string().into_bytes()))
                            .map_err(|_| {
                                s3_error!(
                                    InternalError,
                                    "Internal data transformer encryption error"
                                )
                            })?,
                    );
                }

                trace_err!(awr.process().await).map_err(|_| {
                    s3_error!(InternalError, "Internal data transformer processing error")
                })?;

                if let Some(r) = receiver {
                    trace_err!(r.recv().await)
                        .map_err(|_| s3_error!(InternalError, "Unable to query etag"))?
                } else {
                    error!("receiver is none");
                    return Err(s3_error!(InternalError, "receiver is none"));
                }
            }
            None => {
                error!("empty body is not allowed");
                return Err(s3_error!(InvalidRequest, "Empty body is not allowed"));
            }
        };

        let output = UploadPartOutput {
            e_tag: Some(format!("-{}", etag)),
            ..Default::default()
        };
        debug!(?output);
        Ok(S3Response::new(output))
    }

    #[tracing::instrument(err)]
    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let CheckAccessResult {
            user_id,
            token_id,
            object,
            ..
        } = trace_err!(req
            .extensions
            .get::<CheckAccessResult>()
            .cloned()
            .ok_or_else(|| s3_error!(UnexpectedContent, "Missing data context")))?;

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
            error!("object not initialized");
            return Err(s3_error!(InvalidArgument, "Object not initialized"));
        };

        let parts = match req.input.multipart_upload {
            Some(parts) => trace_err!(parts
                .parts
                .ok_or_else(|| s3_error!(InvalidPart, "Parts must be specified"))),
            None => {
                error!("parts is none");
                return Err(s3_error!(InvalidPart, "Parts must be specified"));
            }
        }?;

        let etag_parts = parts
            .into_iter()
            .map(|a| {
                Ok(PartETag {
                    part_number: a.part_number,
                    etag: trace_err!(a
                        .e_tag
                        .ok_or_else(|| s3_error!(InvalidPart, "etag must be specified")))?,
                })
            })
            .collect::<Result<Vec<PartETag>, S3Error>>()?;

        // Does this object exists (including object id etc)
        //req.input.multipart_upload.unwrap().
        trace_err!(
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
        )
        .map_err(|_| s3_error!(InternalError, "Unable to finish upload"))?;

        let response = CompleteMultipartUploadOutput {
            e_tag: Some(object.id.to_string()),
            ..Default::default()
        };

        let mut new_location = trace_err!(
            self.backend
                .initialize_location(&object, None, None, false)
                .await
        )
        .map_err(|_| s3_error!(InternalError, "Unable to create object_location"))?;

        let hashes = trace_err!(
            DataHandler::finalize_location(self.backend.clone(), &old_location, &mut new_location)
                .await
        )
        .map_err(|_| s3_error!(InternalError, "Unable to finalize location"))?;

        if let Some(handler) = self.cache.aruna_client.read().await.as_ref() {
            if let Some(token) = &impersonating_token {
                // Set id of new location to object id to satisfy FK constraint
                let object = trace_err!(
                    handler
                        .finish_object(object.id, new_location.raw_content_len, hashes, None, token)
                        .await
                )
                .map_err(|_| s3_error!(InternalError, "Unable to create object"))?;

                trace_err!(self.cache.upsert_object(object, Some(new_location)).await)
                    .map_err(|_| s3_error!(InternalError, "Unable to cache object after finish"))?;

                trace_err!(self.backend.delete_object(old_location).await)
                    .map_err(|_| s3_error!(InternalError, "Unable to delete old object"))?;
            }
        }
        debug!(?response);
        Ok(S3Response::new(response))
    }

    #[tracing::instrument(err)]
    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let CheckAccessResult {
            object,
            bundle,
            headers,
            ..
        } = trace_err!(req
            .extensions
            .get::<CheckAccessResult>()
            .cloned()
            .ok_or_else(|| s3_error!(InternalError, "No context found")))?;

        if let Some(_bundle) = bundle {
            let id = match object {
                Some((obj, _)) => obj.id,
                None => return Err(s3_error!(NoSuchKey, "Object not found")),
            };
            let levels = trace_err!(self.cache.get_path_levels(id))
                .map_err(|_| s3_error!(InternalError, "Unable to get path levels"))?;

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
            None => {
                error!("Object not found");
                return Err(s3_error!(NoSuchKey, "Object not found"));
            }
        };

        let cache_result = trace_err!(self
            .cache
            .resources
            .get(&id)
            .ok_or_else(|| s3_error!(NoSuchKey, "Object not found")))?;

        let (_, location) = cache_result.value();
        let location = trace_err!(location
            .as_ref()
            .ok_or_else(|| s3_error!(InternalError, "Object location not found")))?
        .clone();
        let content_length = location.raw_content_len;
        let encryption_key = location
            .clone()
            .encryption_key
            .map(|k| k.as_bytes().to_vec());

        let (sender, receiver) = async_channel::bounded(10);

        // Gets 128 kb chunks (last 2)
        let footer_limit = 5242880 + 80 * 28;
        let footer_parser: Option<FooterParser> = if content_length > footer_limit {
            trace!("getting footer");
            // Without encryption block because this is already checked inside
            let (footer_sender, footer_receiver) = async_channel::unbounded();
            pin!(footer_receiver);

            let parser = match encryption_key.clone() {
                Some(key) => {
                    trace_err!(
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
                    )
                    .map_err(|_| s3_error!(InternalError, "Unable to get encryption_footer"))?;

                    // Stream takes receiver chunks und them into vec
                    let mut output = Vec::with_capacity(131_128);
                    let mut arsw =
                        ArunaStreamReadWriter::new_with_writer(footer_receiver, &mut output);

                    // processes chunks and puts them into output
                    trace_err!(arsw.process().await)
                        .map_err(|_| s3_error!(InternalError, "Unable to get footer"))?;
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
                    trace_err!(
                        self.backend
                            .get_object(
                                location.clone(),
                                Some(format!("bytes=-{}", 65536 * 2)),
                                // when not encrypted without 28
                                footer_sender,
                            )
                            .await
                    )
                    .map_err(|_| s3_error!(InternalError, "Unable to get compression footer"))?;
                    let mut output = Vec::with_capacity(131_128);
                    let mut arsw =
                        ArunaStreamReadWriter::new_with_writer(footer_receiver, &mut output);

                    trace_err!(arsw.process().await)
                        .map_err(|_| s3_error!(InternalError, "Unable to get footer"))?;
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
        trace!("calculating ranges");
        let (query_ranges, filter_ranges, actual_from) =
            match calculate_ranges(req.input.range, content_length as u64, footer_parser) {
                Ok((query_ranges, filter_ranges, actual_from)) => {
                    (query_ranges, filter_ranges, actual_from)
                }
                Err(err) => {
                    warn!("Error while parsing ranges: {}", err);
                    if let Some(range) = req.input.range {
                        let mut aruna_range =
                            aruna_range_from_s3range(range, content_length as u64);
                        aruna_range.to += 1;
                        (None, Some(aruna_range), aruna_range.from)
                    } else {
                        (None, None, 0)
                    }
                }
            };

        let (content_length, accept_ranges, content_range) = match filter_ranges {
            Some(filter_range) => (
                calculate_content_length_from_range(filter_range),
                Some("bytes".to_string()),
                Some(format!(
                    "bytes {}-{}/{}",
                    actual_from + filter_range.from,
                    actual_from + filter_range.to - 1,
                    content_length
                )),
            ),
            None => (content_length, None, None),
        };
        debug!(
            ?content_length,
            ?query_ranges,
            ?filter_ranges,
            ?accept_ranges,
            ?content_range
        );

        // Spawn get_object to fetch bytes from storage storage
        let backend = self.backend.clone();
        tokio::spawn(
            async move {
                backend
                    .get_object(location.clone(), query_ranges, sender)
                    .await
            }
            .instrument(info_span!("get_object")),
        );
        let (final_send, final_rcv) = async_channel::unbounded();

        // Spawn final part
        let cloned_key = encryption_key.clone();
        tokio::spawn(
            async move {
                pin!(receiver);
                let mut asrw = ArunaStreamReadWriter::new_with_sink(
                    receiver,
                    AsyncSenderSink::new(final_send),
                );

                asrw = asrw
                    .add_transformer(trace_err!(ChaCha20Dec::new(cloned_key)
                        .map_err(|_| { s3_error!(InternalError, "Internal notifier error") }))?)
                    .add_transformer(ZstdDec::new());

                if let Some(filter_range) = filter_ranges {
                    asrw = asrw.add_transformer(Filter::new(filter_range));
                };

                trace_err!(asrw.process().await)
                    .map_err(|_| s3_error!(InternalError, "Internal notifier error"))?;

                Ok::<_, anyhow::Error>(())
            }
            .instrument(tracing::info_span!("query_data")),
        );

        let body =
            Some(StreamingBlob::wrap(trace_err!(final_rcv).map_err(|_| {
                s3_error!(InternalError, "Internal processing error")
            })));

        let output = GetObjectOutput {
            body,
            accept_ranges,
            content_range,
            content_length,
            last_modified: None,
            e_tag: Some(format!("-{}", id)),
            version_id: None,
            ..Default::default()
        };
        debug!(?output);

        let mut resp = S3Response::new(output);
        if let Some(headers) = headers {
            for (k, v) in headers {
                resp.headers.insert(
                    HeaderName::from_bytes(k.as_bytes())
                        .map_err(|_| s3_error!(InternalError, "Unable to parse header name"))?,
                    HeaderValue::from_str(&*v)
                        .map_err(|_| s3_error!(InternalError, "Unable to parse header value"))?,
                );
            }
        }
        Ok(resp)
    }

    #[tracing::instrument(err)]
    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let CheckAccessResult {
            object,
            bundle,
            headers,
            ..
        } = trace_err!(req
            .extensions
            .get::<CheckAccessResult>()
            .cloned()
            .ok_or_else(|| s3_error!(InternalError, "No context found")))?;

        trace!(?object, ?bundle);

        if let Some(_bundle) = bundle {
            let id = match object {
                Some((obj, _)) => obj.id,
                None => {
                    trace!("Object not found");
                    return Err(s3_error!(NoSuchKey, "Object not found"));
                }
            };
            let _levels = trace_err!(self.cache.get_path_levels(id))
                .map_err(|_| s3_error!(InternalError, "Unable to get path levels"))?;

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

        let (object, location) =
            trace_err!(object.ok_or_else(|| s3_error!(NoSuchKey, "No object found")))?;

        let content_len = location.map(|l| l.raw_content_len).unwrap_or_default();

        let output = HeadObjectOutput {
            content_length: content_len,
            last_modified: Some(
                // FIXME: Real time ...
                time::OffsetDateTime::from_unix_timestamp(Utc::now().timestamp())
                    .unwrap()
                    .into(),
            ),
            e_tag: Some(object.id.to_string()),
            ..Default::default()
        };

        debug!(?output);

        debug!(?headers);

        let mut resp = S3Response::new(output);
        if let Some(headers) = headers {
            for (k, v) in headers {
                resp.headers.insert(
                    HeaderName::from_bytes(k.as_bytes())
                        .map_err(|_| s3_error!(InternalError, "Unable to parse header name"))?,
                    HeaderValue::from_str(&*v)
                        .map_err(|_| s3_error!(InternalError, "Unable to parse header value"))?,
                );
            }
        }

        Ok(resp)
    }

    #[tracing::instrument(err)]
    async fn list_objects(
        &self,
        _req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        error!("ListObjects is not implemented yet");
        Err(s3_error!(
            NotImplemented,
            "ListObjects is not implemented yet"
        ))
    }

    #[tracing::instrument(err)]
    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let CheckAccessResult { headers, .. } = trace_err!(req
            .extensions
            .get::<CheckAccessResult>()
            .cloned()
            .ok_or_else(|| s3_error!(InternalError, "No context found")))?;
        // Fetch the project name, delimiter and prefix from the request
        let project_name = &req.input.bucket;
        let delimiter = req.input.delimiter;
        let prefix = req.input.prefix.filter(|prefix| !prefix.is_empty());

        // Check if bucket exists as root in cache of paths
        match self
            .cache
            .paths
            .get(&ResourceString::Project(project_name.to_string()))
        {
            Some(_) => {}
            None => {
                error!("No bucket found");
                return Err(s3_error!(NoSuchBucket, "No bucket found"));
            }
        };

        // Process continuation token from request
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

        // Filter all objects from cache which have the project as root
        let sorted = filter_list_objects(&self.cache.paths, project_name);
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

        let (keys, common_prefixes, new_continuation_token) = trace_err!(list_response(
            sorted,
            &self.cache,
            &delimiter,
            &prefix,
            &start_after,
            max_keys,
        ))
        .map_err(|_| s3_error!(NoSuchKey, "Keys not found in ListObjectsV2"))?;

        let key_count = (keys.len() + common_prefixes.len()) as i32;
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
                    last_modified: e.created_at.map(|t| {
                        s3s::dto::Timestamp::from(
                            time::OffsetDateTime::from_unix_timestamp(t.timestamp()).unwrap(),
                        )
                    }),
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
            max_keys: max_keys
                .try_into()
                .map_err(|err| s3_error!(InternalError, "[BACKEND] Conversion failure: {}", err))?,
            name: Some(project_name.clone()),
            next_continuation_token: new_continuation_token,
            prefix,
            start_after: Some(start_after),
            ..Default::default()
        };
        debug!(?result);

        let mut resp = S3Response::new(result);

        if let Some(headers) = headers {
            for (k, v) in headers {
                resp.headers.insert(
                    HeaderName::from_bytes(k.as_bytes())
                        .map_err(|_| s3_error!(InternalError, "Unable to parse header name"))?,
                    HeaderValue::from_str(&*v)
                        .map_err(|_| s3_error!(InternalError, "Unable to parse header value"))?,
                );
            }
        }

        Ok(resp)
    }

    #[tracing::instrument(err)]
    async fn put_bucket_cors(
        &self,
        req: S3Request<PutBucketCorsInput>,
    ) -> S3Result<S3Response<PutBucketCorsOutput>> {
        let config = crate::structs::CORSConfiguration(
            req.input
                .cors_configuration
                .cors_rules
                .into_iter()
                .map(CORSRule::into)
                .collect(),
        );

        let data = req.extensions.get::<CheckAccessResult>().cloned();

        if let Some(client) = self.cache.aruna_client.read().await.as_ref() {
            let CheckAccessResult {
                user_id,
                token_id,
                object,
                ..
            } = trace_err!(data.ok_or_else(|| s3_error!(InternalError, "Internal Error")))?;

            trace!(?token_id, ?user_id, "put_bucket_cors");

            let (bucket_obj, _) =
                object.ok_or_else(|| s3_error!(NoSuchBucket, "Bucket not found"))?;

            let token = trace_err!(self
                .cache
                .auth
                .read()
                .await
                .as_ref()
                .ok_or_else(|| s3_error!(InternalError, "Missing auth handler")))?
            .sign_impersonating_token(
                trace_err!(user_id.ok_or_else(|| {
                    s3_error!(NotSignedUp, "Unauthorized: Impersonating user error")
                }))?,
                token_id,
            )
            .map_err(|_| s3_error!(NotSignedUp, "Unauthorized: Impersonating error"))?;

            trace_err!(
                client
                    .add_or_replace_key_value_project(
                        &token,
                        bucket_obj,
                        Some((
                            "app.aruna-storage.org/cors",
                            &serde_json::to_string(&config).map_err(|_| s3_error!(
                                InternalError,
                                "Unable to serialize cors configuration"
                            ))?
                        ))
                    )
                    .await
            )
            .map_err(|_| s3_error!(InternalError, "Unable to update KeyValues"))?;
        }
        Ok(S3Response::new(PutBucketCorsOutput::default()))
    }

    #[tracing::instrument(err)]
    async fn delete_bucket_cors(
        &self,
        req: S3Request<DeleteBucketCorsInput>,
    ) -> S3Result<S3Response<DeleteBucketCorsOutput>> {
        let data = req.extensions.get::<CheckAccessResult>().cloned();

        if let Some(client) = self.cache.aruna_client.read().await.as_ref() {
            let CheckAccessResult {
                user_id,
                token_id,
                object,
                ..
            } = trace_err!(data.ok_or_else(|| s3_error!(InternalError, "Internal Error")))?;

            let (bucket_obj, _) =
                object.ok_or_else(|| s3_error!(NoSuchBucket, "Bucket not found"))?;

            let token = trace_err!(self
                .cache
                .auth
                .read()
                .await
                .as_ref()
                .ok_or_else(|| s3_error!(InternalError, "Missing auth handler")))?
            .sign_impersonating_token(
                trace_err!(user_id.ok_or_else(|| {
                    s3_error!(NotSignedUp, "Unauthorized: Impersonating user error")
                }))?,
                token_id,
            )
            .map_err(|_| s3_error!(NotSignedUp, "Unauthorized: Impersonating error"))?;

            trace_err!(
                client
                    .add_or_replace_key_value_project(&token, bucket_obj, None,)
                    .await
            )
            .map_err(|_| s3_error!(InternalError, "Unable to update KeyValues"))?;
        }
        Ok(S3Response::new(DeleteBucketCorsOutput::default()))
    }

    #[tracing::instrument(err)]
    async fn get_bucket_location(
        &self,
        _req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        return Ok(S3Response::new(GetBucketLocationOutput {
            // TODO: Return proxy location / id -> Not possible restricted set of allowed locations
            location_constraint: None,
        }));
    }

    #[tracing::instrument(err)]
    async fn get_bucket_cors(
        &self,
        req: S3Request<GetBucketCorsInput>,
    ) -> S3Result<S3Response<GetBucketCorsOutput>> {
        let data = req.extensions.get::<CheckAccessResult>().cloned();

        let CheckAccessResult { object, .. } =
            trace_err!(data.ok_or_else(|| s3_error!(InternalError, "Internal Error")))?;

        let (bucket_obj, _) =
            object.ok_or_else(|| s3_error!(NoSuchBucket, "Bucket not found"))?;

        let cors = bucket_obj
            .key_values
            .into_iter()
            .find(|kv| kv.key == "app.aruna-storage.org/cors")
            .map(|kv| kv.value);

        if let Some(cors) = cors {
            let cors: crate::structs::CORSConfiguration =
                trace_err!(serde_json::from_str(&cors))
                    .map_err(|_| s3_error!(InternalError, "Unable to deserialize cors"))?;
            return Ok(S3Response::new(cors.into()));
        }
        Ok(S3Response::new(GetBucketCorsOutput::default()))
    }
}
