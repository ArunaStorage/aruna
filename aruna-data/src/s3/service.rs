use crate::lmdbstore::LmdbStore;
use s3s::{dto::{PutObjectInput, PutObjectOutput}, s3_error, S3Request, S3Response, S3Result, S3};
use tracing::error;
use std::sync::Arc;

pub struct ArunaS3Service {
    storage: Arc<LmdbStore>,
}

impl ArunaS3Service {
    pub fn new(storage: Arc<LmdbStore>) -> Self {
        Self { storage }
    }
}

#[async_trait::async_trait]
impl S3 for ArunaS3Service {
    #[tracing::instrument(err, skip(self, req))]
    #[allow(clippy::blocks_in_conditions)]
    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {

        todo!()
        // match req.input.content_length {
        //     Some(0) | None => {
        //         error!("Missing or invalid (0) content-length");
        //         return Err(s3_error!(
        //             MissingContentLength,
        //             "Missing or invalid (0) content-length"
        //         ));
        //     }
        //     _ => {}
        // };



        // let impersonating_token =
        //     user_state.sign_impersonating_token(self.cache.auth.read().await.as_ref());

        // let (states, _) = objects_state.require_regular()?;

        // let (_, collection, dataset, object, location_state) = states.to_new_or_existing()?;

        // let (mut new_object, was_init) = match object {
        //     NewOrExistingObject::Existing(ob) => {
        //         if ob.object_status == Status::Initializing {
        //             trace!("Object is initializing");
        //             (ob, true)
        //         } else {
        //             let mut new_revision = if let Some(handler) =
        //                 self.cache.aruna_client.read().await.as_ref()
        //             {
        //                 if let Some(token) = &impersonating_token {
        //                     handler
        //                         .init_object_update(ob, token, true)
        //                         .await
        //                         .map_err(|_| {
        //                             error!(error = "Object update failed");
        //                             s3_error!(InternalError, "Object update failed")
        //                         })?
        //                 } else {
        //                     error!("missing impersonating token");
        //                     return Err(s3_error!(InternalError, "Token creation failed"));
        //                 }
        //             } else {
        //                 error!("ArunaServer client not available");
        //                 return Err(s3_error!(InternalError, "ArunaServer client not available"));
        //             };
        //             new_revision.hashes = HashMap::default();
        //             new_revision.synced = false;
        //             new_revision.children = None;
        //             new_revision.dynamic = false;
        //             (new_revision, true)
        //         }
        //     }
        //     NewOrExistingObject::Missing(object) => (object, false),
        //     NewOrExistingObject::None => {
        //         return Err(s3_error!(
        //             InvalidObjectState,
        //             "Object in invalid state: None"
        //         ))
        //     }
        // };

        // trace!(?new_object);

        // let mut location = self
        //     .backend
        //     .initialize_location(&new_object, req.input.content_length, location_state, false)
        //     .await
        //     .map_err(|_| {
        //         error!(error = "Unable to create object_location");
        //         s3_error!(InternalError, "Unable to create object_location")
        //     })?;
        // trace!(?location);

        // trace!("Initialized data location");

        // // Initialize hashing transformers
        // let (initial_sha_trans, initial_sha_recv) =
        //     HashingTransformer::new_with_backchannel(Sha256::new(), "sha256".to_string());
        // let (initial_md5_trans, initial_md5_recv) =
        //     HashingTransformer::new_with_backchannel(Md5::new(), "md5".to_string());
        // let (initial_size_trans, initial_size_recv) = SizeProbe::new();
        // let (final_sha_trans, final_sha_recv) =
        //     HashingTransformer::new_with_backchannel(Sha256::new(), "sha256".to_string());
        // let (final_size_trans, final_size_recv) = SizeProbe::new();

        // match req.input.body {
        //     Some(data) => {
        //         let (tx, rx) = async_channel::bounded(10);

        //         let mut awr = GenericStreamReadWriter::new_with_sink(
        //             data,
        //             BufferedS3Sink::new(
        //                 self.backend.clone(),
        //                 location.clone(),
        //                 None,
        //                 None,
        //                 false,
        //                 None,
        //                 false,
        //             )
        //             .0,
        //         );

        //         awr.add_message_receiver(rx).await.map_err(|_| {
        //             error!(error = "Internal notifier error");
        //             s3_error!(InternalError, "Internal notifier error")
        //         })?;

        //         awr = awr.add_transformer(initial_sha_trans);
        //         awr = awr.add_transformer(initial_md5_trans);
        //         awr = awr.add_transformer(initial_size_trans);

        //         if location.is_compressed() && !location.is_pithos() {
        //             trace!("adding zstd decompressor");
        //             awr = awr.add_transformer(ZstdEnc::new());
        //         }

        //         if let Some(enc_key) = &location.get_encryption_key() {
        //             if !location.is_pithos() {
        //                 awr = awr.add_transformer(ChaCha20Enc::new_with_fixed(*enc_key).map_err(
        //                     |_| {
        //                         error!(error = "Unable to initialize ChaCha20Enc");
        //                         s3_error!(
        //                             InternalError,
        //                             "Internal data transformer encryption error"
        //                         )
        //                     },
        //                 )?);
        //             }
        //         }

        //         if location.is_pithos() {
        //             let ctx = new_object
        //                 .get_file_context(Some(location.clone()), req.input.content_length)
        //                 .map_err(|_| {
        //                     error!(error = "Unable to get file context");
        //                     s3_error!(InternalError, "Unable to get file context")
        //                 })?;
        //             tx.send(PithosMessage::FileContext(ctx))
        //                 .await
        //                 .map_err(|_| {
        //                     error!(error = "Internal notifier error");
        //                     s3_error!(InternalError, "Internal notifier error")
        //                 })?;
        //             awr = awr.add_transformer(PithosTransformer::new());
        //             awr = awr.add_transformer(FooterGenerator::new(None));
        //         }
        //         awr = awr.add_transformer(final_sha_trans);
        //         awr = awr.add_transformer(final_size_trans);

        //         awr.process().await.map_err(|_| {
        //             error!(error = "Internal data transformer processing error");
        //             s3_error!(InternalError, "Internal data transformer processing error")
        //         })?;
        //     }
        //     None => {
        //         error!("Empty body is not allowed");
        //         return Err(s3_error!(InvalidRequest, "Empty body is not allowed"));
        //     }
        // }

        // let mut collection_id = None;
        // if let NewOrExistingObject::Missing(collection) = collection {
        //     if let Some(handler) = self.cache.aruna_client.read().await.as_ref() {
        //         if let Some(token) = &impersonating_token {
        //             let col = handler
        //                 .create_collection(collection, token)
        //                 .await
        //                 .map_err(|_| {
        //                     error!(error = "Unable to create collection");
        //                     s3_error!(InternalError, "Unable to create collection")
        //                 })?;
        //             collection_id = Some(col.id)
        //         }
        //     }
        // }

        // let mut dataset_id = None;
        // if let NewOrExistingObject::Missing(mut dataset) = dataset {
        //     if let Some(handler) = self.cache.aruna_client.read().await.as_ref() {
        //         if let Some(token) = &impersonating_token {
        //             if let Some(collection_id) = collection_id {
        //                 dataset.parents = Some(HashSet::from_iter([TypedRelation::Collection(
        //                     collection_id,
        //                 )]));
        //             }
        //             let dataset = handler.create_dataset(dataset, token).await.map_err(|_| {
        //                 error!(error = "Unable to create dataset");
        //                 s3_error!(InternalError, "Unable to create dataset")
        //             })?;
        //             dataset_id = Some(dataset.id);
        //         }
        //     }
        // }

        // if let Some(dataset_id) = dataset_id {
        //     new_object.parents = Some(HashSet::from_iter([TypedRelation::Dataset(dataset_id)]));
        // } else if let Some(collection_id) = collection_id {
        //     new_object.parents = Some(HashSet::from_iter([TypedRelation::Collection(
        //         collection_id,
        //     )]));
        // }

        // // Fetch calculated hashes
        // trace!("fetching hashes");
        // let md5_initial = Some(initial_md5_recv.try_recv().map_err(|_| {
        //     error!(error = "Unable to md5 hash initial data");
        //     s3_error!(InternalError, "Unable to md5 hash initial data")
        // })?);
        // let sha_initial = Some(initial_sha_recv.try_recv().map_err(|_| {
        //     error!(error = "Unable to sha hash initial data");
        //     s3_error!(InternalError, "Unable to sha hash initial data")
        // })?);
        // let sha_final: String = final_sha_recv.try_recv().map_err(|_| {
        //     error!(error = "Unable to sha hash final data");
        //     s3_error!(InternalError, "Unable to sha hash final data")
        // })?;
        // let initial_size: u64 = initial_size_recv.try_recv().map_err(|_| {
        //     error!(error = "Unable to get size");
        //     s3_error!(InternalError, "Unable to get size")
        // })?;
        // let final_size: u64 = final_size_recv.try_recv().map_err(|_| {
        //     error!(error = "Unable to get size");
        //     s3_error!(InternalError, "Unable to get size")
        // })?;

        // new_object.hashes = vec![
        //     (
        //         "MD5".to_string(),
        //         md5_initial.clone().ok_or_else(|| {
        //             error!(error = "Unable to get md5 hash initial data");
        //             s3_error!(InternalError, "Unable to get md5 hash initial data")
        //         })?,
        //     ),
        //     (
        //         "SHA256".to_string(),
        //         sha_initial.clone().ok_or_else(|| {
        //             error!(error = "Unable to get sha hash initial data");
        //             s3_error!(InternalError, "Unable to get sha hash initial data")
        //         })?,
        //     ),
        // ]
        // .into_iter()
        // .collect::<HashMap<String, String>>();

        // let hashes = vec![
        //     Hash {
        //         alg: Hashalgorithm::Sha256.into(),
        //         hash: sha_initial.clone().ok_or_else(|| {
        //             error!(error = "Unable to get sha hash initial data");
        //             s3_error!(InternalError, "Unable to get sha hash initial data")
        //         })?,
        //     },
        //     Hash {
        //         alg: Hashalgorithm::Md5.into(),
        //         hash: md5_initial.clone().ok_or_else(|| {
        //             error!(error = "Unable to get md5 hash initial data");
        //             s3_error!(InternalError, "Unable to get md5 hash initial data")
        //         })?,
        //     },
        // ];

        // location.raw_content_len = initial_size as i64;
        // location.disk_content_len = final_size as i64;
        // location.disk_hash = Some(sha_final.clone());

        // trace!("finishing object");
        // if let Some(handler) = self.cache.aruna_client.read().await.as_ref() {
        //     if let Some(token) = &impersonating_token {
        //         if was_init {
        //             new_object = handler
        //                 .finish_object(
        //                     new_object.id,
        //                     location.raw_content_len,
        //                     hashes.clone(),
        //                     token,
        //                 )
        //                 .await
        //                 .map_err(|_| {
        //                     error!(error = "Unable to finish object");
        //                     s3_error!(InternalError, "Unable to finish object")
        //                 })?;
        //         } else {
        //             new_object = handler
        //                 .create_and_finish(new_object.clone(), location.raw_content_len, token)
        //                 .await
        //                 .map_err(|e| {
        //                     error!(error = ?e, "Unable to create and finish object");
        //                     s3_error!(InvalidObjectState, "{}", e.to_string())
        //                 })?;
        //         }
        //     }
        // }

        // self.cache
        //     .add_location_with_binding(new_object.id, location)
        //     .await
        //     .map_err(|e| {
        //         error!(error = ?e, msg = "Unable to add location with binding");
        //         s3_error!(InternalError, "Unable to add location with binding")
        //     })?;

        // let output = PutObjectOutput {
        //     e_tag: md5_initial,
        //     checksum_sha256: sha_initial,
        //     version_id: Some(new_object.id.to_string()),
        //     ..Default::default()
        // };
        // debug!(?output);

        // let mut resp = S3Response::new(output);
        // if let Some(headers) = headers {
        //     for (k, v) in headers {
        //         resp.headers.insert(
        //             HeaderName::from_bytes(k.as_bytes())
        //                 .map_err(|_| s3_error!(InternalError, "Unable to parse header name"))?,
        //             HeaderValue::from_str(&v)
        //                 .map_err(|_| s3_error!(InternalError, "Unable to parse header value"))?,
        //         );
        //     }
        // }

        // Ok(resp)
    }
}
