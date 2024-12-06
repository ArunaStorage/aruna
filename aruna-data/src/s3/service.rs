use crate::{client::ServerClient, error::ProxyError, lmdbstore::LmdbStore, structs::ObjectInfo};
use s3s::{
    dto::{
        CreateBucketInput, CreateBucketOutput, GetObjectInput, GetObjectOutput, HeadObjectInput, HeadObjectOutput, PutObjectInput, PutObjectOutput, StreamingBlob
    },
    s3_error, S3Request, S3Response, S3Result, S3,
};
use std::sync::Arc;
use tracing::error;

use super::{
    object_writer::ObjectWriter,
    utils::{
        ensure_parts_exists, finish_data_upload, get_operator, permute_path, token_from_credentials,
    },
};

pub struct ArunaS3Service {
    storage: Arc<LmdbStore>,
    client: ServerClient,
}

impl ArunaS3Service {
    pub fn new(storage: Arc<LmdbStore>, client: ServerClient) -> Self {
        Self { storage, client }
    }
}

#[async_trait::async_trait]
impl S3 for ArunaS3Service {
    #[tracing::instrument(err, skip(self, req))]
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let user_token = token_from_credentials(req.credentials.as_ref())?;
        let uid = self
            .client
            .create_project(&req.input.bucket, &user_token)
            .await?;
        self.storage.put_key(uid, &req.input.bucket)?;
        Ok(S3Response::new(CreateBucketOutput::default()))
    }

    #[tracing::instrument(err, skip(self, req))]
    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        // Split path into "parts"
        // e.g. "bucket/key/foo" -> ["bucket", "bucket/key", "bucket/key/foo"]
        let parts: Vec<String> = permute_path(&req.input.bucket, &req.input.key);

        // Query the parent id
        let parent_id = self
            .storage
            .get_object_id(&req.input.bucket)
            .ok_or_else(|| {
                error!("Bucket not found");
                s3_error!(NoSuchBucket, "Bucket not found")
            })?;

        // Sign a temp token from the credentials
        let user_token = token_from_credentials(req.credentials.as_ref())?;

        // Ensure that all parts exist and init the folder locations in storage
        let object_id =
            ensure_parts_exists(&self.storage, &self.client, parts, parent_id, &user_token).await?;

        let object_info =
            ObjectWriter::new(req.input.bucket.clone(), req.input.key.clone(), object_id)
                .write(req.input.body)
                .await?;

        finish_data_upload(
            &self.storage,
            &self.client,
            &object_id,
            &object_info,
            &user_token,
            &req.input.bucket,
            &req.input.key,
        )
        .await?;

        let output = PutObjectOutput {
            e_tag: object_info.get_md5_hash(),
            checksum_sha256: object_info.get_sha256_hash(),
            version_id: Some(object_id.to_string()),
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    #[tracing::instrument(err, skip(self, req))]
    async fn head_object(&self, req: S3Request<HeadObjectInput>) -> S3Result<S3Response<HeadObjectOutput>> {
        let (object_id, info) = self
        .storage
        .get_object(&format!("{}/{}", &req.input.bucket, &req.input.key))
        .ok_or_else(|| {
            error!("Object not found");
            s3_error!(NoSuchKey, "Object not found")
        })?;
        let token = token_from_credentials(req.credentials.as_ref())?;
        self.client.authorize(object_id, &token).await?;

        let e_tag = info.get_md5_hash();
        let ObjectInfo::Object { meta, .. } = info else {
            return Err(s3_error!(NoSuchKey, "Object not found"));
        };

        Ok(S3Response::new(HeadObjectOutput {
            e_tag,
            content_length: Some(meta.input_size as i64),
            ..Default::default()
        }))
    }

    #[tracing::instrument(err, skip(self, req))]
    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let (object_id, ObjectInfo::Object { location, meta, .. }) = self
            .storage
            .get_object(&format!("{}/{}", &req.input.bucket, &req.input.key))
            .ok_or_else(|| {
                error!("Object not found");
                s3_error!(NoSuchKey, "Object not found")
            })?
        else {
            return Err(s3_error!(NoSuchKey, "Object not found"));
        };

        let token = token_from_credentials(req.credentials.as_ref())?;
        self.client.authorize(object_id, &token).await?;

        let path = location.get_path();
        let op = get_operator(&location)?;
        let reader = op
            .reader(&path)
            .await
            .map_err(ProxyError::from)?
            .into_bytes_stream(..)
            .await
            .map_err(ProxyError::from)?;

        let streaming_blob = StreamingBlob::wrap(reader);

        Ok(S3Response::new(GetObjectOutput {
            e_tag: Some(meta.md5),
            body: Some(streaming_blob),
            content_length: Some(meta.input_size as i64),
            ..Default::default()
        }))
    }
}
