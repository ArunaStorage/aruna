use crate::{
    client::ServerClient, config::Backend, error::ProxyError, lmdbstore::LmdbStore, CONFIG,
};
use aruna_server::models::{
    models::{Hash, HashAlgorithm, ResourceVariant},
    requests::RegisterDataRequest,
};
use futures::{StreamExt, TryStreamExt};
use md5::{Digest, Md5};
use opendal::{services, Operator};
use s3s::{
    dto::{CreateBucketInput, CreateBucketOutput, PutObjectInput, PutObjectOutput},
    s3_error, S3Request, S3Response, S3Result, S3,
};
use sha2::Sha256;
use std::sync::Arc;
use tracing::error;

use super::utils::{
    ensure_parts_exists, finish_data_upload, get_operator, permute_path, token_from_credentials,
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

        // Ensure that all parts exist and init the location in storage
        let (object_id, location) = ensure_parts_exists(
            &self.storage,
            &self.client,
            parts,
            parent_id,
            &user_token,
            &req.input.bucket,
            &req.input.key,
        )
        .await?;

        // Get the operator (this can be anything, s3, file, etc.)
        let op = get_operator(location)?;

        // Create a new md5 and sha256 hasher
        let mut md5_hash = Md5::new();
        let mut sha256_hash = Sha256::new();

        // Get a writer from the operator
        let mut writer = op
            .writer(&format!("{}/{}", &req.input.bucket, &req.input.key))
            .await
            .map_err(ProxyError::from)?;
        let body = req.input.body.ok_or_else(|| {
            error!("Body is missing");
            s3_error!(InvalidRequest, "Body is missing")
        })?;

        // Inspect the body and update the hashes
        let mut stream = body.inspect_ok(|bytes| {
            md5_hash.update(bytes.as_ref());
            sha256_hash.update(bytes.as_ref());
        });

        // Write the body to the operator
        while let Some(body) = stream.next().await {
            let body = body.map_err(|e| ProxyError::BodyError(e.to_string()))?;
            writer.write(body).await.map_err(ProxyError::from)?;
        }

        // Close the writer
        writer.close().await.map_err(ProxyError::from)?;

        // Finalize the hashes
        let md5_final = hex::encode(md5_hash.finalize());
        let sha_final = hex::encode(sha256_hash.finalize());

        finish_data_upload(
            &self.client,
            object_id,
            md5_final.clone(),
            sha_final.clone(),
            &user_token,
        )
        .await?;

        let output = PutObjectOutput {
            e_tag: Some(md5_final),
            checksum_sha256: Some(sha_final),
            version_id: Some(object_id.to_string()),
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }
}
