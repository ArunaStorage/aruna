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
    dto::{PutObjectInput, PutObjectOutput},
    s3_error, S3Request, S3Response, S3Result, S3,
};
use sha2::Sha256;
use std::sync::Arc;
use tracing::error;

use super::utils::{permute_path, sign_user_token};

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
    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let parts: Vec<String> = permute_path(&req.input.bucket, &req.input.key);

        let mut parent_id = self
            .storage
            .get_object_id(&req.input.bucket)
            .ok_or_else(|| {
                error!("Bucket not found");
                s3_error!(NoSuchBucket, "Bucket not found")
            })?;

        let project_id = parent_id;

        let user_token = sign_user_token(
            CONFIG.proxy.get_encoding_key()?,
            CONFIG.proxy.endpoint_id,
            req.credentials.map(|c| c.access_key).ok_or_else(|| {
                error!("Access key is missing");
                s3_error!(InvalidAccessKeyId, "Access key is missing")
            })?,
        )?;

        let parts_len = parts.len() - 1;
        for (i, part) in parts.into_iter().enumerate() {
            parent_id = if let Some(exists) = self.storage.get_object_id(&part) {
                exists
            } else {
                let variant = if i != parts_len {
                    ResourceVariant::Folder
                } else {
                    ResourceVariant::Object
                };
                let name = part.split("/").last().ok_or_else(|| {
                    error!("Invalid path");
                    s3_error!(InternalError, "Invalid path")
                })?;
                let id = self
                    .client
                    .create_object(name, variant.clone(), parent_id, &user_token)
                    .await?;

                if variant == ResourceVariant::Object {
                    self.storage.put_object(
                        id,
                        &req.input.key,
                        crate::structs::ObjectInfo::Object {
                            location: crate::structs::StorageLocation::S3 {
                                bucket: "aruna".to_string(),
                                key: format!("{}/{}", &req.input.bucket, &req.input.key),
                            },
                            storage_format: crate::structs::StorageFormat::Raw,
                            project: project_id,
                            revision_number: 0,
                            public: false,
                        },
                    )?;
                } else {
                    self.storage.put_key(id, &part)?;
                }
                id
            };
        }
        // Rename the variable to avoid confusion
        let object_id = parent_id;

        let Backend::S3 {
            host: Some(host),
            access_key: Some(access_key),
            secret_key: Some(secret_key),
            ..
        } = &CONFIG.backend
        else {
            return Err(s3_error!(InternalError, "Invalid backend"));
        };

        let builder = services::S3::default()
            .bucket("aruna")
            .endpoint(host)
            .access_key_id(access_key)
            .secret_access_key(secret_key);

        // Init an operator
        let op = Operator::new(builder).map_err(ProxyError::from)?.finish();

        let mut md5_hash = Md5::new();
        let mut sha256_hash = Sha256::new();

        let mut writer = op
            .writer(&format!("{}/{}", &req.input.bucket, &req.input.key))
            .await
            .map_err(ProxyError::from)?;
        let body = req.input.body.ok_or_else(|| {
            error!("Body is missing");
            s3_error!(InvalidRequest, "Body is missing")
        })?;

        let mut stream = body.inspect_ok(|bytes| {
            md5_hash.update(bytes.as_ref());
            sha256_hash.update(bytes.as_ref());
        });

        while let Some(body) = stream.next().await {
            let body = body.map_err(|e| ProxyError::BodyError(e.to_string()))?;
            writer.write(body).await.map_err(ProxyError::from)?;
        }

        writer.close().await.map_err(ProxyError::from)?;

        let md5_final = hex::encode(md5_hash.finalize());
        let sha_final = hex::encode(sha256_hash.finalize());

        self.client
            .add_data(
                object_id,
                RegisterDataRequest {
                    object_id,
                    component_id: CONFIG.proxy.endpoint_id,
                    hashes: vec![
                        Hash {
                            algorithm: HashAlgorithm::MD5,
                            value: md5_final.clone(),
                        },
                        Hash {
                            algorithm: HashAlgorithm::Sha256,
                            value: sha_final.clone(),
                        },
                    ],
                },
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
