use futures::{StreamExt, TryStreamExt};
use md5::{Digest, Md5};
use s3s::dto::StreamingBlob;
use sha2::Sha256;
use ulid::Ulid;

use crate::{
    error::ProxyError,
    structs::{Metadata, ObjectInfo},
};

use super::utils::{create_location, get_operator};

pub struct ObjectWriter {
    bucket: String,
    key: String,
    project_ulid: Ulid,
}

impl ObjectWriter {
    pub fn new(bucket: String, key: String, project_ulid: Ulid) -> Self {
        Self {
            bucket,
            key,
            project_ulid,
        }
    }

    pub async fn write(self, body: Option<StreamingBlob>) -> Result<ObjectInfo, ProxyError> {
        let location = create_location(&self.bucket, &self.key);
        // Get the operator (this can be anything, s3, file, etc.)
        let op = get_operator(&location)?;

        // Create a new md5 and sha256 hasher
        let mut md5_hash = Md5::new();
        let mut sha256_hash = Sha256::new();

        // Get a writer from the operator
        let mut writer = op
            .writer(&format!("{}/{}", &self.bucket, &self.key))
            .await
            .map_err(ProxyError::from)?;
        let body = body.ok_or_else(|| ProxyError::InternalError("No body provided".to_string()))?;

        let mut input_size = 0;
        let mut disk_size = 0;

        // Inspect the body and update the hashes
        let mut stream = body.inspect_ok(|bytes| {
            md5_hash.update(bytes.as_ref());
            sha256_hash.update(bytes.as_ref());
            input_size += bytes.len() as u64;
            disk_size += bytes.len() as u64; // For now, disk size is the same as input size
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

        let info = ObjectInfo::Object {
            storage_format: crate::structs::StorageFormat::Raw,
            location,
            meta: Metadata {
                md5: md5_final,
                sha256: sha_final,
                disk_size,
                input_size,
                content_type: "application/octet-stream".to_string(),
                last_modified: chrono::Utc::now(),
            },
            project: self.project_ulid,
            revision_number: 0, // TODO: Increment on update
            public: false,
        };

        Ok(info)
    }
}
