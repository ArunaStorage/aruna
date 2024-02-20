use anyhow::Result;
use async_channel::{Receiver, Sender};
use async_trait::async_trait;
use bytes::BytesMut;
use digest::Digest;
use futures_util::StreamExt;
use md5::Md5;
use rand::{
    distributions::{Alphanumeric, DistString},
    thread_rng, Rng,
};
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::pin;

use crate::structs::{Object, ObjectLocation, PartETag};

use super::storage_backend::StorageBackend;

#[derive(Debug, Clone)]
pub struct FSBackend {
    _endpoint_id: String,
    pub base_path: String,
}

impl FSBackend {
    #[tracing::instrument(level = "debug")]
    #[allow(dead_code)]
    pub async fn new(_endpoint_id: String) -> Result<Self> {
        let base_path = dotenvy::var("FS_BASEPATH")
            .map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })
            .unwrap();

        let handler = FSBackend {
            _endpoint_id,
            base_path,
        };
        Ok(handler)
    }

    #[tracing::instrument(level = "trace", skip(self, bucket))]
    pub async fn check_and_create_bucket(&self, bucket: String) -> Result<()> {
        let path = Path::new(&self.base_path).join(bucket);
        if !path.exists() {
            std::fs::create_dir_all(path).map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;
        }
        Ok(())
    }
}

// Data backend for an FS based storage.
#[async_trait]
impl StorageBackend for FSBackend {
    // Uploads a single object in chunks
    // Objects are uploaded in chunks that come from a channel to allow modification in the data middleware
    #[tracing::instrument(level = "trace", skip(self, recv, location, _content_len))]
    async fn put_object(
        &self,
        recv: Receiver<Result<bytes::Bytes>>,
        location: ObjectLocation,
        _content_len: i64,
    ) -> Result<()> {
        self.check_and_create_bucket(location.bucket.to_string())
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;

        let mut file = tokio::fs::File::create(
            Path::new(&self.base_path)
                .join(&location.bucket)
                .join(&location.key),
        )
        .await
        .map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?;

        pin!(recv);
        while let Some(data) = recv.next().await {
            let data = data.map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;
            file.write_all(&data).await.map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;
        }
        Ok(())
    }

    // Downloads the given object from the s3 storage
    // The body is wrapped into an async reader and reads the data in chunks.
    // The chunks are then transferred into the sender.
    #[tracing::instrument(level = "trace", skip(self, location, _range, sender))]
    async fn get_object(
        &self,
        location: ObjectLocation,
        _range: Option<String>,
        sender: Sender<Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync>>>,
    ) -> Result<()> {
        let file = tokio::fs::File::open(
            Path::new(&self.base_path)
                .join(&location.bucket)
                .join(&location.key),
        )
        .await
        .map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?;

        let mut reader = tokio::io::BufReader::new(file);
        let mut buf = BytesMut::with_capacity(1024 * 16);

        while reader.read_buf(&mut buf).await.is_ok() {
            sender.send(Ok(buf.split().freeze())).await.map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, location))]
    async fn head_object(&self, location: ObjectLocation) -> Result<i64> {
        let len = tokio::fs::File::open(
            Path::new(&self.base_path)
                .join(&location.bucket)
                .join(&location.key),
        )
        .await
        .map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?
        .metadata()
        .await
        .map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?
        .len() as i64;
        Ok(len)
    }

    // Initiates a multipart upload in s3 and returns the associated upload id.
    #[tracing::instrument(level = "trace", skip(self, location))]
    async fn init_multipart_upload(&self, location: ObjectLocation) -> Result<String> {
        self.check_and_create_bucket(location.bucket.clone())
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;

        let up_id: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(15)
            .map(char::from)
            .collect();

        let path = Path::new(&self.base_path).join(&up_id);
        std::fs::create_dir_all(path).map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?;

        return Ok(up_id);
    }

    #[tracing::instrument(
        level = "trace",
        skip(self, recv, _location, upload_id, _content_len, part_number)
    )]
    async fn upload_multi_object(
        &self,
        recv: Receiver<Result<bytes::Bytes>>,
        _location: ObjectLocation,
        upload_id: String,
        _content_len: i64,
        part_number: i32,
    ) -> Result<PartETag> {
        let mut file = tokio::fs::File::create(
            Path::new(&self.base_path)
                .join(&upload_id)
                .join(format!(".{}.part", part_number)),
        )
        .await
        .map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?;
        let mut md5 = Md5::new();

        pin!(recv);
        while let Some(data) = recv.next().await {
            let data = data.map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;
            md5.update(&data);
            file.write_all(&data).await.map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;
        }
        return Ok(PartETag {
            part_number,
            etag: format!("{:x}", md5.finalize()),
        });
    }

    #[tracing::instrument(level = "trace", skip(self, location, parts, upload_id))]
    async fn finish_multipart_upload(
        &self,
        location: ObjectLocation,
        parts: Vec<PartETag>,
        upload_id: String,
    ) -> Result<()> {
        self.check_and_create_bucket(location.bucket.to_string())
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;

        let mut final_file = tokio::fs::File::create(
            Path::new(&self.base_path)
                .join(&location.bucket)
                .join(&location.key),
        )
        .await
        .map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?;

        for part in parts {
            let mut file = tokio::fs::File::open(
                Path::new(&self.base_path)
                    .join(&upload_id)
                    .join(format!(".{}.part", part.part_number)),
            )
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;
            tokio::io::copy(&mut file, &mut final_file)
                .await
                .map_err(|e| {
                    tracing::error!(error = ?e, msg = e.to_string());
                    e
                })?;
        }

        // Remove the temp dir
        tokio::fs::remove_dir_all(Path::new(&self.base_path).join(&upload_id))
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, bucket))]
    async fn create_bucket(&self, bucket: String) -> Result<()> {
        self.check_and_create_bucket(bucket).await
    }

    #[tracing::instrument(level = "trace", skip(self, location))]
    /// Delete a object from the storage system
    /// # Arguments
    /// * `location` - The location of the object
    async fn delete_object(&self, location: ObjectLocation) -> Result<()> {
        tokio::fs::remove_file(
            Path::new(&self.base_path)
                .join(&location.bucket)
                .join(&location.key),
        )
        .await
        .map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, _obj, expected_size, ex_bucket, temp))]
    /// Initialize a new location for a specific object
    /// This takes the object_info into account and creates a new location for the object
    async fn initialize_location(
        &self,
        _obj: &Object,
        expected_size: Option<i64>,
        ex_bucket: Option<String>,
        temp: bool,
    ) -> Result<ObjectLocation> {
        let key: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(30)
            .map(char::from)
            .collect::<String>()
            .to_ascii_lowercase();

        let bucket: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(5)
            .map(char::from)
            .collect::<String>()
            .to_ascii_lowercase();

        let encryption_key: String = Alphanumeric.sample_string(&mut thread_rng(), 32);

        Ok(ObjectLocation {
            id: diesel_ulid::DieselUlid::generate(),
            bucket: ex_bucket.unwrap_or_else(|| bucket),
            upload_id: None,
            key,
            encryption_key: Some(encryption_key),
            compressed: !temp,
            raw_content_len: expected_size.unwrap_or_default(),
            disk_content_len: 0,
            disk_hash: None,
        })
    }
}
