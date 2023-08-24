use crate::structs::Object;
use crate::structs::ObjectLocation;
use crate::structs::PartETag;

use super::storage_backend::StorageBackend;
use anyhow::anyhow;
use anyhow::Result;
use async_channel::{Receiver, Sender};
use async_trait::async_trait;
use aws_sdk_s3::{
    config::Region,
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart},
    Client,
};
use diesel_ulid::DieselUlid;
use rand::distributions::Alphanumeric;
use rand::distributions::DistString;
use rand::thread_rng;
use rand::Rng;
use tokio_stream::StreamExt;

#[derive(Debug, Clone)]
pub struct S3Backend {
    pub s3_client: Client,
    endpoint_id: String,
}

impl S3Backend {
    pub async fn new(endpoint_id: String) -> Result<Self> {
        let s3_endpoint = dotenvy::var("AWS_S3_HOST").unwrap();

        let config = aws_config::load_from_env().await;
        let s3_config = aws_sdk_s3::config::Builder::from(&config)
            .region(Region::new("RegionOne"))
            .endpoint_url(&s3_endpoint)
            .build();

        let s3_client = Client::from_conf(s3_config);

        let handler = S3Backend {
            s3_client,
            endpoint_id,
        };
        Ok(handler)
    }
}

// Data backend for an S3 based storage.
#[async_trait]
impl StorageBackend for S3Backend {
    // Uploads a single object in chunks
    // Objects are uploaded in chunks that come from a channel to allow modification in the data middleware
    // The receiver can directly will be wrapped and will then be directly passed into the s3 client
    async fn put_object(
        &self,
        recv: Receiver<Result<bytes::Bytes>>,
        location: ObjectLocation,
        content_len: i64,
    ) -> Result<()> {
        self.check_and_create_bucket(location.bucket.clone())
            .await?;

        let hyper_body = hyper::Body::wrap_stream(recv);
        let bytestream = ByteStream::from(hyper_body);

        match self
            .s3_client
            .put_object()
            .set_bucket(Some(location.bucket))
            .set_key(Some(location.key))
            .set_content_length(Some(content_len))
            .body(bytestream)
            .send()
            .await
        {
            Ok(_) => {}
            Err(err) => {
                log::error!("{}", err);
                return Err(err.into());
            }
        }

        Ok(())
    }

    // Downloads the given object from the s3 storage
    // The body is wrapped into an async reader and reads the data in chunks.
    // The chunks are then transferred into the sender.
    async fn get_object(
        &self,
        location: ObjectLocation,
        range: Option<String>,
        sender: Sender<Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync>>>,
    ) -> Result<()> {
        let object = self
            .s3_client
            .get_object()
            .set_bucket(Some(location.bucket))
            .set_key(Some(location.key))
            .set_range(range);

        let mut object_request = match object.send().await {
            Ok(value) => value,
            Err(err) => {
                log::error!("{}", err);
                return Err(err.into());
            }
        };

        while let Some(bytes) = object_request.body.next().await {
            sender.send(Ok(bytes?)).await?;
        }
        return Ok(());
    }

    async fn head_object(&self, location: ObjectLocation) -> Result<i64> {
        let object = self
            .s3_client
            .head_object()
            .set_bucket(Some(location.bucket))
            .set_key(Some(location.key))
            .send()
            .await;

        Ok(object?.content_length())
    }

    // Initiates a multipart upload in s3 and returns the associated upload id.
    async fn init_multipart_upload(&self, location: ObjectLocation) -> Result<String> {
        self.check_and_create_bucket(location.bucket.clone())
            .await?;

        let multipart = self
            .s3_client
            .create_multipart_upload()
            .set_bucket(Some(location.bucket))
            .set_key(Some(location.key))
            .send()
            .await?;

        return Ok(multipart.upload_id().unwrap().to_string());
    }

    async fn upload_multi_object(
        &self,
        recv: Receiver<Result<bytes::Bytes>>,
        location: ObjectLocation,
        upload_id: String,
        content_len: i64,
        part_number: i32,
    ) -> Result<PartETag> {
        log::debug!("Submitted content-length was: {:#?}", content_len);
        let hyper_body = hyper::Body::wrap_stream(recv);
        let bytestream = ByteStream::from(hyper_body);

        let upload = self
            .s3_client
            .upload_part()
            .set_bucket(Some(location.bucket))
            .set_key(Some(location.key))
            .set_part_number(Some(part_number))
            .set_content_length(Some(content_len))
            .set_upload_id(Some(upload_id))
            .body(bytestream)
            .send()
            .await?;

        return Ok(PartETag {
            part_number,
            etag: upload.e_tag.ok_or_else(|| anyhow!("Missing etag"))?,
        });
    }

    async fn finish_multipart_upload(
        &self,
        location: ObjectLocation,
        parts: Vec<PartETag>,
        upload_id: String,
    ) -> Result<()> {
        let mut completed_parts = Vec::new();
        for etag in parts {
            let part_number = etag.part_number;

            let completed_part = CompletedPart::builder()
                .e_tag(etag.etag.replace('-', ""))
                .part_number(part_number)
                .build();

            completed_parts.push(completed_part);
        }

        match self
            .s3_client
            .complete_multipart_upload()
            .bucket(location.bucket)
            .key(location.key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(completed_parts))
                    .build(),
            )
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                log::error!("{:?}", e.raw_response());
                Err(e.into())
            }
        }
    }

    async fn create_bucket(&self, bucket: String) -> Result<()> {
        self.check_and_create_bucket(bucket).await
    }

    /// Delete a object from the storage system
    /// # Arguments
    /// * `location` - The location of the object
    async fn delete_object(&self, location: ObjectLocation) -> Result<()> {
        self.s3_client
            .delete_object()
            .bucket(location.bucket)
            .key(location.key)
            .send()
            .await?;
        Ok(())
    }

    /// Initialize a new location for a specific object
    /// This takes the object_info into account and creates a new location for the object
    async fn initialize_location(
        &self,
        _obj: &Object,
        expected_size: Option<i64>,
        ex_bucket: Option<String>,
    ) -> Result<ObjectLocation> {
        let key: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(30)
            .map(char::from)
            .collect();

        // TODO: READ setting and set this based on settings
        let encryption_key: String = Alphanumeric.sample_string(&mut rand::thread_rng(), 32);

        Ok(ObjectLocation {
            id: DieselUlid::generate(),
            bucket: match ex_bucket {
                Some(bucket) => bucket,
                None => self.get_random_bucket().to_ascii_lowercase(),
            },
            upload_id: None,
            key,
            encryption_key: Some(encryption_key),
            compressed: true,
            raw_content_len: expected_size.unwrap_or_default(),
            disk_content_len: 0,
        })
    }
}

impl S3Backend {
    pub async fn check_and_create_bucket(&self, bucket: String) -> Result<()> {
        match self
            .s3_client
            .get_bucket_location()
            .bucket(bucket.clone())
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(_) => match self.s3_client.create_bucket().bucket(bucket).send().await {
                Ok(_) => Ok(()),
                Err(err) => {
                    log::error!("{}", err);
                    Err(err.into())
                }
            },
        }
    }

    pub fn get_random_bucket(&self) -> String {
        let bucket: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(2)
            .map(char::from)
            .collect();
        format!("{}-{}", self.endpoint_id, bucket)
    }
}
