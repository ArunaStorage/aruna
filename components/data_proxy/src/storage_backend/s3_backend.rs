use std::{ env, str::FromStr };

use async_channel::{ Receiver, Sender };
use aws_sdk_s3::{
    model::{ CompletedMultipartUpload, CompletedPart },
    types::{ ByteStream, SdkError },
    Client,
    Endpoint,
    Region,
    error::CreateBucketError,
};
use http::Uri;
use tokio::io::{ AsyncBufReadExt, BufReader };

use std::convert::TryFrom;

use crate::api::aruna::api::internal::proxy::v1::{ Location, PartETag };

const DOWNLOAD_CHUNK_SIZE: usize = 500000;
const S3_ENDPOINT_HOST_ENV_VAR: &str = "S3_ENDPOINT_HOST";

#[derive(Debug)]
pub struct S3Backend {
    pub s3_client: Client,
}

// Data backend for an S3 based storage.
impl S3Backend {
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let endpoint = env
            ::var(S3_ENDPOINT_HOST_ENV_VAR)
            .unwrap_or("http://localhost:9000".to_string());

        let config = aws_config::load_from_env().await;
        let s3_config = aws_sdk_s3::config::Builder
            ::from(&config)
            .region(Region::new("RegionOne"))
            .endpoint_resolver(Endpoint::immutable(Uri::from_str(endpoint.as_str())?))
            .build();

        let s3_client = aws_sdk_s3::Client::from_conf(s3_config);

        let handler = S3Backend {
            s3_client: s3_client,
        };
        return Ok(handler);
    }

    // Uploads a single object in chunks
    // Objects are uploaded in chunks that come from a channel to allow modification in the data middleware
    // The receiver can directly will be wrapped and will then be directly passed into the s3 client
    pub async fn upload_object(
        &self,
        recv: Receiver<Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync + 'static>>>,
        bucket: String,
        key: String,
        content_len: i64
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        self.check_and_create_bucket(bucket.clone()).await?;

        let hyper_body = hyper::Body::wrap_stream(recv);
        let bytestream = ByteStream::from(hyper_body);

        match
            self.s3_client
                .put_object()
                .set_bucket(Some(bucket))
                .set_key(Some(key))
                .set_content_length(Some(content_len))
                .body(bytestream)
                .send().await
        {
            Ok(_) => {}
            Err(err) => {
                log::error!("{}", err);
                return Err(Box::new(err));
            }
        }

        Ok(())
    }

    // Downloads the given object from the s3 storage
    // The body is wrapped into an async reader and reads the data in chunks.
    // The chunks are then transfered into the sender.
    pub async fn download(&self, bucket: String, key: String, sender: Sender<bytes::Bytes>) {
        let object = self.s3_client
            .get_object()
            .set_bucket(Some(bucket))
            .set_key(Some(key))
            .send().await
            .unwrap();

        let body_reader = object.body.into_async_read();

        let mut buf_reader = BufReader::with_capacity(DOWNLOAD_CHUNK_SIZE, body_reader);

        loop {
            let consumed_len = {
                let buffer_result = buf_reader.fill_buf().await;
                let buf = buffer_result.unwrap();
                let buf_len = buf.len().clone();
                let bytes_buf = bytes::Bytes::copy_from_slice(buf);

                match sender.send(bytes_buf).await {
                    Ok(_) => {}
                    Err(err) => {
                        log::error!("{}", err);
                        break;
                    }
                }

                buf_len
            };

            if consumed_len == 0 {
                break;
            }

            buf_reader.consume(consumed_len);
        }
    }

    // Initiates a multipart upload in s3 and returns the associated upload id.
    pub async fn init_multipart_upload(
        &self,
        location: Location
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync + 'static>> {
        self.check_and_create_bucket(location.bucket.clone()).await?;

        let multipart = self.s3_client
            .create_multipart_upload()
            .set_bucket(Some(location.bucket))
            .set_key(Some(location.path))
            .send().await
            .unwrap();

        return Ok(multipart.upload_id().unwrap().to_string());
    }

    pub async fn upload_multi_object(
        &self,
        recv: Receiver<Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync + 'static>>>,
        bucket: String,
        key: String,
        upload_id: String,
        content_len: i64,
        part_number: i32
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync + 'static>> {
        let hyper_body = hyper::Body::wrap_stream(recv);
        let bytestream = ByteStream::from(hyper_body);

        let upload = self.s3_client
            .upload_part()
            .set_bucket(Some(bucket))
            .set_key(Some(key))
            .set_part_number(Some(part_number))
            .set_content_length(Some(content_len))
            .set_upload_id(Some(upload_id))
            .body(bytestream)
            .send().await?;

        return Ok(upload.e_tag().unwrap().to_string());
    }

    pub async fn finish_multipart_upload(
        &self,
        parts: Vec<PartETag>,
        bucket: String,
        key: String,
        upload_id: String
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let mut completed_parts = Vec::new();
        for etag in parts {
            let part_number = i32::try_from(etag.part_number)?;

            let completed_part = CompletedPart::builder()
                .e_tag(etag.etag)
                .part_number(part_number)
                .build();

            completed_parts.push(completed_part);
        }

        log::info!("{:?}", completed_parts);

        self.s3_client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder().set_parts(Some(completed_parts)).build()
            )
            .send().await?;

        return Ok(());
    }

    pub async fn create_bucket(
        &self,
        bucket: String
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        self.check_and_create_bucket(bucket).await
    }

    pub async fn check_and_create_bucket(
        &self,
        bucket: String
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        match self.s3_client.get_bucket_location().bucket(bucket.clone()).send().await {
            Ok(_) => Ok(()),
            Err(_) => {
                match self.s3_client.create_bucket().bucket(bucket).send().await {
                    Ok(_) => {
                        return Ok(());
                    }
                    Err(err) => {
                        log::error!("{}", err);
                        return Err(Box::new(err));
                    }
                }
            }
        }
    }
}