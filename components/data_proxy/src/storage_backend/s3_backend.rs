use std::{env, str::FromStr};

use async_channel::{Receiver, Sender};
use aws_sdk_s3::{types::ByteStream, Client, Endpoint, Region};
use http::Uri;
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::api::aruna::api::internal::proxy::v1::Location;

const DOWNLOAD_CHUNK_SIZE: usize = 500000;
const S3_ENDPOINT_HOST_ENV_VAR: &str = "S3_ENDPOINT_HOST";

#[derive(Debug)]
pub struct S3Backend {
    pub s3_client: Client,
}

// Data backend for an S3 based storage.
impl S3Backend {
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let endpoint =
            env::var(S3_ENDPOINT_HOST_ENV_VAR).unwrap_or("http://localhost:9000".to_string());

        let config = aws_config::load_from_env().await;
        let s3_config = aws_sdk_s3::config::Builder::from(&config)
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
        content_len: i64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let hyper_body = hyper::Body::wrap_stream(recv);
        let bytestream = ByteStream::from(hyper_body);

        match self
            .s3_client
            .put_object()
            .set_bucket(Some(bucket))
            .set_key(Some(key))
            .set_content_length(Some(content_len))
            .body(bytestream)
            .send()
            .await
        {
            Ok(_) => {}
            Err(err) => {
                log::error!("{}", err);
                return Err(Box::new(err));
            }
        };

        Ok(())
    }

    // Downloads the given object from the s3 storage
    // The body is wrapped into an async reader and reads the data in chunks.
    // The chunks are then transfered into the sender.
    pub async fn download(&self, bucket: String, key: String, sender: Sender<bytes::Bytes>) {
        let object = self
            .s3_client
            .get_object()
            .set_bucket(Some(bucket))
            .set_key(Some(key))
            .send()
            .await
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
                };

                buf_len
            };

            if consumed_len == 0 {
                break;
            };

            buf_reader.consume(consumed_len);
        }
    }

    // Initiates a multipart upload in s3 and returns the associated upload id.
    pub async fn init_multipart_upload(
        &self,
        location: Location,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync + 'static>> {
        let multipart = self
            .s3_client
            .create_multipart_upload()
            .set_bucket(Some(location.bucket))
            .set_key(Some(location.path))
            .send()
            .await
            .unwrap();

        return Ok(multipart.upload_id().unwrap().to_string());
    }
}
