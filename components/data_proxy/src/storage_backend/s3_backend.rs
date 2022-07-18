use async_channel::Receiver;
use async_trait::async_trait;
use aws_sdk_s3::{types::ByteStream, Client, Endpoint, Region};
use aws_smithy_http::body::SdkBody;
use http::Uri;

use super::storage_backend::StorageBackend;

pub struct S3Backend {
    pub s3_client: Client,
}

impl S3Backend {
    pub async fn new() -> Self {
        let config = aws_config::load_from_env().await;
        let s3_config = aws_sdk_s3::config::Builder::from(&config)
            .region(Region::new("RegionOne"))
            .endpoint_resolver(Endpoint::immutable(Uri::from_static(
                "http://127.0.0.1:9000",
            )))
            .build();

        let s3_client = aws_sdk_s3::Client::from_conf(s3_config);

        let handler = S3Backend {
            s3_client: s3_client,
        };
        return handler;
    }
}

//Box<(dyn futures::Stream<Item = Result<bytes::Bytes, Box<(dyn std::error::Error + Sync + std::marker::Send + 'static)>>> + std::marker::Send + 'static)>>
#[async_trait]
impl StorageBackend for S3Backend {
    async fn upload_object(
        &self,
        recv: Receiver<Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync + 'static>>>,
        bucket: String,
        key: String,
        content_len: i64,
    ) {
        let hyper_body = hyper::Body::wrap_stream(recv);
        let sdk_body = SdkBody::from(hyper_body);
        let bytestream = ByteStream::from(sdk_body);

        self.s3_client
            .put_object()
            .set_bucket(Some(bucket))
            .set_key(Some(key))
            .set_content_length(Some(content_len))
            .body(bytestream)
            .send()
            .await
            .unwrap();
    }

    async fn create_object_key(&self, object_id: String, revision_id: String) -> String {
        return format!("{}/{}", object_id, revision_id);
    }
}
