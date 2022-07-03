use std::time::Duration;

use aws_sdk_s3::{presigning::config::PresigningConfig, Client, Endpoint};
use http::Uri;

pub struct S3Handler {
    pub s3_client: Client,
}

impl S3Handler {
    pub async fn new() -> Self {
        let config = aws_config::load_from_env().await;
        let s3_config = aws_sdk_s3::config::Builder::from(&config)
            .endpoint_resolver(Endpoint::immutable(Uri::from_static(
                "http://127.0.0.1:9001",
            )))
            .build();

        let s3_client = aws_sdk_s3::Client::from_conf(s3_config);

        let handler = S3Handler {
            s3_client: s3_client,
        };
        return handler;
    }

    pub async fn create_bucket(&self, collection_id: uuid::Uuid) {
        self.s3_client
            .create_bucket()
            .bucket(collection_id.to_string())
            .send()
            .await
            .unwrap();
    }

    pub async fn create_object(&self, collection_id: uuid::Uuid, object_id: uuid::Uuid) -> String {
        let expires_in = Duration::from_secs(3600);

        let presigned_req = self
            .s3_client
            .put_object()
            .bucket(collection_id.to_string())
            .key(object_id.to_string())
            .presigned(PresigningConfig::expires_in(expires_in).unwrap())
            .await
            .unwrap();

        return presigned_req.uri().to_string();
    }

    pub async fn get_object_presigned_get_link(
        &self,
        collection_id: uuid::Uuid,
        object_id: uuid::Uuid,
    ) -> String {
        let expires_in = Duration::from_secs(3600);
        let presigned_req = self
            .s3_client
            .get_object()
            .bucket(collection_id.to_string())
            .key(object_id.to_string())
            .presigned(PresigningConfig::expires_in(expires_in).unwrap())
            .await
            .unwrap();

        return presigned_req.uri().to_string();
    }
}

mod test {
    use super::S3Handler;

    #[tokio::test]
    async fn my_test() {
        let s3_handler = S3Handler::new().await;
        s3_handler.create_bucket(uuid::Uuid::new_v4()).await;
    }
}
