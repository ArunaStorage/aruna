use anyhow::Result;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::{config::Credentials, types::CompletedMultipartUpload, Client};
use std::sync::Arc;

use diesel_ulid::DieselUlid;

use crate::{
    auth::permission_handler::PermissionHandler,
    caching::cache::Cache,
    middlelayer::db_handler::DatabaseHandler,
};

use super::finish_request_types::FinishRequest;

impl DatabaseHandler {
    pub async fn complete_multipart_upload(
        &self,
        request: FinishRequest,
        user_id: DieselUlid,
        cache: Arc<Cache>,
        authorizer: Arc<PermissionHandler>,
        token: Option<DieselUlid>,
    ) -> Result<()> {
        // Get staging object meta info
        let (project_id, bucket_name, key) =
            DatabaseHandler::get_path(request.get_object_id()?, cache.clone()).await?;

        // Get endpoint
        let endpoint = self.get_fullsync_endpoint(project_id).await?;

        let (_, endpoint_s3_url, _, credentials) =
            DatabaseHandler::get_or_create_credentials(authorizer, user_id, token, endpoint, true)
                .await?;

        // Impersonate User for CompleteMultiPartUpload at endpoint_s3_url
        let creds = Credentials::new(
            credentials.access_key,
            credentials.secret_key,
            None,
            None,
            "ARUNA_SERVER", // Endpoint name?
        );
        let config = aws_config::defaults(BehaviorVersion::v2024_03_28())
            .credentials_provider(creds)
            .load()
            .await;
        let s3_config = aws_sdk_s3::config::Builder::from(&config)
            .region(Region::new("RegionOne"))
            .endpoint_url(endpoint_s3_url)
            .build();

        let s3_client = Client::from_conf(s3_config);

        let completed_multipart_upload: CompletedMultipartUpload =
            CompletedMultipartUpload::builder()
                .set_parts(Some(request.get_parts()))
                .build();

        let _res = s3_client
            .complete_multipart_upload()
            .bucket(&bucket_name)
            .key(&key)
            .upload_id(request.get_upload_id())
            .multipart_upload(completed_multipart_upload)
            .send()
            .await?;

        Ok(())
    }
}
