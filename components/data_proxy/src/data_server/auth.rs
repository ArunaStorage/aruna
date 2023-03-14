use anyhow::Result;
use aruna_rust_api::api::internal::v1::{
    internal_authorize_service_client::InternalAuthorizeServiceClient, GetSecretRequest,
};
use s3s::{
    auth::{S3Auth, SecretKey},
    s3_error, S3Result,
};
use tonic::transport::Channel;
use tonic::Request;

/// Aruna authprovider
#[derive(Debug)]
pub struct AuthProvider {
    client: InternalAuthorizeServiceClient<Channel>,
}

impl AuthProvider {
    pub async fn new(aruna_url: impl Into<String>) -> Result<Self> {
        let client = InternalAuthorizeServiceClient::connect(aruna_url.into()).await?;

        Ok(Self { client })
    }
}

#[async_trait::async_trait]
impl S3Auth for AuthProvider {
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        let secret = self
            .client
            .clone()
            .get_secret(Request::new(GetSecretRequest {
                accesskey: access_key.to_string(),
            }))
            .await
            .map_err(|_| s3_error!(NotSignedUp, "Unable to authenticate user"))?
            .into_inner()
            .authorization
            .ok_or(s3_error!(NotSignedUp, "Unable to authenticate user"))?
            .accesskey;
        Ok(secret.into())
    }
}
