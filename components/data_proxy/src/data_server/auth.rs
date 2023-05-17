use anyhow::Result;
use aruna_rust_api::api::internal::v1::{
    internal_authorize_service_client::InternalAuthorizeServiceClient, GetSecretRequest,
};
use s3s::{
    auth::{S3Auth, SecretKey},
    s3_error, S3Result,
};
use tonic::Request;

/// Aruna authprovider
#[derive(Debug)]
pub struct AuthProvider {
    aruna_endpoint: tonic::transport::Endpoint,
}

impl AuthProvider {
    pub async fn new(aruna_url: String) -> Result<Self> {
        Ok(Self {
            aruna_endpoint: tonic::transport::Endpoint::try_from(aruna_url)?,
        })
    }
}

#[async_trait::async_trait]
impl S3Auth for AuthProvider {
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        let secret = InternalAuthorizeServiceClient::connect(self.aruna_endpoint.clone())
            .await
            .map_err(|_| s3_error!(NotSignedUp, "Unable to authenticate user"))?
            .get_secret(Request::new(GetSecretRequest {
                accesskey: access_key.to_string(),
            }))
            .await
            .map_err(|_| s3_error!(NotSignedUp, "Unable to authenticate user"))?
            .into_inner()
            .authorization
            .ok_or_else(|| s3_error!(NotSignedUp, "Unable to authenticate user"))?
            .secretkey;
        Ok(secret.into())
    }
}
