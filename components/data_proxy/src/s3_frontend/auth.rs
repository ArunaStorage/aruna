use crate::caching::cache::Cache;
use s3s::{
    auth::{S3Auth, S3AuthContext, SecretKey},
    s3_error, S3Result,
};
use std::sync::Arc;

/// Aruna authprovider
pub struct AuthProvider {
    cache: Arc<Cache>,
}

impl AuthProvider {
    pub async fn new(cache: Arc<Cache>) -> Self {
        Self { cache }
    }
}

#[async_trait::async_trait]
impl S3Auth for AuthProvider {
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        Ok(self
            .cache
            .get_secret(access_key)
            .map_err(|_| s3_error!(AccessDenied, "Invalid access key"))?)
    }

    async fn check_access(&self, cx: &mut S3AuthContext<'_>) -> S3Result<()> {
        if cx.method() == "GET" {}

        match cx.credentials() {
            Some(cred) => match self.cache.auth.read().await.as_ref() {
                Some(auth) => auth.check_permissions(token),
                None => Ok(()),
            },
            None => Err(s3_error!(AccessDenied, "Signature is required")),
        }
    }
}
