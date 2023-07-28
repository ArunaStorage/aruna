use crate::caching::cache::Cache;
use s3s::{
    auth::{S3Auth, S3AuthContext, SecretKey},
    s3_error, S3Result,
};
use std::sync::Arc;

/// Aruna authprovider
#[derive(Debug)]
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
        self.cache.get_secret(access_key)
    }

    async fn check_access(&self, cx: &mut S3AuthContext<'_>) -> S3Result<()> {
        match cx.credentials() {
            Some(_) => Ok(()),
            None => Err(s3_error!(AccessDenied, "Signature is required")),
        }
    }
}
