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
        match self.cache.auth.read().await.as_ref() {
            Some(auth) => {
                let result = auth
                    .check_access(cx.credentials(), cx.method(), cx.s3_path())
                    .await
                    .map_err(|_| s3_error!(AccessDenied, "Access denied"))?;

                cx.extensions_mut().insert(result);
                Ok(())
            }
            None => Ok(()),
        }
    }
}
