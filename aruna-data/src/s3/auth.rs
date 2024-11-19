use crate::lmdbstore::LmdbStore;
use s3s::{
    auth::{S3Auth, SecretKey},
    S3Result,
};
use std::sync::Arc;

pub struct AuthProvider {
    database: Arc<LmdbStore>,
}

#[async_trait::async_trait]
impl S3Auth for AuthProvider {
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        // Get shared secret for server and proxy

        todo!();
        //Ok(SecretKey::from(key))
    }
}

fn get_shared_secret(database: &Arc<LmdbStore>, access_key: &str) -> Option<String> {
    None
}
