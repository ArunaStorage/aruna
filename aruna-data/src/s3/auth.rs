use crate::lmdbstore::LmdbStore;
use s3s::auth::S3Auth;
use std::sync::Arc;

pub struct AuthProvider {
    database: Arc<LmdbStore>,
}

#[async_trait::async_trait]
impl S3Auth for AuthProvider {
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        // Get shared secret for server and proxy

        Ok(SecretKey::from(key))
    }
}

fn get_shared_secret(database: &Arc<LmdbStore>, access_key: &str) -> Option<String> {
    let key = database.get(access_key);
    key
}
