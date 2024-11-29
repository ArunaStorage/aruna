use std::sync::Arc;

use s3s::{
    access::{S3Access, S3AccessContext},
    s3_error, S3Result,
};

use crate::{client::ServerClient, lmdbstore::LmdbStore};

pub struct AccessChecker {
    store: Arc<LmdbStore>,
    client: ServerClient,
}

impl AccessChecker {
    pub fn new(store: Arc<LmdbStore>, client: ServerClient) -> Self {
        Self { store, client }
    }
}

#[async_trait::async_trait]
impl S3Access for AccessChecker {
    async fn check(&self, _cx: &mut S3AccessContext<'_>) -> S3Result<()> {
        Ok(())
    }
}
