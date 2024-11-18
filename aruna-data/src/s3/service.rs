use crate::{config::Config, lmdbstore::LmdbStore};
use s3s::S3;
use std::sync::Arc;

pub struct ArunaS3Service {
    storage: Arc<LmdbStore>,
    config: Arc<Config>,
}

impl ArunaS3Service {
    pub fn new(storage: Arc<LmdbStore>, config: Arc<Config>) -> Self {
        Self { storage, config }
    }
}

#[async_trait::async_trait]
impl S3 for ArunaS3Service {}
