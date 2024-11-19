use crate::lmdbstore::LmdbStore;
use s3s::S3;
use std::sync::Arc;

pub struct ArunaS3Service {
    storage: Arc<LmdbStore>,
}

impl ArunaS3Service {
    pub fn new(storage: Arc<LmdbStore>) -> Self {
        Self { storage }
    }
}

#[async_trait::async_trait]
impl S3 for ArunaS3Service {}
