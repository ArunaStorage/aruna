use std::sync::Arc;
use crate::{caching::cache::Cache, data_backends::storage_backend::StorageBackend};
use anyhow::Result;

pub struct DropBoxHandler {
    backend: Arc<Box<dyn StorageBackend>>,
    cache: Arc<Cache>,
}


impl DropBoxHandler {
    #[tracing::instrument(level = "trace", skip(backend, cache))]
    pub async fn new(
        backend: Arc<Box<dyn StorageBackend>>,
        cache: Arc<Cache>,
    ) -> Self {
        Self { backend, cache }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn run(self) -> Result<()> {
        bail!("Not implemented")
    }
}