use ahash::RandomState;
use anyhow::{anyhow, Result};
use async_channel::Receiver;
use dashmap::DashSet;
use diesel_ulid::DieselUlid;
use std::sync::Arc;
use tracing::error;

use crate::{caching::cache::Cache, database::database::Database, trace_err};

pub struct ReplicationMessage {
    pub object_id: DieselUlid,
    pub download_url: String,
    pub direction: Direction,
}

#[derive(Debug, Hash, Eq, PartialEq)]
pub enum Direction {
    Push,
    Pull,
}

pub struct ReplicationHandler {
    pub receiver: Receiver<ReplicationMessage>,
}

impl ReplicationHandler {
    #[tracing::instrument(level = "trace")]
    pub fn new(receiver: Receiver<ReplicationMessage>) -> Self {
        Self { receiver }
    }

    #[tracing::instrument(level = "trace", skip(self, cache))]
    pub async fn run(self, cache: Arc<Cache>) -> Result<()> {
        let queue: Arc<DashSet<(DieselUlid, String, Direction), RandomState>> =
            Arc::new(DashSet::default());

        // Push messages into DashMap for deduplication
        let queue_clone = queue.clone();
        let receiver = self.receiver.clone();
        let recieve = tokio::spawn(async move {
            while let Ok(ReplicationMessage {
                object_id,
                download_url,
                direction,
            }) = receiver.recv().await
            {
                queue_clone.insert((object_id, download_url, direction));
            }
        });

        // Proccess DashMap entries in separate task
        let process = tokio::spawn(async move {
            loop {
                let next = match queue.iter().next() {
                    Some(entry) => entry,
                    None => continue,
                };
                let (id, url, direction) = next.key();
                ReplicationHandler::process(id, url, direction, cache.clone()).await;
            }
        });
        // Run both tasks simultaneously
        tokio::join!(recieve, process);
        trace_err!(Err(anyhow!("Not implemented!")))
    }

    #[tracing::instrument(level = "trace", skip(cache))]
    // TODO
    // - Pull/Push logic
    // - write objects into storage backend
    // - write objects into cache/db
    async fn process(
        id: &DieselUlid,
        url: &str,
        direction: &Direction,
        _cache: Arc<Cache>,
    ) -> Result<()> {
        trace_err!(Err(anyhow!("Not implemented!")))
    }
}
