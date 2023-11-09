use std::sync::Arc;

use ahash::RandomState;
use anyhow::{anyhow, Result};
use async_channel::Receiver;
use dashmap::DashMap;
use diesel_ulid::DieselUlid;

pub struct ReplicationMessage {
    object_id: DieselUlid,
    download_url: String,
}

pub struct ReplicationHandler {
    pub reciever: Receiver<ReplicationMessage>,
    pub queue: Arc<DashMap<DieselUlid, String, RandomState>>,
}

impl ReplicationHandler {
    pub fn new(reciever: Receiver<ReplicationMessage>) -> Self {
        Self {
            reciever,
            queue: Arc::new(DashMap::default()),
        }
    }
    pub async fn run(&self) -> Result<()> {
        let recieve = tokio::spawn(async {
            while let Ok(ReplicationMessage {
                object_id,
                download_url,
            }) = self.reciever.recv().await
            {
                self.queue.insert(object_id, download_url);
            }
        });
        Err(anyhow!("Not implemented!"))
    }
}
