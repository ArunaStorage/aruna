use anyhow::Result;
use aruna_file::transformer::{Sink, Transformer};
use aruna_rust_api::api::dataproxy::services::v2::{
    pull_replication_response::Message, Chunk, PullReplicationResponse,
};
use bytes::{BufMut, BytesMut};
use md5::{Digest, Md5};
use tokio::sync::mpsc::Sender as TokioSender;

use crate::trace_err;

pub struct ReplicationSink {
    object_id: String,
    blocklist: Vec<u8>,
    chunk_counter: usize, // One chunk contains multiple blocks
    block_counter: u8,
    sender: TokioSender<Result<PullReplicationResponse, tonic::Status>>,
    buffer: BytesMut,
    is_finished: bool,
}

impl Sink for ReplicationSink {}

impl ReplicationSink {
    #[tracing::instrument(level = "trace", skip())]
    pub fn new(
        object_id: String,
        blocklist: Vec<u8>,
        sender: TokioSender<Result<PullReplicationResponse, tonic::Status>>,
    ) -> ReplicationSink {
        ReplicationSink {
            object_id,
            blocklist,
            sender,
            block_counter: 0,
            chunk_counter: 0,
            buffer: BytesMut::with_capacity((1024 * 1024 * 5) + 128),
            is_finished: false,
        }
    }
    async fn create_and_send_message(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        // create a Md5 hasher instance
        let mut hasher = Md5::new();

        // process input message
        hasher.update(&self.buffer);

        // acquire hash digest in the form of GenericArray,
        // which in this case is equivalent to [u8; 16]
        let result = hasher.finalize();

        let message = PullReplicationResponse {
            message: Some(Message::Chunk(Chunk {
                object_id: self.object_id.clone(),
                chunk_idx: self.chunk_counter as i64,
                data: self.buffer.split().to_vec(), // Clear self.buffer
                checksum: hex::encode(result),
            })),
        };
        trace_err!(self.sender.send(Ok(message)).await)?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl Transformer for ReplicationSink {
    #[tracing::instrument(level = "trace", skip(self, buf, finished))]
    async fn process_bytes(&mut self, buf: &mut BytesMut, finished: bool, _: bool) -> Result<bool> {
        self.buffer.put(buf.split());

        self.block_counter += 1;

        if finished && !self.buffer.is_empty() {
            trace_err!(self.create_and_send_message().await)?;
            self.is_finished = true;
            return Ok(self.is_finished);
        }

        if self.chunk_counter > self.blocklist.len() {
            return Ok(self.is_finished);
        } else if self.block_counter == self.blocklist[self.chunk_counter] {
            self.chunk_counter += 1;
            self.block_counter = 0;

            if self.chunk_counter != self.blocklist.len() {
                trace_err!(self.create_and_send_message().await)?;
            }
        }
        Ok(self.is_finished)
    }
}
