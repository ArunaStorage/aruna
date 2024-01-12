use anyhow::anyhow;
use anyhow::Result;
use aruna_file::transformer::{Sink, Transformer};
use aruna_rust_api::api::dataproxy::services::v2::{
    pull_replication_response::Message, Chunk, PullReplicationResponse,
};
use bytes::{BufMut, BytesMut};
use md5::{Digest, Md5};
use tokio::sync::mpsc::Sender as TokioSender;
use tracing::trace;

use crate::trace_err;

pub struct ReplicationSink {
    object_id: String,
    blocklist: Vec<u8>,
    chunk_counter: usize, // One chunk contains multiple blocks
    sender: TokioSender<Result<PullReplicationResponse, tonic::Status>>,
    error_recv: async_channel::Receiver<Option<(i64, String)>>,
    buffer: BytesMut,
    is_finished: bool,
    bytes_counter: u64,
    bytes_start: u64,
}

impl Sink for ReplicationSink {}

impl ReplicationSink {
    #[tracing::instrument(level = "trace", skip())]
    pub fn new(
        object_id: String,
        blocklist: Vec<u8>,
        sender: TokioSender<Result<PullReplicationResponse, tonic::Status>>,
        error_recv: async_channel::Receiver<Option<(i64, String)>>,
    ) -> ReplicationSink {
        ReplicationSink {
            object_id,
            blocklist,
            sender,
            error_recv,
            chunk_counter: 0,
            buffer: BytesMut::with_capacity((1024 * 1024 * 5) + 128),
            is_finished: false,
            bytes_counter: 0,
            bytes_start: 0,
        }
    }
    async fn create_and_send_message(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let len = if self.blocklist.len() == 0 {
            self.buffer.len() as u64
        } else {
            65536 * (self.blocklist[self.chunk_counter as usize] as u64)
        };

        let data = self.buffer.split_to(len as usize).to_vec();

        // create a Md5 hasher instance
        let mut hasher = Md5::new();
        // process input message
        hasher.update(&data);

        // acquire hash digest in the form of GenericArray,
        // which in this case is equivalent to [u8; 16]
        let result = hasher.finalize();

        let message = PullReplicationResponse {
            message: Some(Message::Chunk(Chunk {
                object_id: self.object_id.clone(),
                chunk_idx: (self.chunk_counter as i64),
                data, // Clear self.buffer
                checksum: hex::encode(result),
            })),
        };

        trace_err!(self.sender.send(Ok(message.clone())).await)?;

        // Send chunk again if lost,
        // else abort (TODO: Retry object)
        if let Some((idx, id)) = trace_err!(self.error_recv.recv().await)? {
            if self.object_id == id && self.chunk_counter as i64 == idx {
                trace_err!(self.sender.send(Ok(message)).await)?;
            } else {
                return Err(anyhow!("Can not retry chunk"));
            }
        }

        self.chunk_counter += 1;
        self.bytes_start += len;

        Ok(())
    }
}

#[async_trait::async_trait]
impl Transformer for ReplicationSink {
    #[tracing::instrument(level = "trace", skip(self, buf, finished))]
    async fn process_bytes(&mut self, buf: &mut BytesMut, finished: bool, _: bool) -> Result<bool> {
        // blocksize: 65536
        self.bytes_counter += buf.len() as u64;
        self.buffer.put(buf.split());

        if finished && !self.buffer.is_empty() {
            trace_err!(self.create_and_send_message().await)?;
            self.is_finished = true;
            return Ok(self.is_finished);
        }

        if self.blocklist.len() == 0 && finished && !&self.is_finished {
            trace_err!(self.create_and_send_message().await)?;
            self.is_finished = true;
            return Ok(self.is_finished);
        }
        if self.blocklist.len() != 0 {
            while (self.bytes_counter - self.bytes_start)
                > 65536 * (self.blocklist[self.chunk_counter as usize] as u64)
            {
                trace_err!(self.create_and_send_message().await)?;
            }
        }

        Ok(self.is_finished)
    }
}
