use std::sync::Arc;

use anyhow::anyhow;
use anyhow::Result;
use aruna_rust_api::api::dataproxy::services::v2::{
    pull_replication_response::Message, Chunk, PullReplicationResponse,
};
use async_channel::Receiver;
use async_channel::Sender;
use async_channel::TryRecvError;
use bytes::{BufMut, BytesMut};
use md5::{Digest, Md5};
use pithos_lib::helpers::notifications::Notifier;
use pithos_lib::transformer::TransformerType;
use pithos_lib::transformer::{Sink, Transformer};
use tokio::sync::mpsc::Sender as TokioSender;
use tracing::error;

pub struct ReplicationSink {
    object_id: String,
    maximum_chunks: usize,
    chunk_counter: usize, // One chunk contains multiple blocks
    sender: TokioSender<Result<PullReplicationResponse, tonic::Status>>,
    error_recv: Receiver<Option<(i64, String)>>,
    buffer: BytesMut,
    is_finished: bool,
    bytes_counter: u64,
    bytes_start: u64,
    notifier: Option<Arc<Notifier>>,
    msg_receiver: Option<Receiver<pithos_lib::helpers::notifications::Message>>,
    idx: Option<usize>,
}

impl Sink for ReplicationSink {}

impl ReplicationSink {
    #[tracing::instrument(level = "trace", skip())]
    pub fn new(
        object_id: String,
        chunks: usize,
        sender: TokioSender<Result<PullReplicationResponse, tonic::Status>>,
        error_recv: Receiver<Option<(i64, String)>>,
    ) -> ReplicationSink {
        ReplicationSink {
            maximum_chunks: chunks,
            object_id,
            sender,
            error_recv,
            chunk_counter: 0,
            buffer: BytesMut::with_capacity((1024 * 1024 * 5) + 128),
            is_finished: false,
            bytes_counter: 0,
            bytes_start: 0,
            notifier: None,
            msg_receiver: None,
            idx: None,
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn process_messages(&mut self) -> Result<bool> {
        if let Some(rx) = &self.msg_receiver {
            loop {
                match rx.try_recv() {
                    Ok(pithos_lib::helpers::notifications::Message::Finished) => return Ok(true),
                    Ok(_) => {}
                    Err(TryRecvError::Empty) => {
                        break;
                    }
                    Err(TryRecvError::Closed) => {
                        error!("Message receiver closed");
                        return Err(anyhow!("Message receiver closed"));
                    }
                }
            }
        }
        Ok(false)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn create_and_send_message(&mut self, is_finished: bool) -> Result<bool> {
        if self.buffer.is_empty() {
            return Ok(false);
        }

        let len = if self.chunk_counter < self.maximum_chunks - 1 {
            65536 + 28
        } else if is_finished {
            self.buffer.len()
        } else {
            return Ok(false);
        };

        if self.buffer.len() < len {
            return Ok(false);
        }

        let data = self.buffer.split_to(len).to_vec();

        // create a Md5 hasher instance
        let mut hasher = Md5::new();
        // process input message
        hasher.update(&data);

        // acquire hash digest in the form of GenericArray,
        // which in this case is equivalent to [u8; 16]
        let result = hasher.finalize();
        //trace!(chunk_len = data.len());

        let message = PullReplicationResponse {
            message: Some(Message::Chunk(Chunk {
                object_id: self.object_id.clone(),
                chunk_idx: self.chunk_counter as i64,
                data,
                checksum: hex::encode(result),
            })),
        };

        self.sender.send(Ok(message.clone())).await.map_err(|e| {
            error!(error = ?e, msg = e.to_string());
            tonic::Status::unauthenticated(e.to_string())
        })?;

        // Send chunk again if lost,
        // else abort (TODO: Retry object)
        if let Some((idx, id)) = self.error_recv.recv().await.map_err(|e| {
            error!(error = ?e, msg = e.to_string());
            tonic::Status::unauthenticated(e.to_string())
        })? {
            if self.object_id == id && self.chunk_counter as i64 == idx {
                self.sender.send(Ok(message)).await.map_err(|e| {
                    error!(error = ?e, msg = e.to_string());
                    tonic::Status::unauthenticated(e.to_string())
                })?;
            } else {
                return Err(anyhow!("Can not retry chunk"));
            }
        }

        self.chunk_counter += 1;
        self.bytes_start += len as u64;

        Ok(true)
    }
}

#[async_trait::async_trait]
impl Transformer for ReplicationSink {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn initialize(
        &mut self,
        idx: usize,
    ) -> (
        TransformerType,
        Sender<pithos_lib::helpers::notifications::Message>,
    ) {
        self.idx = Some(idx);
        let (sx, rx) = async_channel::bounded(10);
        self.msg_receiver = Some(rx);
        (TransformerType::Sink, sx)
    }

    #[tracing::instrument(level = "trace", skip(self, buf))]
    async fn process_bytes(&mut self, buf: &mut BytesMut) -> Result<()> {
        // block size: 65536
        self.bytes_counter += buf.len() as u64;
        self.buffer.put(buf.split());

        let finished = self.process_messages()?;

        // trace!(first=finished, self_finished=self.is_finished, len=self.buffer.len(), self.chunk_counter, self.maximum_chunks);

        if finished && !self.buffer.is_empty() && !self.is_finished {
            self.create_and_send_message(finished).await.map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                tonic::Status::unauthenticated(e.to_string())
            })?;
            self.is_finished = true;

            if let Some(notifier) = &self.notifier {
                notifier
                    .send_read_writer(pithos_lib::helpers::notifications::Message::Completed)?;
            }

            // trace!(second=finished, self.is_finished, len=self.buffer.len());
            return Ok(());
        }

        if !self.buffer.is_empty() {
            while self.create_and_send_message(finished).await.map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                tonic::Status::unauthenticated(e.to_string())
            })? {}

            //trace!(third=finished, self.is_finished, len=self.buffer.len());
        }
        //trace!(fourth=finished, self.is_finished, len=self.buffer.len());

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, notifier))]
    #[inline]
    async fn set_notifier(&mut self, notifier: Arc<Notifier>) -> Result<()> {
        self.notifier = Some(notifier);
        Ok(())
    }
}
