use anyhow::{anyhow, Result};
use async_channel::{Receiver, Sender, TryRecvError};
use bytes::BytesMut;
use pithos_lib::helpers::notifications::{Message, Notifier};
use pithos_lib::transformer::{Transformer, TransformerType};
use std::sync::Arc;
use tracing::{error, trace};

#[derive(Default)]
pub struct DebugTransformer {
    name: String,
    accumulator: usize,
    notifier: Option<Arc<Notifier>>,
    msg_receiver: Option<Receiver<Message>>,
    idx: Option<usize>,
    counter: usize,
}

impl DebugTransformer {
    #[tracing::instrument(level = "trace", skip(name))]
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            counter: usize::MAX,
            ..Default::default()
        }
    }

    #[tracing::instrument(level = "trace", skip(name))]
    pub fn new_with_backoff(name: &str, counter: usize) -> Self {
        Self {
            name: name.to_string(),
            counter,
            ..Default::default()
        }
    }
}

impl DebugTransformer {
    #[tracing::instrument(level = "trace", skip(self))]
    fn process_messages(&mut self) -> Result<(bool, bool)> {
        if let Some(rx) = &self.msg_receiver {
            loop {
                match rx.try_recv() {
                    Ok(Message::Finished) => return Ok((true, false)),
                    Ok(Message::ShouldFlush) => return Ok((false, true)),
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
        Ok((false, false))
    }
}

#[async_trait::async_trait]
impl Transformer for DebugTransformer {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn initialize(&mut self, idx: usize) -> (TransformerType, Sender<Message>) {
        self.idx = Some(idx);
        let (sx, rx) = async_channel::bounded(10);
        self.msg_receiver = Some(rx);
        (TransformerType::Unspecified, sx)
    }

    #[tracing::instrument(level = "trace", skip(self, buf))]
    async fn process_bytes(&mut self, buf: &mut BytesMut) -> Result<()> {

        if self.counter != usize::MAX {
            if self.counter == 0 {
                return Err(anyhow!("Backoff limit reached"));
            }
            self.counter -= 1;
        }

        let (finished, should_flush) = self.process_messages()?;
        self.accumulator += buf.len();
        trace!(name = ?self.name, ?finished, ?should_flush,  len = ?buf.len(), processed = ?self.accumulator, "process_bytes");
        if finished {
            if let Some(notifier) = &self.notifier {
                notifier.send_next(
                    self.idx.ok_or_else(|| anyhow!("Missing idx"))?,
                    Message::Finished,
                )?;
            }
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, notifier))]
    #[inline]
    async fn set_notifier(&mut self, notifier: Arc<Notifier>) -> Result<()> {
        self.notifier = Some(notifier);
        Ok(())
    }
}
