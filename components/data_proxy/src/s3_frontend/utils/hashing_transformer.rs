use anyhow::Result;
use aruna_file::notifications::{Message, ProbeBroadcast};
use aruna_file::transformer::{Transformer, TransformerType};
use async_channel::{Receiver, Sender};
use digest::{Digest, FixedOutputReset};

pub struct HashingTransformer<T: Digest + Send + FixedOutputReset> {
    hasher: T,
    sender: Option<Sender<Message>>,
}

impl<T> HashingTransformer<T>
where
    T: Digest + Send + Sync + FixedOutputReset,
{
    #[allow(dead_code)]
    pub fn new(hasher: T) -> (HashingTransformer<T>, Receiver<Message>) {
        let (size_sender, size_receiver) = async_channel::bounded(1);
        (
            HashingTransformer {
                hasher,
                sender: Some(size_sender),
            },
            size_receiver,
        )
    }
}

#[async_trait::async_trait]
impl<T> Transformer for HashingTransformer<T>
where
    T: Digest + Send + Sync + FixedOutputReset,
{
    async fn process_bytes(
        &mut self,
        buf: &mut bytes::BytesMut,
        finished: bool,
        _: bool,
    ) -> Result<bool> {
        Digest::update(&mut self.hasher, buf);

        if finished {
            if let Some(s) = &self.sender {
                s.send(Message {
                    target: TransformerType::SizeProbe,
                    data: aruna_file::notifications::MessageData::ProbeBroadcast(ProbeBroadcast {
                        message: format!("{:x?}", self.hasher.finalize_reset()),
                    }),
                })
                .await?;
            }
        }

        Ok(true)
    }
    fn add_sender(&mut self, s: Sender<Message>) {
        self.sender = Some(s);
    }

    fn get_type(&self) -> TransformerType {
        TransformerType::SizeProbe
    }
}
