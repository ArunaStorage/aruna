use anyhow::Result;
use aruna_file::{
    notifications::{Message, Response},
    transformer::Transformer,
};

#[derive(Default)]
pub struct DebugTransformer {
    _counter: u64,
    id: String,
}

impl DebugTransformer {
    pub fn new(id: String) -> Self {
        DebugTransformer { id, _counter: 0 }
    }
}

#[async_trait::async_trait]
impl Transformer for DebugTransformer {
    async fn process_bytes(
        &mut self,
        buf: &mut bytes::BytesMut,
        finished: bool,
        should_flush: bool,
    ) -> Result<bool> {
        dbg!((buf.len(), &self.id, finished, should_flush));
        Ok(finished)
    }
    #[allow(unused_variables)]
    async fn notify(&mut self, message: &Message) -> Result<Response> {
        //dbg!(message, &self.id);
        Ok(Response::Ok)
    }
}
