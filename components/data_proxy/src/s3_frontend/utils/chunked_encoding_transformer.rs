use anyhow::Result;
use aruna_file::{
    notifications::{Message, Response},
    transformer::Transformer,
};

#[derive(Default)]
pub struct ChunkedEncodingTransformer {
    finished: bool,
}

impl ChunkedEncodingTransformer {
    #[allow(dead_code)]
    pub fn new() -> Self {
        ChunkedEncodingTransformer { finished: false }
    }
}

#[async_trait::async_trait]
impl Transformer for ChunkedEncodingTransformer {
    async fn process_bytes(
        &mut self,
        buf: &mut bytes::BytesMut,
        finished: bool,
        _should_flush: bool,
    ) -> Result<bool> {
        if buf.len() > 0 {
            let mut chunk = bytes::BytesMut::new();
            chunk.extend_from_slice(format!("{:x}\r\n", buf.len()).as_bytes());
            chunk.extend_from_slice(&buf);
            chunk.extend_from_slice(b"\r\n");
            buf.clear();
            buf.extend_from_slice(&chunk);
        } else if finished {
            buf.extend_from_slice("0\r\n\r\n".as_bytes());
            self.finished = true;
        }
        Ok(self.finished)
    }
    #[allow(unused_variables)]
    async fn notify(&mut self, message: &Message) -> Result<Response> {
        //dbg!(message, &self.id);
        Ok(Response::Ok)
    }
}
