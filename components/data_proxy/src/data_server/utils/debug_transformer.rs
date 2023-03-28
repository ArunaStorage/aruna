use anyhow::Result;
use aruna_file::transformer::{AddTransformer, Notifications, Transformer};

pub struct DebugTransformer<'a> {
    next: Option<Box<dyn Transformer + Send + 'a>>,
}

impl DebugTransformer<'_> {
    pub fn new() -> Self {
        Self { next: None }
    }
}

impl<'a> AddTransformer<'a> for DebugTransformer<'a> {
    fn add_transformer(&mut self, t: Box<dyn Transformer + Send + 'a>) {
        self.next = Some(t)
    }
}
#[async_trait::async_trait]
impl Transformer for DebugTransformer<'_> {
    async fn process_bytes(&mut self, buf: &mut bytes::Bytes, finished: bool) -> Result<bool> {
        // Try to write the buf to the "next" in the chain, even if the buf is empty
        if let Some(next) = &mut self.next {
            // Should be called even if bytes.len() == 0 to drive underlying Transformer to completion
            log::info!(
                "DebugTransformer: processed: {} Bytes, finished: {}",
                buf.len(),
                finished
            );
            return next.process_bytes(buf, finished).await;
        }
        Ok(false)
    }

    async fn notify(&mut self, notes: &mut Vec<Notifications>) -> Result<()> {
        if let Some(next) = &mut self.next {
            next.notify(notes).await?
        }
        Ok(())
    }
}
