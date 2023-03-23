use std::sync::Arc;

use crate::backends::storage_backend::StorageBackend;
use anyhow::{anyhow, Result};
use aruna_file::transformer::{AddTransformer, Data, Notifications, Sink, Transformer};
use aruna_rust_api::api::internal::v1::{Location, PartETag};
use bytes::{BufMut, BytesMut};

pub struct BufferedS3Sink {
    backend: Arc<Box<dyn StorageBackend>>,
    buffer: BytesMut,
    target_location: Location,
    upload_id: Option<String>,
    part_number: Option<i32>,
    only_parts: bool,
    tags: Vec<PartETag>,
}

impl Sink for BufferedS3Sink {}

impl BufferedS3Sink {
    pub fn new(
        backend: Arc<Box<dyn StorageBackend>>,
        target_location: Location,
        upload_id: Option<String>,
        part_number: Option<i32>,
        only_parts: bool,
        tags: Option<Vec<PartETag>>,
    ) -> Self {
        let t = match tags {
            Some(t) => t,
            None => Vec::new(),
        };
        Self {
            backend,
            buffer: BytesMut::with_capacity(10_000_000),
            target_location,
            upload_id,
            part_number,
            only_parts,
            tags: t,
        }
    }
}

impl AddTransformer<'_> for BufferedS3Sink {
    fn add_transformer<'a>(self: &mut BufferedS3Sink, _t: Box<dyn Transformer + Send + 'a>) {}
}

impl BufferedS3Sink {
    async fn initialize_multipart(&mut self) -> Result<()> {
        self.part_number = Some(1);

        self.upload_id = Some(
            self.backend
                .init_multipart_upload(self.target_location.clone())
                .await?,
        );
        Ok(())
    }

    async fn upload_single(&mut self) -> Result<()> {
        let backend_clone = self.backend.clone();
        let expected_len: i64 = self.buffer.len() as i64;
        let location_clone = self.target_location.clone();

        let (sender, receiver) = async_channel::bounded(10);

        sender.send(Ok(self.buffer.split().freeze())).await?;

        tokio::spawn(async move {
            backend_clone
                .put_object(receiver, location_clone, expected_len)
                .await
        })
        .await??;

        Ok(())
    }

    async fn upload_part(&mut self) -> Result<()> {
        let backend_clone = self.backend.clone();
        let expected_len: i64 = self.buffer.len() as i64;
        let location_clone = self.target_location.clone();
        let pnummer = self
            .part_number
            .ok_or_else(|| anyhow!("PartNumber expected"))?;

        let up_id = self
            .upload_id
            .clone()
            .ok_or_else(|| anyhow!("Upload ID not found"))?;

        let (sender, receiver) = async_channel::bounded(10);
        sender.send(Ok(self.buffer.split().freeze())).await?;

        self.tags.push(
            tokio::spawn(async move {
                backend_clone
                    .upload_multi_object(receiver, location_clone, up_id, expected_len, pnummer)
                    .await
            })
            .await??,
        );

        self.part_number = Some(pnummer + 1);

        Ok(())
    }

    async fn finish_multipart(&mut self) -> Result<()> {
        let up_id = self
            .upload_id
            .clone()
            .ok_or_else(|| anyhow!("Upload ID not found"))?;
        self.backend
            .finish_multipart_upload(self.target_location.clone(), self.tags.clone(), up_id)
            .await?;

        Ok(())
    }
    async fn _get_parts(&self) -> Vec<PartETag> {
        self.tags.clone()
    }
}

#[async_trait::async_trait]
impl Transformer for BufferedS3Sink {
    async fn process_bytes(&mut self, buf: &mut bytes::Bytes, finished: bool) -> Result<bool> {
        let len = buf.len();

        self.buffer.put(buf);
        if self.buffer.len() > 5242880 && !self.only_parts {
            // 5 Mib -> initialize multipart
            if self.upload_id.is_none() {
                self.initialize_multipart().await?;
            }
            self.upload_part().await?;
        }

        if len == 0 && finished {
            if self.upload_id.is_none() {
                self.upload_single().await?;
            } else {
                // Upload den Rest +
                self.upload_part().await?;
                if !self.only_parts {
                    self.finish_multipart().await?;
                }
            }
            return Ok(true);
        }
        Ok(false)
    }
    async fn notify(&mut self, notes: &mut Vec<Notifications>) -> Result<()> {
        notes.push(Notifications::Response(Data {
            recipient: format!("SINK_TAG"),
            info: self.tags.pop().map(|tag| tag.etag.as_bytes().to_vec()),
        }));
        Ok(())
    }
}

pub fn parse_notes_get_etag(notes: Vec<Notifications>) -> Result<String> {
    let mut result = String::new();
    for note in notes {
        match note {
            Notifications::Response(data) => {
                if data.recipient.starts_with("SINK_TAG") {
                    result =
                        String::from_utf8_lossy(&data.info.ok_or(anyhow!("No chunks responded"))?)
                            .to_string()
                }
            }
            _ => continue,
        }
    }
    Ok(result)
}
