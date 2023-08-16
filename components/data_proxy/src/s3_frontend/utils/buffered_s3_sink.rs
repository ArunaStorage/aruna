use crate::data_backends::storage_backend::StorageBackend;
use crate::structs::{ObjectLocation, PartETag};
use anyhow::{anyhow, Result};
use aruna_file::transformer::{Sink, Transformer};
use async_channel::{Receiver, Sender};
use bytes::{BufMut, BytesMut};
use std::sync::Arc;

pub struct BufferedS3Sink {
    backend: Arc<Box<dyn StorageBackend>>,
    buffer: BytesMut,
    target_location: ObjectLocation,
    upload_id: Option<String>,
    part_number: Option<i32>,
    only_parts: bool,
    tags: Vec<PartETag>,
    sum: usize,
    sender: Sender<String>,
}

impl Sink for BufferedS3Sink {}

impl BufferedS3Sink {
    pub fn new(
        backend: Arc<Box<dyn StorageBackend>>,
        target_location: ObjectLocation,
        upload_id: Option<String>,
        part_number: Option<i32>,
        only_parts: bool,
        tags: Option<Vec<PartETag>>,
    ) -> (Self, Receiver<String>) {
        let t = match tags {
            Some(t) => t,
            None => Vec::new(),
        };

        let (tx, sx) = async_channel::bounded(2);

        (
            Self {
                backend,
                buffer: BytesMut::with_capacity(10_000_000),
                target_location,
                upload_id,
                part_number,
                only_parts,
                tags: t,
                sum: 0,
                sender: tx,
            },
            sx,
        )
    }
}

impl BufferedS3Sink {
    async fn initialize_multipart(&mut self) -> Result<()> {
        self.part_number = Some(1);

        self.upload_id = Some(
            self.backend
                .init_multipart_upload(self.target_location.clone())
                .await?,
        );
        log::info!("Initialized multipart: {:?}", self.upload_id);
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

        log::debug!("Single upload to: {:?}", self.target_location.clone());
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

        let tag = tokio::spawn(async move {
            backend_clone
                .upload_multi_object(receiver, location_clone, up_id, expected_len, pnummer)
                .await
        })
        .await??;
        self.sender.send(tag.etag.to_string()).await?;
        self.tags.push(tag);
        self.part_number = Some(pnummer + 1);

        log::debug!(
            "Uploaded part: {:?}, number:{}, size: {}",
            self.upload_id,
            pnummer,
            expected_len
        );

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

        log::debug!("Finished multipart: {:?}", self.upload_id);

        log::info!("Finished with: {:?}", self.sum);
        Ok(())
    }
    async fn _get_parts(&self) -> Vec<PartETag> {
        self.tags.clone()
    }
}

#[async_trait::async_trait]
impl Transformer for BufferedS3Sink {
    async fn process_bytes(
        &mut self,
        buf: &mut bytes::BytesMut,
        finished: bool,
        _: bool,
    ) -> Result<bool> {
        self.sum += buf.len();
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
}
