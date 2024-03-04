use crate::data_backends::storage_backend::StorageBackend;
use crate::structs::{ObjectLocation, PartETag};
use anyhow::{anyhow, Result};
use async_channel::{Receiver, Sender, TryRecvError};
use bytes::{BufMut, BytesMut};
use pithos_lib::helpers::notifications::{Message, Notifier};
use pithos_lib::transformer::{Sink, Transformer, TransformerType};
use std::sync::Arc;
use tracing::{debug, error, info_span, trace, Instrument};

pub struct BufferedS3Sink {
    backend: Arc<Box<dyn StorageBackend>>,
    buffer: BytesMut,
    target_location: ObjectLocation,
    upload_id: Option<String>,
    part_number: Option<i32>,
    single_part_upload: bool,
    tags: Vec<PartETag>,
    sum: usize,
    sender: Option<Sender<String>>,
    notifier: Option<Arc<Notifier>>,
    msg_receiver: Option<Receiver<Message>>,
    idx: Option<usize>,
}

impl Sink for BufferedS3Sink {}

impl BufferedS3Sink {
    #[tracing::instrument(
        level = "trace",
        skip(
            backend,
            target_location,
            upload_id,
            part_number,
            single_part_upload,
            tags,
            with_sender
        )
    )]
    pub fn new(
        backend: Arc<Box<dyn StorageBackend>>,
        target_location: ObjectLocation,
        upload_id: Option<String>,
        part_number: Option<i32>,
        single_part_upload: bool,
        tags: Option<Vec<PartETag>>,
        with_sender: bool,
    ) -> (Self, Option<Receiver<String>>) {
        let t = tags.unwrap_or_else(Vec::new);

        let (sx, tx) = if with_sender {
            let (tx, sx) = async_channel::bounded(2);
            (Some(sx), Some(tx))
        } else {
            (None, None)
        };

        (
            Self {
                backend,
                buffer: BytesMut::with_capacity(10_000_000),
                target_location,
                upload_id,
                part_number,
                single_part_upload,
                tags: t,
                sum: 0,
                sender: tx,
                notifier: None,
                msg_receiver: None,
                idx: None,
            },
            sx,
        )
    }
}

impl BufferedS3Sink {
    #[tracing::instrument(level = "trace", skip(self))]
    fn process_messages(&mut self) -> Result<bool> {
        if let Some(rx) = &self.msg_receiver {
            loop {
                match rx.try_recv() {
                    Ok(Message::Finished) => {
                        return Ok(true)
                    },
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
    async fn initialize_multipart(&mut self) -> Result<()> {
        trace!("Initializing multipart");
        self.part_number = Some(1);

        self.upload_id = Some(
            self.backend
                .init_multipart_upload(self.target_location.clone())
                .await?,
        );
        debug!("Initialized multipart");
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn upload_single(&mut self) -> Result<()> {
        let backend_clone = self.backend.clone();
        let expected_len: i64 = self.buffer.len() as i64;

        trace!(?expected_len, "uploading single");
        let location_clone = self.target_location.clone();

        let (sender, receiver) = async_channel::bounded(10);

        sender
            .send(Ok(self.buffer.split().freeze()))
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;

        tokio::spawn(
            async move {
                backend_clone
                    .put_object(receiver, location_clone, expected_len)
                    .await
            }
            .instrument(info_span!("upload_single_spawn")),
        )
        .await??;
        debug!(?self.target_location, "uploaded single");
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn upload_part(&mut self) -> Result<()> {
        trace!("uploading part");
        let backend_clone = self.backend.clone();
        let expected_len: i64 = self.buffer.len() as i64;
        let location_clone = self.target_location.clone();
        let pnumber = self.part_number.ok_or_else(|| {
            tracing::error!(error = "PartNumber expected");
            anyhow!("PartNumber expected")
        })?;

        let up_id = self.upload_id.clone().ok_or_else(|| {
            tracing::error!(error = "Upload ID not found");
            anyhow!("Upload ID not found")
        })?;

        let (sender, receiver) = async_channel::bounded(10);
        sender
            .try_send(Ok(self.buffer.split().freeze()))
            .map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;

        let tag = tokio::spawn(
            async move {
                backend_clone
                    .upload_multi_object(receiver, location_clone, up_id, expected_len, pnumber)
                    .await
            }
            .instrument(info_span!("upload_part_spawn")),
        )
        .await??;
        if let Some(s) = &self.sender {
            s.send(tag.etag.to_string()).await.map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;
        }
        self.tags.push(tag);
        self.part_number = Some(pnumber + 1);
        debug!(self.upload_id, pnumber, expected_len, "uploaded part");
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn finish_multipart(&mut self) -> Result<()> {
        trace!("Finishing multipart");
        let up_id = self.upload_id.clone().ok_or_else(|| {
            tracing::error!(error = "Upload ID not found");
            anyhow!("Upload ID not found")
        })?;
        self.backend
            .finish_multipart_upload(
                self.target_location.clone(),
                self.tags.clone(),
                up_id.clone(),
            )
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;
        debug!(up_id, "finished multipart");
        Ok(())
    }
    #[tracing::instrument(level = "trace", skip(self))]
    async fn _get_parts(&self) -> Vec<PartETag> {
        debug!(?self.tags, "get_parts");
        self.tags.clone()
    }
}

#[async_trait::async_trait]
impl Transformer for BufferedS3Sink {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn initialize(&mut self, idx: usize) -> (TransformerType, Sender<Message>) {
        self.idx = Some(idx);
        let (sx, rx) = async_channel::bounded(10);
        self.msg_receiver = Some(rx);
        (TransformerType::Sink, sx)
    }

    #[tracing::instrument(level = "trace", skip(self, buf))]
    async fn process_bytes(&mut self, buf: &mut BytesMut) -> Result<()> {

        let finished = self.process_messages()?;

        self.sum += buf.len();

        self.buffer.put(buf.split());

        if self.single_part_upload {
            if finished {
                self.upload_part().await?;
                if let Some(notifier) = &self.notifier {
                    notifier.send_read_writer(Message::Finished)?;
                }
                return Ok(());
            }
            Ok(())
        } else {
            if self.buffer.len() > 5242880 {
                trace!("exceeds 5 Mib -> upload multi part");
                // 5 Mib -> initialize multipart
                if self.upload_id.is_none() {
                    self.initialize_multipart().await?;
                }
                self.upload_part().await?;
            }

            if finished {
                if self.upload_id.is_none() {
                    self.upload_single().await?;
                } else {
                    // Upload den Rest +
                    self.upload_part().await?;
                    if !self.single_part_upload {
                        trace!("finishing multipart");
                        self.finish_multipart().await?;
                    }
                }

                if let Some(notifier) = &self.notifier {
                    notifier.send_read_writer(Message::Completed)?;
                }
                return Ok(());
            }
            Ok(())
        }
    }

    #[tracing::instrument(level = "trace", skip(self, notifier))]
    #[inline]
    async fn set_notifier(&mut self, notifier: Arc<Notifier>) -> Result<()> {
        self.notifier = Some(notifier);
        Ok(())
    }
}
