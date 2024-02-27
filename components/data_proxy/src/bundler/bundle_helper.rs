use std::sync::Arc;

use crate::{data_backends::storage_backend::StorageBackend, structs::ObjectLocation};
use pithos_lib::{
    helpers::structs::FileContext, streamreadwrite::GenericStreamReadWriter, transformer::ReadWriter, transformers::{
        async_sender_sink::AsyncSenderSink, decrypt::ChaCha20Dec, gzip_comp::GzipEnc, tar::TarEnc,
        zstd_decomp::ZstdDec,
    }
};
use pithos_lib::helpers::notifications::Message;
use futures_util::TryStreamExt;
use s3s::{dto::StreamingBlob, s3_error};
use tokio::pin;
use tracing::{debug, info_span, trace, Instrument};

#[tracing::instrument(level = "trace", skip(path_level_vec, backend))]
pub async fn get_bundle(
    path_level_vec: Vec<(String, Option<ObjectLocation>)>,
    backend: Arc<Box<dyn StorageBackend>>,
) -> Option<StreamingBlob> {
    let (file_info_sender, file_info_receiver) = async_channel::bounded(10);
    let (data_tx, data_sx) = async_channel::bounded(10);
    let (final_sender, final_receiver) = async_channel::bounded(10);
    let final_sender_clone = final_sender.clone();
    let final_receiver_clone = final_receiver.clone();

    tokio::spawn(
        async move {
            let mut counter = 1; // Start with 1 for comparison with len()
            let len = path_level_vec.len();
            for (name, loc) in path_level_vec {
                trace!(object = name, ?loc);
                let data_tx_clone = data_tx.clone();
                let file_info_sender_clone = file_info_sender.clone();
                if let Some(location) = loc {
                    file_info_sender_clone
                        .clone()
                        .send(
                            Message::FileContext(FileContext {
                                file_path: name.to_string(),
                                compressed_size: location.disk_content_len as u64,
                                decompressed_size: location.raw_content_len as u64,
                                compression: location.file_format.is_compressed(),
                                encryption_key: location.file_format.get_encryption_key_as_enc_key(),
                                ..Default::default()
                            }),
                        )
                        .await
                        .map_err(|e| {
                            tracing::error!(error = ?e, msg = e.to_string());
                            e
                        })?;

                    backend
                        .get_object(location.clone(), None, data_tx_clone)
                        .await
                        .map_err(|e| {
                            tracing::error!(error = ?e, msg = e.to_string());
                            e
                        })?;
                } else {
                    file_info_sender_clone
                        .clone()
                        .send(
                            Message::FileContext(FileContext {
                                file_path: name.to_string(),
                                is_dir: true,
                                ..Default::default()
                            })
                        )
                        .await
                        .map_err(|e| {
                            tracing::error!(error = ?e, msg = e.to_string());
                            e
                        })?;
                }
                counter += 1;
                trace!("finished file {}/{}", counter, len)
            }
            trace!("Final counter: {}", counter);

            Ok::<(), anyhow::Error>(())
        }
        .instrument(info_span!("get_bundle_reader")),
    );

    let data_clone = data_sx.clone();

    tokio::spawn(
        async move {
            pin!(data_clone);

            let mut aruna_stream_writer = GenericStreamReadWriter::new_with_sink(
                data_clone,
                AsyncSenderSink::new(final_sender_clone.clone()),
            )
            .add_transformer(ChaCha20Dec::new().map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?)
            .add_transformer(ZstdDec::new())
            .add_transformer(TarEnc::new())
            .add_transformer(GzipEnc::new());
            aruna_stream_writer
                .add_message_receiver(file_info_receiver.clone())
                .await
                .map_err(|e| {
                    tracing::error!(error = ?e, msg = e.to_string());
                    e
                })?;
            trace!("Starting read_writer process");
            aruna_stream_writer.process().await.map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;

            Ok::<(), anyhow::Error>(())
        }
        .instrument(info_span!("get_bundle_writer")),
    );
    debug!("Starting response streaming");
    Some(StreamingBlob::wrap(final_receiver_clone.map_err(|_| {
        s3_error!(InternalError, "Internal processing error")
    })))
}
