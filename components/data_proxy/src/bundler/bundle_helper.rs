use crate::{data_backends::storage_backend::StorageBackend, structs::ObjectLocation};
use aruna_file::{
    streamreadwrite::ArunaStreamReadWriter,
    transformer::{FileContext, ReadWriter},
    transformers::{
        async_sender_sink::AsyncSenderSink, decrypt::ChaCha20Dec, gzip_comp::GzipEnc, tar::TarEnc,
        zstd_decomp::ZstdDec,
    },
};
use futures_util::TryStreamExt;
use s3s::{dto::StreamingBlob, s3_error};

pub async fn bundle_helper(
    path_level_vec: Vec<(String, Option<ObjectLocation>)>,
    backend: Box<dyn StorageBackend>,
) -> Option<StreamingBlob> {
    // if !file_name.ends_with(".tar.gz") {
    //     return Err(RequestError(anyhow::anyhow!(
    //         "Only .tar.gz is currently supported"
    //     )));
    // }

    let (file_info_sender, file_info_receiver) = async_channel::bounded(10);
    let (data_tx, data_sx) = async_channel::bounded(10);
    let file_info_recv_clone = file_info_receiver.clone();
    let (final_sender, final_receiver) = async_channel::bounded(10);
    let final_sender_clone = final_sender.clone();
    let final_receiver_clone = final_receiver.clone();

    tokio::spawn(async move {
        let mut counter = 0;
        while let Some((name, loc)) = path_level_vec.iter().next() {
            let data_tx_clone = data_tx.clone();
            let file_info_sender_clone = file_info_sender.clone();
            if let Some(location) = loc {
                file_info_sender_clone
                    .clone()
                    .send((
                        FileContext {
                            file_name: name.to_string(),
                            input_size: location.disk_content_len as u64,
                            file_size: location.raw_content_len as u64,
                            skip_decompression: !location.compressed,
                            skip_decryption: !location.encryption_key.is_some(),
                            encryption_key: location
                                .encryption_key
                                .as_ref()
                                .map(|e| e.to_string().into_bytes()),
                            ..Default::default()
                        },
                        counter < path_level_vec.len(),
                    ))
                    .await
                    .map_err(|e| {
                        log::debug!("[BUNDLER] File info send failed: {e}");
                        e
                    })?;

                backend
                    .get_object(location.clone(), None, data_tx_clone)
                    .await
                    .map_err(|e| {
                        log::error!("[BUNDLER] Get object failed: {e}");
                        e
                    })?;
            } else {
                file_info_sender_clone
                    .clone()
                    .send((
                        FileContext {
                            file_name: name.to_string(),
                            is_dir: true,
                            ..Default::default()
                        },
                        counter < path_level_vec.len(),
                    ))
                    .await
                    .map_err(|e| {
                        log::debug!("[BUNDLER] File info send failed: {e}");
                        e
                    })?;
            }
            counter += 1;
        }
        Ok::<(), anyhow::Error>(())
    });

    tokio::spawn(async move {
        log::debug!("[BUNDLER] Spawned aruna-read-writer!");
        let mut aruna_stream_writer = ArunaStreamReadWriter::new_with_sink(
            data_sx.clone(),
            AsyncSenderSink::new(final_sender_clone.clone()),
        )
        .add_transformer(ChaCha20Dec::new(None).map_err(|e| {
            log::error!("[BUNDLER] ChaCha20 Dec failed: {e}");
            e
        })?)
        .add_transformer(ZstdDec::new())
        .add_transformer(TarEnc::new())
        .add_transformer(GzipEnc::new());

        aruna_stream_writer
            .add_file_context_receiver(file_info_recv_clone.clone())
            .await
            .map_err(|e| {
                log::error!("[BUNDLER] StreamReadWriter add_file_context failed: {e}");
                e
            })?;

        // Start only if receiver has data!
        while data_sx.is_empty() {}

        log::debug!("[BUNDLER] Aruna-read-writer started!");
        aruna_stream_writer.process().await.map_err(|e| {
            log::error!("[BUNDLER] StreamReadWriter process failed: {e}");
            e
        })
    });

    log::debug!("[BUNDLER] Start streaming!");
    Some(StreamingBlob::wrap(final_receiver_clone.map_err(|_| {
        s3_error!(InternalError, "Internal processing error")
    })))
}
