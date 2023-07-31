use crate::data_backends::storage_backend::StorageBackend;
use anyhow::Result;
use aruna_file::{
    streamreadwrite::ArunaStreamReadWriter,
    transformer::{FileContext, ReadWriter},
    transformers::{
        async_sender_sink::AsyncSenderSink, decrypt::ChaCha20Dec, gzip_comp::GzipEnc, tar::TarEnc,
        zstd_decomp::ZstdDec,
    },
};
use axum::{
    body::StreamBody,
    error_handling::HandleErrorLayer,
    extract::{Path, State},
    response::{IntoResponse, Response},
    routing::get,
    BoxError, Router,
};
use hyper::StatusCode;
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tower::{buffer::BufferLayer, limit::RateLimitLayer, ServiceBuilder};

use super::bundler::Bundler;

struct RequestError(anyhow::Error);

pub async fn run_axum(
    bundler_addr: String,
    bundler: Arc<Mutex<Bundler>>,
    backend: Arc<Box<dyn StorageBackend>>,
) -> Result<()> {
    // build our application with a route
    let app = Router::new()
        // `GET /` goes to `root`
        .route("/:bundle_id/:file_name", get(get_bundle))
        .with_state((bundler.clone(), backend))
        .layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(|err: BoxError| async move {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Unhandled error: {}", err),
                    )
                }))
                .layer(BufferLayer::new(1024))
                .layer(RateLimitLayer::new(5, Duration::from_secs(1))),
        );

    // Axum Server
    // run our app with hyper, listening globally on port 3000
    axum::Server::bind(&bundler_addr.parse()?)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}

async fn get_bundle(
    State((bundler, backend)): State<(Arc<Mutex<Bundler>>, Arc<Box<dyn StorageBackend>>)>,
    Path((bundle_id, file_name)): Path<(String, String)>,
) -> Result<impl IntoResponse, RequestError> {
    if !file_name.ends_with(".tar.gz") {
        return Err(RequestError(anyhow::anyhow!(
            "Only .tar.gz is currently supported"
        )));
    }
    let bundler_store = &bundler.lock().await.bundle_store;
    let bundles = bundler_store.get(&bundle_id);
    let (tx, rx) = async_channel::bounded(10);
    let (file_info_sender, file_info_receiver) = async_channel::bounded(10);
    let file_info_recv_clone = file_info_receiver.clone();
    let file_info_sender_clone = file_info_sender.clone();
    let tx_clone = tx.clone();
    let rx_clone = rx.clone();
    match bundles {
        Some(b) => match b {
            Some(bundle) => {
                // TODO: check if bundle is expired
                let cloned_bundle = bundle.clone();
                tokio::spawn(async move {
                    let len = cloned_bundle.object_refs.len();
                    for (i, object_ref) in cloned_bundle.object_refs.into_iter().enumerate() {
                        let object_info = object_ref.object_info.unwrap_or_default();
                        let object_location =
                            object_ref.object_location.clone().unwrap_or_default();
                        let object_internal_size = backend
                            .head_object(object_location.clone())
                            .await
                            .map_err(|e| {
                                log::debug!("[BUNDLER] Head object failed: {e}");
                                e
                            })?;

                        file_info_sender_clone
                            .clone()
                            .send((
                                FileContext {
                                    file_name: object_info.filename.to_string(),
                                    input_size: object_internal_size as u64,
                                    file_size: object_info.content_len as u64,
                                    file_path: Some(object_ref.sub_path),
                                    skip_decompression: !object_location.is_compressed,
                                    skip_decryption: !object_location.is_encrypted,
                                    encryption_key: object_ref
                                        .object_location
                                        .map(|e| e.encryption_key.into_bytes()),
                                    ..Default::default()
                                },
                                len == i + 1,
                            ))
                            .await
                            .map_err(|e| {
                                log::debug!("[BUNDLER] File info send failed: {e}");
                                e
                            })?;

                        log::debug!("[BUNDLER] Spawning get_object {:#?}", &object_info.filename);
                        backend
                            .get_object(object_location, None, tx_clone.clone())
                            .await
                            .map_err(|e| {
                                log::error!("[BUNDLER] Get object failed: {e}");
                                e
                            })?;
                    }
                    Ok::<(), anyhow::Error>(())
                });
            }
            None => return Err(anyhow::anyhow!("Bundle is still initializing!").into()),
        },
        None => return Err(anyhow::anyhow!("Invalid bundle!").into()),
    }

    let (final_sender, final_receiver) = async_channel::bounded(10);
    let final_sender_clone = final_sender.clone();
    let final_receiver_clone = final_receiver.clone();

    tokio::spawn(async move {
        log::debug!("[BUNDLER] Spawned aruna-read-writer!");
        let mut aruna_stream_writer = ArunaStreamReadWriter::new_with_sink(
            rx_clone.clone(),
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
        while rx_clone.is_empty() {}

        log::debug!("[BUNDLER] Aruna-read-writer started!");
        aruna_stream_writer.process().await.map_err(|e| {
            log::error!("[BUNDLER] StreamReadWriter process failed: {e}");
            e
        })
    });

    log::debug!("[BUNDLER] Start streaming!");
    let body = StreamBody::new(final_receiver_clone);

    log::debug!("[BUNDLER] Sending response!");
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(
            "Content-Disposition",
            format!("attachment; filename=\"{file_name}\""),
        )
        .body(body)?)
}

// Tell axum how to convert `RequestError` into a response.
impl IntoResponse for RequestError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Something went wrong: {}", self.0),
        )
            .into_response()
    }
}

// This enables using `?` on functions that return `Result<_, anyhow::Error>` to turn them into
// `Result<_, AppError>`. That way you don't need to do that manually.
impl<E> From<E> for RequestError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}
