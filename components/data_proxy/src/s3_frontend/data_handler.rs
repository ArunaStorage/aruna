use crate::caching::cache::Cache;
use crate::data_backends::storage_backend::StorageBackend;
use crate::s3_frontend::utils::buffered_s3_sink::BufferedS3Sink;
use crate::structs::ObjectLocation;
use anyhow::anyhow;
use anyhow::Result;
use aruna_rust_api::api::storage::models::v2::Hash;
use aruna_rust_api::api::storage::models::v2::Hashalgorithm;
use diesel_ulid::DieselUlid;
use md5::{Digest, Md5};
use pithos_lib::helpers::structs::FileContext;
use pithos_lib::streamreadwrite::GenericStreamReadWriter;
use pithos_lib::transformer::ReadWriter;
use pithos_lib::transformers::decrypt::ChaCha20Dec;
use pithos_lib::transformers::encrypt::ChaCha20Enc;
use pithos_lib::transformers::footer::FooterGenerator;
use pithos_lib::transformers::hashing_transformer::HashingTransformer;
use pithos_lib::transformers::size_probe::SizeProbe;
use pithos_lib::transformers::zstd_comp::ZstdEnc;
use pithos_lib::transformers::zstd_decomp::ZstdDec;
use sha2::Sha256;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::pin;
use tracing::debug;
use tracing::error;
use tracing::info_span;
use tracing::Instrument;

#[derive(Debug)]
pub struct DataHandler {}

impl DataHandler {
    #[tracing::instrument(level = "trace", skip(cache, backend, before_location, new_location))]
    pub async fn finalize_location(
        object_id: DieselUlid,
        cache: Arc<Cache>,
        token: String,
        backend: Arc<Box<dyn StorageBackend>>,
        ctx: FileContext,
        before_location: ObjectLocation,
        mut new_location: ObjectLocation,
    ) -> Result<()> {
        debug!(?before_location, ?new_location, "Finalizing location");

        let (tx_send, tx_receive) = async_channel::bounded(10);

        let clone_key = before_location.get_encryption_key();

        let after_key = new_location.get_encryption_key();

        let before_location = before_location.clone();
        let backend_clone = backend.clone();
        let new_location_clone = new_location.clone();
        let is_compressed = before_location.file_format.is_compressed();

        let aswr_handle = tokio::spawn(
            async move {
                let (tx, rx) = async_channel::bounded(10);
                let (sink, _) = BufferedS3Sink::new(
                    backend_clone,
                    new_location_clone,
                    None,
                    None,
                    false,
                    None,
                    false,
                );

                pin!(tx_receive);
                // Bind to variable to extend the lifetime of arsw to the end of the function
                let mut asr = GenericStreamReadWriter::new_with_sink(tx_receive, sink);
                let (orig_probe, orig_size_stream) = SizeProbe::new();
                asr = asr.add_transformer(orig_probe);
                asr.add_message_receiver(rx).await?;
                tx.send(pithos_lib::helpers::notifications::Message::FileContext(
                    ctx,
                ))
                .await?;

                if let Some(key) = clone_key.clone() {
                    asr = asr.add_transformer(ChaCha20Dec::new_with_fixed(key).map_err(|e| {
                        error!(error = ?e, msg = e.to_string());
                        tonic::Status::unauthenticated(e.to_string())
                    })?);
                }

                if is_compressed {
                    asr = asr.add_transformer(ZstdDec::new());
                }

                let (uncompressed_probe, uncompressed_stream) = SizeProbe::new();

                asr = asr.add_transformer(uncompressed_probe);

                let (sha_transformer, sha_recv) =
                    HashingTransformer::new_with_backchannel(Sha256::new(), "sha256".to_string());
                let (md5_transformer, md5_recv) =
                    HashingTransformer::new_with_backchannel(Md5::new(), "md5".to_string());

                asr = asr.add_transformer(sha_transformer);
                asr = asr.add_transformer(md5_transformer);
                asr = asr.add_transformer(ZstdEnc::new());
                asr = asr.add_transformer(FooterGenerator::new(None));
                asr = asr.add_transformer(ChaCha20Enc::new_with_fixed(
                    after_key.ok_or_else(|| anyhow!("Missing encryption_key"))?,
                )?);

                let (final_sha, final_sha_recv) =
                    HashingTransformer::new_with_backchannel(Sha256::new(), "sha256".to_string());

                asr = asr.add_transformer(final_sha);
                asr.process().await.map_err(|e| {
                    error!(error = ?e, msg = e.to_string());
                    tonic::Status::unauthenticated(e.to_string())
                })?;

                Ok::<(u64, u64, String, String, String), anyhow::Error>((
                    orig_size_stream.try_recv().map_err(|e| {
                        error!(error = ?e, msg = e.to_string());
                        tonic::Status::unauthenticated(e.to_string())
                    })?,
                    uncompressed_stream.try_recv().map_err(|e| {
                        error!(error = ?e, msg = e.to_string());
                        tonic::Status::unauthenticated(e.to_string())
                    })?,
                    sha_recv.try_recv().map_err(|e| {
                        error!(error = ?e, msg = e.to_string());
                        tonic::Status::unauthenticated(e.to_string())
                    })?,
                    md5_recv.try_recv().map_err(|e| {
                        error!(error = ?e, msg = e.to_string());
                        tonic::Status::unauthenticated(e.to_string())
                    })?,
                    final_sha_recv.try_recv().map_err(|e| {
                        error!(error = ?e, msg = e.to_string());
                        tonic::Status::unauthenticated(e.to_string())
                    })?,
                ))
            }
            .instrument(info_span!("finalize_location")),
        );

        backend
            .get_object(before_location.clone(), None, tx_send)
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                tonic::Status::unauthenticated(e.to_string())
            })?;

        //

        let (before_size, after_size, sha, md5, final_sha) = aswr_handle
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                tonic::Status::unauthenticated(e.to_string())
            })?
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                tonic::Status::unauthenticated(e.to_string())
            })?;

        debug!(new_location = ?new_location, "Finished finalizing location");

        new_location.disk_content_len = before_size as i64;
        new_location.raw_content_len = after_size as i64;
        new_location.disk_hash = Some(final_sha);

        let hashes = vec![
            Hash {
                alg: Hashalgorithm::Sha256.into(),
                hash: sha,
            },
            Hash {
                alg: Hashalgorithm::Md5.into(),
                hash: md5,
            },
        ];

        if let Some(handler) = cache.aruna_client.read().await.as_ref() {
            // Set id of new location to object id to satisfy FK constraint
            let _object = handler
                .finish_object(
                    object_id,
                    new_location.raw_content_len,
                    hashes,
                    None,
                    &token,
                )
                .await?;

            cache.update_location(object_id, new_location).await?;

            backend.delete_object(before_location).await?;
        }

        Ok(())
    }
}
