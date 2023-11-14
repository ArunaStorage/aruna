use crate::data_backends::storage_backend::StorageBackend;
use crate::s3_frontend::utils::buffered_s3_sink::BufferedS3Sink;
use crate::structs::ObjectLocation;
use crate::trace_err;
use anyhow::anyhow;
use anyhow::Result;
use aruna_file::streamreadwrite::ArunaStreamReadWriter;
use aruna_file::transformer::ReadWriter;
use aruna_file::transformers::decrypt::ChaCha20Dec;
use aruna_file::transformers::encrypt::ChaCha20Enc;
use aruna_file::transformers::hashing_transformer::HashingTransformer;
use aruna_file::transformers::size_probe::SizeProbe;
use aruna_file::transformers::zstd_comp::ZstdEnc;
use aruna_file::transformers::zstd_decomp::ZstdDec;
use aruna_rust_api::api::storage::models::v2::Hash;
use aruna_rust_api::api::storage::models::v2::Hashalgorithm;
use md5::{Digest, Md5};
use sha2::Sha256;
use std::fmt::Debug;
use std::sync::Arc;
use tracing::debug;
use tracing::info_span;
use tracing::Instrument;

#[derive(Debug)]
pub struct DataHandler {}

impl DataHandler {
    #[tracing::instrument(level = "trace", skip(backend, before_location, new_location))]
    pub async fn finalize_location(
        backend: Arc<Box<dyn StorageBackend>>,
        before_location: &ObjectLocation,
        new_location: &mut ObjectLocation,
    ) -> Result<Vec<Hash>> {
        debug!(?before_location, ?new_location, "Finalizing location");

        let (tx_send, tx_receive) = async_channel::bounded(10);

        let clone_key: Option<Vec<u8>> = before_location
            .clone()
            .encryption_key
            .map(|k| k.as_bytes().to_vec());

        let after_key: Option<Vec<u8>> = new_location
            .clone()
            .encryption_key
            .map(|k| k.as_bytes().to_vec());

        let before_location = before_location.clone();
        let backend_clone = backend.clone();
        let new_location_clone = new_location.clone();

        let aswr_handle = tokio::spawn(
            async move {
                let (sink, _) = BufferedS3Sink::new(
                    backend_clone,
                    new_location_clone,
                    None,
                    None,
                    false,
                    None,
                    false,
                );

                // Bind to variable to extend the lifetime of arsw to the end of the function
                let mut asr = ArunaStreamReadWriter::new_with_sink(tx_receive.clone(), sink);
                let (orig_probe, orig_size_stream) = SizeProbe::new();
                asr = asr.add_transformer(orig_probe);

                if let Some(key) = clone_key.clone() {
                    asr = asr.add_transformer(trace_err!(ChaCha20Dec::new(Some(key)))?);
                }

                if before_location.compressed {
                    asr = asr.add_transformer(ZstdDec::new());
                }

                let (uncompressed_probe, uncompressed_stream) = SizeProbe::new();

                asr = asr.add_transformer(uncompressed_probe);

                let (sha_transformer, sha_recv) = HashingTransformer::new(Sha256::new());
                let (md5_transformer, md5_recv) = HashingTransformer::new(Md5::new());

                asr = asr.add_transformer(sha_transformer);
                asr = asr.add_transformer(md5_transformer);
                asr = asr.add_transformer(ZstdEnc::new(true));
                asr = asr.add_transformer(ChaCha20Enc::new(
                    false,
                    after_key.ok_or_else(|| anyhow!("Missing encryption_key"))?,
                )?);

                let (final_sha, final_sha_recv) = HashingTransformer::new(Sha256::new());

                asr = asr.add_transformer(final_sha);
                trace_err!(asr.process().await)?;

                Ok::<(u64, u64, String, String, String), anyhow::Error>((
                    trace_err!(orig_size_stream.try_recv())?,
                    trace_err!(uncompressed_stream.try_recv())?,
                    trace_err!(sha_recv.try_recv())?,
                    trace_err!(md5_recv.try_recv())?,
                    trace_err!(final_sha_recv.try_recv())?,
                ))
            }
            .instrument(info_span!("finalize_location")),
        );

        trace_err!(
            backend
                .get_object(before_location.clone(), None, tx_send)
                .await
        )?;

        //

        let (before_size, after_size, sha, md5, final_sha) =
            trace_err!(trace_err!(aswr_handle.await)?)?;

        debug!(new_location = ?new_location, "Finished finalizing location");

        new_location.disk_content_len = before_size as i64;
        new_location.raw_content_len = after_size as i64;
        new_location.disk_hash = Some(final_sha);

        Ok(vec![
            Hash {
                alg: Hashalgorithm::Sha256.into(),
                hash: sha,
            },
            Hash {
                alg: Hashalgorithm::Md5.into(),
                hash: md5,
            },
        ])
    }
}
