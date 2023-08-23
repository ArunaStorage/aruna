use crate::data_backends::storage_backend::StorageBackend;
use crate::s3_frontend::utils::buffered_s3_sink::BufferedS3Sink;
use crate::structs::ObjectLocation;
use anyhow::anyhow;
use anyhow::Result;
use aruna_file::streamreadwrite::ArunaStreamReadWriter;
use aruna_file::transformer::ReadWriter;
use aruna_file::transformers::async_sender_sink::AsyncSenderSink;
use aruna_file::transformers::decrypt::ChaCha20Dec;
use aruna_file::transformers::size_probe::SizeProbe;
use aruna_file::transformers::zstd_decomp::ZstdDec;
use aruna_rust_api::api::storage::models::v2::Hash;
use aruna_rust_api::api::storage::models::v2::Hashalgorithm;
use futures::future;
use futures::StreamExt;
use md5::{Digest, Md5};
use sha2::Sha256;
use std::sync::Arc;

#[derive(Debug)]
pub struct DataHandler {}

impl DataHandler {
    pub async fn finalize_location(
        backend: Arc<Box<dyn StorageBackend>>,
        before_location: &mut ObjectLocation,
    ) -> Result<Vec<Hash>> {
        let location = before_location.clone();

        log::debug!(
            "Finalizing {:?}/{:?}",
            location.bucket.to_string(),
            location.key.to_string()
        );

        let (tx_send, tx_receive) = async_channel::bounded(10);

        let clone_key = location
            .clone()
            .encryption_key
            .map(|k| k.as_bytes().to_vec());
        let mut md5_hash = Md5::new();
        let mut sha256_hash = Sha256::new();

        let (transform_send, transform_receive) = async_channel::bounded(10);

        let aswr_handle = tokio::spawn(async move {
            // Bind to variable to extend the lifetime of arsw to the end of the function
            let mut asr = ArunaStreamReadWriter::new_with_sink(
                tx_receive.clone(),
                AsyncSenderSink::new(transform_send),
            );
            let (orig_probe, orig_size_stream) = SizeProbe::new();
            asr = asr.add_transformer(orig_probe);

            if let Some(key) = clone_key.clone() {
                asr = asr.add_transformer(ChaCha20Dec::new(Some(key))?);
            }

            if location.compressed {
                asr = asr.add_transformer(ZstdDec::new());
            }

            let (uncompressed_probe, uncompressed_stream) = SizeProbe::new();

            asr = asr.add_transformer(uncompressed_probe);

            asr.process().await?;

            match 1 {
                1 => Ok((
                    orig_size_stream.try_recv()?,
                    uncompressed_stream.try_recv()?,
                )),
                _ => Err(anyhow!("Will not occur")),
            }
        });

        let hashing_handle = tokio::spawn(async move {
            let md5_str = transform_receive.inspect(|res_bytes| {
                if let Ok(bytes) = res_bytes {
                    md5_hash.update(bytes)
                }
            });
            let sha_str = md5_str.inspect(|res_bytes| {
                if let Ok(bytes) = res_bytes {
                    sha256_hash.update(bytes)
                }
            });
            sha_str.for_each(|_| future::ready(())).await;
            (
                format!("{:x}", sha256_hash.finalize()),
                format!("{:x}", md5_hash.finalize()),
            )
        });

        backend.get_object(location.clone(), None, tx_send).await?;
        let (before_size, after_size) = aswr_handle.await??;
        let (sha, md5) = hashing_handle.await?;

        log::debug!(
            "Finished finalizing location {:?}/{:?}",
            location.bucket,
            location.key
        );

        before_location.disk_content_len = before_size as i64;
        before_location.raw_content_len = after_size as i64;

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
