use crate::data_backends::storage_backend::StorageBackend;
use crate::s3_frontend::utils::hashing_transformer::HashingTransformer;
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
use hmac::digest::core_api::CoreWrapper;
use hmac::digest::core_api::CtVariableCoreWrapper;
use md5::Md5Core;
use md5::{Digest, Md5};
use sha2::Sha256;
use sha2::Sha256VarCore;
use std::sync::Arc;

#[derive(Debug)]
pub struct DataHandler {}

impl DataHandler {
    pub async fn finalize_location(
        backend: Arc<Box<dyn StorageBackend>>,
        before_location: &ObjectLocation,
        new_location: &mut ObjectLocation,
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

            let (sha_transformer, sha_recv) = HashingTransformer::new(Sha256::new());
            let (md5_transformer, md5_recv) = HashingTransformer::new(Md5::new());

            asr = asr.add_transformer(sha_transformer);
            asr = asr.add_transformer(md5_transformer);

            asr.process().await?;

            match 1 {
                1 => Ok((
                    orig_size_stream.try_recv()?,
                    uncompressed_stream.try_recv()?,
                    sha_recv.try_recv()?.data,
                    md5_recv.try_recv()?.data,
                )),
                _ => Err(anyhow!("Will not occur")),
            }
        });

        backend.get_object(location.clone(), None, tx_send).await?;
        let (before_size, after_size, sha, md5) = aswr_handle.await??;

        log::debug!(
            "Finished finalizing location {:?}/{:?}",
            location.bucket,
            location.key
        );

        new_location.disk_content_len = before_size as i64;
        new_location.raw_content_len = after_size as i64;

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
