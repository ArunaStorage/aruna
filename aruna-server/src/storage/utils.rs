use jsonwebtoken::{DecodingKey, EncodingKey};

use crate::{crypto, error::ArunaError, transactions::controller::KeyConfig};

pub(crate) struct SigningInfoCodec;

impl heed::BytesEncode<'_> for SigningInfoCodec {
    type EItem = (u32, EncodingKey, [u8; 32], DecodingKey);

    fn bytes_encode(item: &Self::EItem) -> Result<std::borrow::Cow<'_, [u8]>, heed::BoxedError> {
        let serial = item.0.to_be_bytes();
        let encode_secret = item.1.as_bytes();
        let decode_secret = item.3.as_bytes();

        let mut buffer = vec![];
        buffer.extend_from_slice(&serial);
        buffer.extend_from_slice((encode_secret.len() as u64).to_be_bytes().as_ref());
        buffer.extend_from_slice(encode_secret);
        buffer.extend_from_slice(&item.2);
        buffer.extend_from_slice(decode_secret);
        Ok(std::borrow::Cow::Owned(buffer))
    }
}

impl heed::BytesDecode<'_> for SigningInfoCodec {
    type DItem = (u32, EncodingKey, [u8; 32], DecodingKey);

    fn bytes_decode(bytes: &'_ [u8]) -> Result<Self::DItem, heed::BoxedError> {
        let serial = u32::from_be_bytes(bytes[0..4].try_into()?);
        let encoded_secret_len = u64::from_be_bytes(bytes[4..12].try_into()?) as usize;
        let encoded_secret =
            EncodingKey::from_ed_der(bytes[12..12 + encoded_secret_len].try_into()?);
        let mut x25519_sec_key = [0u8; 32];
        x25519_sec_key.copy_from_slice(&bytes[12 + encoded_secret_len..44 + encoded_secret_len]);
        let decoded_secret = DecodingKey::from_ed_der(&bytes[44 + encoded_secret_len..]);
        Ok((serial, encoded_secret, x25519_sec_key, decoded_secret))
    }
}

pub(super) fn config_into_keys(
    (serial, encode_secret, decode_secret): &KeyConfig,
) -> Result<(u32, EncodingKey, [u8; 32], DecodingKey), ArunaError> {
    let x25519_seckey = crypto::ed25519_to_x25519_privatekey(&encode_secret).map_err(|e| {
        tracing::error!(
            "Error converting ed25519 to x25519 private key: {}",
            e.message
        );
        ArunaError::ConfigError("Error converting ed25519 to x25519 private key".to_string())
    })?;

    let private_pem = format!(
        "-----BEGIN PRIVATE KEY-----{}-----END PRIVATE KEY-----",
        encode_secret
    );
    let public_pem = format!(
        "-----BEGIN PUBLIC KEY-----{}-----END PUBLIC KEY-----",
        decode_secret
    );

    Ok((
        *serial,
        EncodingKey::from_ed_pem(private_pem.as_bytes()).map_err(|_| {
            tracing::error!("Error creating encoding key");
            ArunaError::ConfigError("Error creating encoding key".to_string())
        })?,
        x25519_seckey,
        DecodingKey::from_ed_pem(public_pem.as_bytes()).map_err(|_| {
            tracing::error!("Error creating decoding key");
            ArunaError::ConfigError("Error creating decoding key".to_string())
        })?,
    ))
}

pub fn pubkey_from_pem(pem: &str) -> Result<(DecodingKey, [u8; 32]), ArunaError> {
    let public_pem = format!("-----BEGIN PUBLIC KEY-----{}-----END PUBLIC KEY-----", pem);

    let x25519_pubkey = crypto::ed25519_to_x25519_pubkey(pem).map_err(|e| {
        tracing::error!(
            "Error converting ed25519 to x25519 public key: {}",
            e.message
        );
        ArunaError::ConfigError("Error converting ed25519 to x25519 public key".to_string())
    })?;

    Ok((
        DecodingKey::from_ed_pem(pem.as_bytes()).map_err(|_| {
            tracing::error!("Error creating decoding key");
            ArunaError::ConfigError("Error creating decoding key".to_string())
        })?,
        x25519_pubkey,
    ))
}
