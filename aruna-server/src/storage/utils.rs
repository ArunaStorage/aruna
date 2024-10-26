use jsonwebtoken::{DecodingKey, EncodingKey};

use crate::{error::ArunaError, requests::controller::KeyConfig};

pub(super) fn config_into_keys(
    (serial, encode_secret, decode_secret): KeyConfig,
) -> Result<(u32, EncodingKey, DecodingKey), ArunaError> {
    let private_pem = format!(
        "-----BEGIN PRIVATE KEY-----{}-----END PRIVATE KEY-----",
        encode_secret
    );
    let public_pem = format!(
        "-----BEGIN PUBLIC KEY-----{}-----END PUBLIC KEY-----",
        decode_secret
    );

    Ok((
        serial,
        EncodingKey::from_ed_pem(private_pem.as_bytes()).map_err(|_| {
            tracing::error!("Error creating encoding key");
            ArunaError::ConfigError("Error creating encoding key".to_string())
        })?,
        DecodingKey::from_ed_pem(public_pem.as_bytes()).map_err(|_| {
            tracing::error!("Error creating decoding key");
            ArunaError::ConfigError("Error creating decoding key".to_string())
        })?,
    ))
}
