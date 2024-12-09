use ed25519_dalek::pkcs8::DecodePrivateKey;
use ed25519_dalek::pkcs8::DecodePublicKey;
use sha2::Digest;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("Crypto error: {message}")]
pub struct CryptoError {
    pub message: String,
}

pub fn ed25519_to_x25519_pubkey(pubkey: &str) -> Result<[u8; 32], CryptoError> {
    let key_pem = format!(
        "-----BEGIN PUBLIC KEY-----\n{}\n-----END PUBLIC KEY-----",
        pubkey
    );
    let pk =
        ed25519_dalek::VerifyingKey::from_public_key_pem(&key_pem).map_err(|e| CryptoError {
            message: e.to_string(),
        })?;
    use curve25519_dalek::edwards::CompressedEdwardsY;
    let cey = CompressedEdwardsY::from_slice(pk.as_bytes()).map_err(|e| CryptoError {
        message: e.to_string(),
    })?;
    match cey.decompress() {
        Some(ep) => Ok(*ep.to_montgomery().as_bytes()),
        None => {
            return Err(CryptoError {
                message: "Invalid public key".to_string(),
            })
        }
    }
}

pub fn ed25519_to_x25519_privatekey(privatekey: &str) -> Result<[u8; 32], CryptoError> {
    let key_pem = format!(
        "-----BEGIN PRIVATE KEY-----\n{}\n-----END PRIVATE KEY-----",
        privatekey
    );
    let sk = ed25519_dalek::SigningKey::from_pkcs8_pem(&key_pem).map_err(|e| CryptoError {
        message: e.to_string(),
    })?;
    if sk.as_bytes().len() < 32 {
        return Err(CryptoError {
            message: "Invalid private key length".to_string(),
        });
    }
    // hash secret
    let hash = sha2::Sha512::digest(&sk.as_bytes()[..32]);
    let mut output = [0u8; 32];
    output.copy_from_slice(&hash[..32]);
    Ok(output)
}
