use std::str::FromStr;
use std::time::SystemTime;
use std::{env, time::Duration};

use anyhow::Context;
use chrono::{DateTime, Utc};

use hmac::{Hmac, Mac};
use rand::Rng;
use sha2::Sha256;

pub const SALT_QUERY_NAME: &str = "salt";
pub const EXPIRY_QUERY_NAME: &str = "expiry";
pub const HMAC_SIGN_BASE_64: &str = "sha265-hmac-sign";
pub const SECRET_ENV_VAR: &str = "HMAC_SIGN_KEY";
pub const DATA_HOSTNAME_ENV_VAR: &str = "PROXY_DATA_HOST";

// Create alias for HMAC-SHA256
type HmacSha256 = Hmac<Sha256>;

/// Generates presigned URLs to be consumed by the data server and to be returned by the internal API
/// Uses by default HMAC-SHA256 to secure the
#[derive(Debug)]
pub struct PresignHandler {
    base_url: String,
    secret: String,
}

impl PresignHandler {
    pub fn new() -> Result<PresignHandler, Box<dyn std::error::Error + Send + Sync>> {
        let sign_secret = env::var(SECRET_ENV_VAR).context(format!(
            "could not find required env var: {}",
            SECRET_ENV_VAR
        ))?;

        let hostname = env::var(DATA_HOSTNAME_ENV_VAR).context(format!(
            "could not find required env var: {}",
            DATA_HOSTNAME_ENV_VAR
        ))?;

        let handler = PresignHandler {
            base_url: hostname,
            secret: sign_secret,
        };

        return Ok(handler);
    }

    /// Signs a url
    pub fn get_sign_url(
        &self,
        path: String,
        duration: Duration,
    ) -> Result<url::Url, Box<dyn std::error::Error>> {
        let expiry_data = SystemTime::now().checked_add(duration).unwrap();
        let expiry_data: DateTime<Utc> = expiry_data.into();
        let expiry_data_rfc3339 = expiry_data.to_rfc3339();

        let salt = rand::thread_rng().gen::<[u8; 32]>();
        let base_64_salt = base64::encode(salt);

        let mut url = url::Url::parse(self.base_url.as_str())?;
        url.set_path(path.as_str());
        url.query_pairs_mut()
            .append_pair(SALT_QUERY_NAME, base_64_salt.as_str())
            .append_pair(EXPIRY_QUERY_NAME, expiry_data_rfc3339.as_str());

        let sign_url = url.as_str();

        let mut mac = HmacSha256::new_from_slice(self.secret.as_bytes()).unwrap();
        mac.update(sign_url.as_bytes());
        let result = mac.finalize();
        let signature = result.into_bytes();
        let signature_base64 = base64::encode(signature);

        url.query_pairs_mut()
            .append_pair(HMAC_SIGN_BASE_64, signature_base64.as_str());

        Ok(url)
    }

    /// Verfies a signed url
    pub fn verify_sign_url(&self, url: url::Url) -> Result<bool, Box<dyn std::error::Error>> {
        let scheme = url.scheme().to_string();
        let host = url.host().context("host not found in url")?.to_string();

        let base_url_parse = format!("{}://{}", scheme, host);

        let mut sign_url = match url::Url::from_str(base_url_parse.as_str()) {
            Ok(value) => value,
            Err(err) => {
                log::error!("{}", err);
                return Err(Box::new(err));
            }
        };

        sign_url.set_path(url.path());

        let mut signature = "".to_string();

        for query in url.query_pairs() {
            match query.0.to_string().as_str() {
                EXPIRY_QUERY_NAME => {
                    sign_url
                        .query_pairs_mut()
                        .append_pair(EXPIRY_QUERY_NAME, query.1.to_string().as_str());
                }
                SALT_QUERY_NAME => {
                    sign_url
                        .query_pairs_mut()
                        .append_pair(SALT_QUERY_NAME, query.1.to_string().as_str());
                }
                HMAC_SIGN_BASE_64 => {
                    signature = query.1.to_string();
                }
                _ => {}
            }
        }

        let verified_url = sign_url.as_str();
        let signature = match base64::decode(signature) {
            Ok(value) => value,
            Err(err) => {
                log::error!("{}", err);
                return Err(Box::new(err));
            }
        };

        let mut mac = HmacSha256::new_from_slice(self.secret.as_bytes()).unwrap();
        mac.update(verified_url.as_bytes());

        match mac.verify_slice(signature.as_slice()) {
            Ok(_) => return Ok(true),
            Err(_) => return Ok(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{env, time::Duration};

    use crate::presign_handler::signer::DATA_HOSTNAME_ENV_VAR;

    use super::{PresignHandler, SECRET_ENV_VAR};

    #[test]
    fn test_signer() {
        env::set_var(SECRET_ENV_VAR, "test");
        env::set_var(DATA_HOSTNAME_ENV_VAR, "http://example.com");

        let signer = PresignHandler::new().unwrap();
        let path = "/test/path/1/3".to_string();
        let duration = Duration::new(15 * 60, 0);

        let signed_url = signer.get_sign_url(path.clone(), duration).unwrap();
        let is_valid = signer.verify_sign_url(signed_url).unwrap();
        assert_eq!(is_valid, true);

        let mut signed_url = signer.get_sign_url(path.clone(), duration).unwrap();
        let bad_path = "/test/path/2/3".to_string();
        signed_url.set_path(bad_path.as_str());
        let is_valid = signer.verify_sign_url(signed_url).unwrap();
        assert_eq!(is_valid, false);
    }
}
