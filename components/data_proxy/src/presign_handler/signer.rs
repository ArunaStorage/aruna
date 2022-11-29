use std::time::SystemTime;
use std::{env, time::Duration};

use anyhow::Context;
use chrono::{DateTime, Utc};

use hmac::{Hmac, Mac};
use rand::Rng;
use sha2::Sha256;

use crate::data_server::server::SignedParamsQuery;

pub const SECRET_ENV_VAR: &str = "HMAC_SIGN_KEY";

// Create alias for HMAC-SHA256
type HmacSha256 = Hmac<Sha256>;

/// Generates presigned URLs to be consumed by the data server and to be returned by the internal API
/// Uses by default HMAC-SHA256 to secure the
#[derive(Debug)]
pub struct PresignHandler {
    secret: String,
}

impl PresignHandler {
    pub fn new() -> Result<PresignHandler, Box<dyn std::error::Error + Send + Sync>> {
        let sign_secret = env::var(SECRET_ENV_VAR).context(format!(
            "could not find required env var: {}",
            SECRET_ENV_VAR
        ))?;

        let handler = PresignHandler {
            secret: sign_secret,
        };

        Ok(handler)
    }

    /// Signs a url
    pub fn sign_url(
        &self,
        duration: Duration,
        upload_id: Option<String>,
        filename: Option<String>,
        mut url: url::Url,
    ) -> Result<url::Url, Box<dyn std::error::Error>> {
        let expiry_data = SystemTime::now().checked_add(duration).unwrap();
        let expiry_data: DateTime<Utc> = expiry_data.into();
        let expiry_data_rfc3339 = expiry_data.to_rfc3339();

        let salt = rand::thread_rng().gen::<[u8; 32]>();
        let base_64_salt = base64::encode(salt);

        let sign_query_params = SignedParamsQuery {
            signature: "".to_string(),
            salt: base_64_salt.clone(),
            expiry: expiry_data_rfc3339.clone(),
            upload_id: upload_id.clone(),
            filename: filename.clone(),
        };

        let query_signature =
            self.query_signature_string(sign_query_params, url.path().to_string());

        let mut mac = HmacSha256::new_from_slice(self.secret.as_bytes()).unwrap();
        mac.update(query_signature.as_bytes());
        let result = mac.finalize();
        let signature = result.into_bytes();
        let signature_base64 = base64::encode(signature);

        url.query_pairs_mut()
            .append_pair("salt", base_64_salt.as_str());

        url.query_pairs_mut()
            .append_pair("expiry", expiry_data_rfc3339.as_str());

        url.query_pairs_mut()
            .append_pair("signature", signature_base64.as_str());

        if let Some(upload_id) = upload_id {
            url.query_pairs_mut()
                .append_pair("upload_id", upload_id.as_str());
        }

        if let Some(filename) = filename {
            url.query_pairs_mut()
                .append_pair("filename", filename.as_str());
        }

        Ok(url)
    }

    /// Verfies a signed url
    pub fn verify_sign_url(
        &self,
        sign_query_params: SignedParamsQuery,
        path: String,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let query_signature = self.query_signature_string(sign_query_params.clone(), path);

        let signature_hmac_key = match base64::decode(sign_query_params.signature) {
            Ok(value) => value,
            Err(err) => {
                log::error!("{}", err);
                return Err(Box::new(err));
            }
        };

        let mut mac = HmacSha256::new_from_slice(self.secret.as_bytes()).unwrap();
        mac.update(query_signature.as_bytes());

        match mac.verify_slice(signature_hmac_key.as_slice()) {
            Ok(_) => {}
            Err(_) => return Ok(false),
        }

        let expiry_time = match DateTime::parse_from_rfc3339(sign_query_params.expiry.as_str()) {
            Ok(expiry_time) => expiry_time.naive_utc(),
            Err(err) => {
                log::error!("{}", err);
                return Ok(false);
            }
        };

        if chrono::offset::Utc::now().naive_utc() > expiry_time {
            return Ok(false);
        }

        return Ok(true);
    }

    fn query_signature_string(&self, sign_query_params: SignedParamsQuery, path: String) -> String {
        let mut params = vec![sign_query_params.expiry, sign_query_params.salt, path];
        if let Some(upload_id) = sign_query_params.upload_id {
            params.push(upload_id);
        }

        params.join("|")
    }
}

#[cfg(test)]
mod tests {
    use std::{env, str::FromStr, thread, time::Duration};

    use crate::data_server::server::SignedParamsQuery;

    use super::{PresignHandler, SECRET_ENV_VAR};

    #[test]
    fn test_signer_expired() {
        env::set_var(SECRET_ENV_VAR, "test");

        let signer = PresignHandler::new().unwrap();
        let path = "/test/path/1/3".to_string();
        let duration = Duration::new(1, 0);

        let url = url::Url::from_str(format!("{}{}", "http://example.com", path).as_str()).unwrap();

        let url = signer.sign_url(duration, None, None, url).unwrap();

        let mut query_sign_params = SignedParamsQuery {
            ..Default::default()
        };
        for (key, value) in url.query_pairs() {
            match key.to_string().as_str() {
                "expiry" => {
                    query_sign_params.expiry = value.to_string();
                }
                "salt" => {
                    query_sign_params.salt = value.to_string();
                }
                "signature" => {
                    query_sign_params.signature = value.to_string();
                }
                _ => {}
            }
        }

        let is_valid = signer
            .verify_sign_url(query_sign_params.clone(), path.clone())
            .unwrap();
        assert!(is_valid);

        thread::sleep(Duration::new(2, 0));

        let is_valid = signer
            .verify_sign_url(query_sign_params.clone(), path)
            .unwrap();
        assert!(!is_valid);

        let bad_path = "/test/path/2/3".to_string();
        let is_valid = signer.verify_sign_url(query_sign_params, bad_path).unwrap();
        assert!(!is_valid);
    }

    #[test]
    fn test_signer() {
        env::set_var(SECRET_ENV_VAR, "test");

        let signer = PresignHandler::new().unwrap();
        let path = "/test/path/1/3".to_string();
        let duration = Duration::new(15 * 60, 0);

        let url = url::Url::from_str(format!("{}{}", "http://example.com", path).as_str()).unwrap();

        let url = signer.sign_url(duration, None, None, url).unwrap();

        let mut query_sign_params = SignedParamsQuery {
            ..Default::default()
        };
        for (key, value) in url.query_pairs() {
            match key.to_string().as_str() {
                "expiry" => {
                    query_sign_params.expiry = value.to_string();
                }
                "salt" => {
                    query_sign_params.salt = value.to_string();
                }
                "signature" => {
                    query_sign_params.signature = value.to_string();
                }
                _ => {}
            }
        }

        let is_valid = signer
            .verify_sign_url(query_sign_params.clone(), path)
            .unwrap();
        assert!(is_valid);

        let bad_path = "/test/path/2/3".to_string();
        let is_valid = signer.verify_sign_url(query_sign_params, bad_path).unwrap();
        assert!(!is_valid);
    }
}
