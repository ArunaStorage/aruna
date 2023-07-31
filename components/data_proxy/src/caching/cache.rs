use anyhow::Result;
use s3s::auth::SecretKey;

pub struct Cache {}

impl Cache {
    pub fn new() -> Self {
        Cache {}
    }

    pub fn get_secret(&self, access_key: &str) -> Result<SecretKey> {
        Ok(SecretKey::from("access_key"))
    }
}
