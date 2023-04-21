use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct ServiceSettings {
    pub endpoint_id: rusty_ulid::Ulid,
    pub encrypting: bool,
    pub compressing: bool,
}

impl Default for ServiceSettings {
    fn default() -> Self {
        Self {
            endpoint_id: rusty_ulid::Ulid::from_str("00000000000000000000000000").unwrap(), // No default implementation ...
            encrypting: true,
            compressing: true,
        }
    }
}
