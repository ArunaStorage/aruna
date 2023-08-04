use anyhow::Result;
use aruna_rust_api::api::storage::services::v2::{DeleteApiTokenRequest, GetApiTokenRequest};
use diesel_ulid::DieselUlid;
use std::str::FromStr;

pub struct DeleteToken(pub DeleteApiTokenRequest);
pub struct GetToken(pub GetApiTokenRequest);

impl DeleteToken {
    pub fn get_token_id(&self) -> Result<DieselUlid> {
        Ok(DieselUlid::from_str(&self.0.token_id)?)
    }
}
impl GetToken {
    pub fn get_token_id(&self) -> Result<DieselUlid> {
        Ok(DieselUlid::from_str(&self.0.token_id)?)
    }
}
