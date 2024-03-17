use anyhow::Result;
use aruna_rust_api::api::storage::services::v2::{
    CreateApiTokenRequest, DeleteApiTokenRequest, GetApiTokenRequest,
};
use chrono::DateTime;
use diesel_ulid::DieselUlid;
use std::str::FromStr;

use crate::database::{
    dsls::user_dsl::APIToken,
    enums::{DbPermissionLevel, ObjectMapping},
};

#[derive(Clone)]
pub struct CreateToken(pub CreateApiTokenRequest);
pub struct DeleteToken(pub DeleteApiTokenRequest);
pub struct GetToken(pub GetApiTokenRequest);

impl CreateToken {
    pub fn build_token(&self, pubkey_serial: i32) -> Result<APIToken> {
        let (resource_id, user_right) = if let Some(perm) = &self.0.permission {
            if let Some(resource_id) = &perm.resource_id {
                let object_mapping = ObjectMapping::try_from(resource_id.clone())?;
                let perm_level = DbPermissionLevel::try_from(perm.permission_level)?;

                (Some(object_mapping), perm_level)
            } else {
                return Err(anyhow::anyhow!("Missing resource id"));
            }
        } else {
            (None, DbPermissionLevel::NONE)
        };

        Ok(APIToken {
            pub_key: pubkey_serial,
            name: self.0.name.clone(),
            created_at: chrono::Utc::now().naive_utc(),
            expires_at: if let Some(expiration) = &self.0.expires_at {
                DateTime::from_timestamp(expiration.seconds, 0).map(|e| e.naive_utc()).ok_or_else(|| anyhow::anyhow!("Timestamp conversion failed"))?
            } else {
                // Add 10 years to token lifetime if expiry unspecified
                DateTime::from_timestamp(chrono::Utc::now().timestamp() + 315360000, 0).map(|e| e.naive_utc()).ok_or_else(|| anyhow::anyhow!("Timestamp conversion failed"))?
            },
            object_id: resource_id,
            user_rights: user_right,
        })
    }
}

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
