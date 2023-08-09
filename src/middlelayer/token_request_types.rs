use crate::database::enums::{DbPermissionLevel, ObjectMapping};
use anyhow::{anyhow, Result};
use aruna_rust_api::api::storage::models::v2::permission::ResourceId;
use aruna_rust_api::api::storage::services::v2::{
    CreateApiTokenRequest, DeleteApiTokenRequest, GetApiTokenRequest, GetS3CredentialsUserRequest,
};
use chrono::{Months, NaiveDateTime, Utc};
use diesel_ulid::DieselUlid;
use std::str::FromStr;

pub struct DeleteToken(pub DeleteApiTokenRequest);
pub struct GetToken(pub GetApiTokenRequest);
pub struct CreateToken(pub CreateApiTokenRequest);
pub struct GetS3(pub GetS3CredentialsUserRequest); // TODO dataproxy impl

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

impl CreateToken {
    pub fn get_resource(&self) -> Result<Option<ObjectMapping<DieselUlid>>> {
        Ok(match &self.0.permission {
            Some(perm) => match &perm.resource_id {
                Some(res) => match res {
                    ResourceId::ProjectId(id) => {
                        Some(ObjectMapping::PROJECT(DieselUlid::from_str(id)?))
                    }
                    ResourceId::CollectionId(id) => {
                        Some(ObjectMapping::COLLECTION(DieselUlid::from_str(id)?))
                    }
                    ResourceId::DatasetId(id) => {
                        Some(ObjectMapping::DATASET(DieselUlid::from_str(id)?))
                    }

                    ResourceId::ObjectId(id) => {
                        Some(ObjectMapping::OBJECT(DieselUlid::from_str(id)?))
                    }
                },
                None => None,
            },
            None => return Err(anyhow!("No permissions provided")),
        })
    }

    pub fn get_expiry(&self) -> Result<NaiveDateTime> {
        let exp = match &self.0.expires_at {
            Some(exp) => NaiveDateTime::from_timestamp_opt(exp.seconds, exp.nanos.try_into()?)
                .ok_or_else(|| anyhow!("Timestamp conversion error"))?,
            None => Utc::now()
                .naive_local()
                .checked_add_months(Months::new(6))
                .ok_or_else(|| anyhow!("Expiry creation error"))?,
        };
        Ok(exp)
    }
    pub fn get_rights(&self) -> Result<DbPermissionLevel> {
        match &self.0.permission {
            Some(perm) => Ok(perm.permission_level.try_into()?),
            None => Err(anyhow!("No permissions provided")),
        }
    }
}

impl GetS3 {
    pub fn get_user_id(&self) -> Result<DieselUlid> {
        Ok(DieselUlid::from_str(&self.0.user_id)?)
    }
    pub fn get_endpoint_id(&self) -> Result<DieselUlid> {
        Ok(DieselUlid::from_str(&self.0.endpoint_id)?)
    }
}
