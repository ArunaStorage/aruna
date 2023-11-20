use anyhow::{anyhow, Result};
use aruna_rust_api::api::storage::{
    models::v2::permission::ResourceId,
    services::v2::{CreateServiceAccountRequest, CreateServiceAccountTokenRequest},
};
use diesel_ulid::DieselUlid;
use std::str::FromStr;

use crate::database::enums::{DbPermissionLevel, ObjectMapping};

pub struct CreateServiceAccount(pub CreateServiceAccountRequest);
pub struct CreateServiceAccountToken(pub CreateServiceAccountTokenRequest);

impl CreateServiceAccount {
    pub fn get_permissions(&self) -> Result<(DieselUlid, ObjectMapping<DbPermissionLevel>)> {
        let permission = self
            .0
            .permission
            .clone()
            .ok_or_else(|| anyhow!("No service_account permissions provided"))?;
        let level: DbPermissionLevel = permission.permission_level.try_into()?;
        let result = match permission
            .resource_id
            .ok_or_else(|| anyhow!("No resource_id provided"))?
        {
            ResourceId::ProjectId(id) => {
                (DieselUlid::from_str(&id)?, ObjectMapping::PROJECT(level))
            }
            ResourceId::CollectionId(id) => {
                (DieselUlid::from_str(&id)?, ObjectMapping::COLLECTION(level))
            }
            ResourceId::DatasetId(id) => {
                (DieselUlid::from_str(&id)?, ObjectMapping::DATASET(level))
            }
            ResourceId::ObjectId(id) => (DieselUlid::from_str(&id)?, ObjectMapping::OBJECT(level)),
        };
        Ok(result)
    }
}

impl CreateServiceAccountToken {
    pub fn get_permissions(&self) -> Result<(DieselUlid, ObjectMapping<DbPermissionLevel>)> {
        let permission = self
            .0
            .permission
            .clone()
            .ok_or_else(|| anyhow!("No service_account permissions provided"))?;
        let level: DbPermissionLevel = permission.permission_level.try_into()?;
        let result = match permission
            .resource_id
            .ok_or_else(|| anyhow!("No resource_id provided"))?
        {
            ResourceId::ProjectId(id) => {
                (DieselUlid::from_str(&id)?, ObjectMapping::PROJECT(level))
            }
            ResourceId::CollectionId(id) => {
                (DieselUlid::from_str(&id)?, ObjectMapping::COLLECTION(level))
            }
            ResourceId::DatasetId(id) => {
                (DieselUlid::from_str(&id)?, ObjectMapping::DATASET(level))
            }
            ResourceId::ObjectId(id) => (DieselUlid::from_str(&id)?, ObjectMapping::OBJECT(level)),
        };
        Ok(result)
    }
}
