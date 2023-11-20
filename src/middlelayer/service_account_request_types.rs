use anyhow::{anyhow, Result};
use aruna_rust_api::api::storage::{
    models::v2::permission::ResourceId,
    services::v2::{
        CreateServiceAccountRequest, CreateServiceAccountTokenRequest,
        SetServiceAccountPermissionRequest,
    },
};
use diesel_ulid::DieselUlid;
use std::str::FromStr;
use tokio_postgres::Client;

use crate::{
    auth::structs::Context,
    database::{
        crud::CrudDb,
        dsls::user_dsl::User,
        enums::{DbPermissionLevel, ObjectMapping},
    },
};

pub struct CreateServiceAccount(pub CreateServiceAccountRequest);
pub struct CreateServiceAccountToken(pub CreateServiceAccountTokenRequest);
pub struct SetServiceAccountPermission(pub SetServiceAccountPermissionRequest);

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
impl SetServiceAccountPermission {
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
    pub async fn get_previous_perms(
        &self,
        client: &Client,
    ) -> Result<(DieselUlid, ObjectMapping<DbPermissionLevel>)> {
        let service_account = User::get(self.0.svc_account_id.clone(), &client)
            .await?
            .ok_or_else(|| anyhow!("Service account not found"))?;
        let perms = service_account
            .attributes
            .0
            .permissions
            .iter()
            .next()
            .ok_or_else(|| anyhow!("No permissions found"))?;
        let (existing_id, existing_level) = perms.pair();
        Ok((*existing_id, existing_level.clone()))
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
