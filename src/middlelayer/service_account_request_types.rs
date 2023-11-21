use crate::{
    auth::structs::Context,
    database::{
        crud::CrudDb,
        dsls::user_dsl::User,
        enums::{DbPermissionLevel, ObjectMapping},
    },
};
use anyhow::{anyhow, Result};
use aruna_rust_api::api::storage::{
    models::v2::permission::ResourceId,
    services::v2::{
        CreateServiceAccountRequest, CreateServiceAccountTokenRequest, DeleteServiceAccountRequest,
        DeleteServiceAccountTokenRequest, DeleteServiceAccountTokensRequest,
        GetServiceAccountTokenRequest, GetServiceAccountTokensRequest,
        SetServiceAccountPermissionRequest,
    },
};
use diesel_ulid::DieselUlid;
use std::str::FromStr;
use tokio_postgres::Client;

/// Wrappers for requests
pub struct CreateServiceAccount(pub CreateServiceAccountRequest);
pub struct CreateServiceAccountToken(pub CreateServiceAccountTokenRequest);
pub struct SetServiceAccountPermission(pub SetServiceAccountPermissionRequest);
pub struct GetServiceAccountToken(pub GetServiceAccountTokenRequest);
pub struct GetServiceAccountTokens(pub GetServiceAccountTokensRequest);
pub struct DeleteServiceAccountToken(pub DeleteServiceAccountTokenRequest);
pub struct DeleteServiceAccountTokens(pub DeleteServiceAccountTokensRequest);
pub struct DeleteServiceAccount(pub DeleteServiceAccountRequest);

/// Impls for wrappers
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
            _ => return Err(anyhow!("Service accounts must have project permissions")),
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
        let service_account = User::get(self.0.svc_account_id.clone(), client)
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
        Ok((*existing_id, *existing_level))
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
impl GetServiceAccountToken {
    /// Returns service_account_id and token_id as ULIDs
    pub fn get_ids(&self) -> Result<(DieselUlid, DieselUlid)> {
        let service_account = DieselUlid::from_str(&self.0.svc_account_id)?;
        let token_id = DieselUlid::from_str(&self.0.token_id)?;
        Ok((service_account, token_id))
    }

    /// Returns service_account
    pub async fn get_service_account(&self, client: &Client) -> Result<User> {
        let (service_account_id, _) = self.get_ids()?;
        let service_account = User::get(service_account_id, client)
            .await?
            .ok_or_else(|| anyhow!("Service account not found"))?;
        Ok(service_account)
    }
    pub fn get_context(user: &User) -> Result<Context> {
        let perms = user
            .attributes
            .0
            .permissions
            .iter()
            .next()
            .ok_or_else(|| anyhow!("Expected exactly one permission for service account"))?;
        let (id, _) = perms.pair();
        Ok(Context::res_ctx(*id, DbPermissionLevel::ADMIN, false))
    }
}

impl GetServiceAccountTokens {
    pub fn get_id(&self) -> Result<DieselUlid> {
        let service_account = DieselUlid::from_str(&self.0.svc_account_id)?;
        Ok(service_account)
    }

    /// Returns service_account
    pub async fn get_service_account(&self, client: &Client) -> Result<User> {
        let service_account_id = self.get_id()?;
        let service_account = User::get(service_account_id, client)
            .await?
            .ok_or_else(|| anyhow!("Service account not found"))?;
        Ok(service_account)
    }
    pub fn get_context(user: &User) -> Result<Context> {
        let perms = user
            .attributes
            .0
            .permissions
            .iter()
            .next()
            .ok_or_else(|| anyhow!("Expected exactly one permission for service account"))?;
        let (id, _) = perms.pair();
        Ok(Context::res_ctx(*id, DbPermissionLevel::ADMIN, false))
    }
}

impl DeleteServiceAccountToken {
    /// Returns service_account_id and token_id as ULIDs
    pub fn get_ids(&self) -> Result<(DieselUlid, DieselUlid)> {
        let service_account = DieselUlid::from_str(&self.0.svc_account_id)?;
        let token_id = DieselUlid::from_str(&self.0.token_id)?;
        Ok((service_account, token_id))
    }

    /// Returns service_account
    pub async fn get_service_account(&self, client: &Client) -> Result<User> {
        let (service_account_id, _) = self.get_ids()?;
        let service_account = User::get(service_account_id, client)
            .await?
            .ok_or_else(|| anyhow!("Service account not found"))?;
        Ok(service_account)
    }
    pub fn get_context(user: &User) -> Result<Context> {
        let perms = user
            .attributes
            .0
            .permissions
            .iter()
            .next()
            .ok_or_else(|| anyhow!("Expected exactly one permission for service account"))?;
        let (id, _) = perms.pair();
        Ok(Context::res_ctx(*id, DbPermissionLevel::ADMIN, false))
    }
}

impl DeleteServiceAccountTokens {
    pub fn get_id(&self) -> Result<DieselUlid> {
        let service_account = DieselUlid::from_str(&self.0.svc_account_id)?;
        Ok(service_account)
    }

    /// Returns service_account
    pub async fn get_service_account(&self, client: &Client) -> Result<User> {
        let service_account_id = self.get_id()?;
        let service_account = User::get(service_account_id, client)
            .await?
            .ok_or_else(|| anyhow!("Service account not found"))?;
        Ok(service_account)
    }

    pub fn get_context(user: &User) -> Result<Context> {
        let perms = user
            .attributes
            .0
            .permissions
            .iter()
            .next()
            .ok_or_else(|| anyhow!("Expected exactly one permission for service account"))?;
        let (id, _) = perms.pair();
        Ok(Context::res_ctx(*id, DbPermissionLevel::ADMIN, false))
    }
}

impl DeleteServiceAccount {
    pub fn get_id(&self) -> Result<DieselUlid> {
        let service_account = DieselUlid::from_str(&self.0.svc_account_id)?;
        Ok(service_account)
    }

    /// Returns service_account
    pub async fn get_service_account(&self, client: &Client) -> Result<User> {
        let service_account_id = self.get_id()?;
        let service_account = User::get(service_account_id, client)
            .await?
            .ok_or_else(|| anyhow!("Service account not found"))?;
        Ok(service_account)
    }

    pub fn get_context(user: &User) -> Result<Context> {
        let perms = user
            .attributes
            .0
            .permissions
            .iter()
            .next()
            .ok_or_else(|| anyhow!("Expected exactly one permission for service account"))?;
        let (id, _) = perms.pair();
        Ok(Context::res_ctx(*id, DbPermissionLevel::ADMIN, false))
    }
}
