use crate::{
    auth::structs::Context,
    database::{
        crud::CrudDb,
        dsls::user_dsl::User,
        enums::{DbPermissionLevel, ObjectMapping},
    },
};
use anyhow::{anyhow, Result};
use aruna_rust_api::api::storage::services::v2::{
    AddDataProxyAttributeSvcAccountRequest, AddPubkeySvcAccountRequest,
    AddTrustedEndpointsSvcAccountRequest, CreateS3CredentialsSvcAccountRequest,
    DeleteS3CredentialsSvcAccountRequest, RemoveDataProxyAttributeSvcAccountRequest,
    RemoveDataProxyAttributeUserRequest, RemoveTrustedEndpointsSvcAccountRequest,
};
use aruna_rust_api::api::storage::{
    models::v2::permission::ResourceId,
    services::v2::{
        CreateDataproxyTokenSvcAccountRequest, CreateServiceAccountRequest,
        CreateServiceAccountTokenRequest, DeleteServiceAccountRequest,
        DeleteServiceAccountTokenRequest, DeleteServiceAccountTokensRequest,
        GetS3CredentialsSvcAccountRequest, GetServiceAccountTokenRequest,
        GetServiceAccountTokensRequest,
    },
};
use diesel_ulid::DieselUlid;
use std::str::FromStr;
use tokio_postgres::Client;

/// Wrappers for requests
pub struct CreateServiceAccount(pub CreateServiceAccountRequest);
pub struct CreateServiceAccountToken(pub CreateServiceAccountTokenRequest);
pub struct GetServiceAccountToken(pub GetServiceAccountTokenRequest);
pub struct GetServiceAccountTokens(pub GetServiceAccountTokensRequest);
pub struct DeleteServiceAccountToken(pub DeleteServiceAccountTokenRequest);
pub struct DeleteServiceAccountTokens(pub DeleteServiceAccountTokensRequest);
pub struct DeleteServiceAccount(pub DeleteServiceAccountRequest);
#[derive(Clone)]
pub struct CreateDataProxyTokenSVCAccount(pub CreateDataproxyTokenSvcAccountRequest);
pub struct GetS3CredentialsSVCAccount(pub GetS3CredentialsSvcAccountRequest);
pub struct CreateS3CredsSvcAccount(pub CreateS3CredentialsSvcAccountRequest);
pub struct DeleteS3CredsSvcAccount(pub DeleteS3CredentialsSvcAccountRequest);
pub struct AddPubkeySvcAccount(pub AddPubkeySvcAccountRequest);
pub struct AddTrustedEndpointSvcAccount(pub AddTrustedEndpointsSvcAccountRequest);
pub struct RemoveTrustedEndpointSvcAccount(pub RemoveTrustedEndpointsSvcAccountRequest);
pub struct AddDataproxyAttributeSvcAccount(pub AddDataProxyAttributeSvcAccountRequest);
pub struct RemoveDataproxyAttributeSvcAccount(pub RemoveDataProxyAttributeSvcAccountRequest);

/// Impls for wrappers
impl CreateServiceAccount {
    pub fn get_permissions(&self) -> Result<(DieselUlid, ObjectMapping<DbPermissionLevel>)> {
        let (id, permission) = (
            DieselUlid::from_str(&self.0.project_id)?,
            self.0.permission_level,
        );
        let level: DbPermissionLevel = permission.try_into()?;

        Ok((id, ObjectMapping::PROJECT(level)))
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

pub(crate) trait GetTokenAndServiceAccountInfo {
    fn get_unparsed_ids(&self) -> (String, String);
    fn get_ids(&self) -> Result<(DieselUlid, DieselUlid)> {
        let (service_account, token) = self.get_unparsed_ids();
        let service_account = DieselUlid::from_str(&service_account)?;
        let token_id = DieselUlid::from_str(&token)?;
        Ok((service_account, token_id))
    }
    async fn get_service_account(&self, client: &Client) -> Result<User> {
        let (service_account_id, _) = self.get_ids()?;
        let service_account = User::get(service_account_id, client)
            .await?
            .ok_or_else(|| anyhow!("Service account not found"))?;
        Ok(service_account)
    }
    fn get_context(user: &User) -> Result<Context> {
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

impl GetTokenAndServiceAccountInfo for GetServiceAccountToken {
    fn get_unparsed_ids(&self) -> (String, String) {
        (self.0.svc_account_id.clone(), self.0.token_id.clone())
    }
}
impl GetTokenAndServiceAccountInfo for DeleteServiceAccountToken {
    fn get_unparsed_ids(&self) -> (String, String) {
        (self.0.svc_account_id.clone(), self.0.token_id.clone())
    }
}

pub(crate) trait GetServiceAccountInfo {
    fn get_unparsed_id(&self) -> String;

    fn get_id(&self) -> Result<DieselUlid> {
        let service_account = DieselUlid::from_str(&self.get_unparsed_id())?;
        Ok(service_account)
    }

    /// Returns service_account
    async fn get_service_account(&self, client: &Client) -> Result<User> {
        let service_account_id = self.get_id()?;
        let service_account = User::get(service_account_id, client)
            .await?
            .ok_or_else(|| anyhow!("Service account not found"))?;
        Ok(service_account)
    }
    fn get_context(user: &User) -> Result<Context> {
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

impl GetServiceAccountInfo for GetServiceAccountTokens {
    fn get_unparsed_id(&self) -> String {
        self.0.svc_account_id.clone()
    }
}

impl GetServiceAccountInfo for DeleteServiceAccountTokens {
    fn get_unparsed_id(&self) -> String {
        self.0.svc_account_id.clone()
    }
}

impl GetServiceAccountInfo for DeleteServiceAccount {
    fn get_unparsed_id(&self) -> String {
        self.0.svc_account_id.clone()
    }
}
impl GetServiceAccountInfo for AddPubkeySvcAccount {
    fn get_unparsed_id(&self) -> String {
        self.0.svc_account_id.clone()
    }
}
impl GetServiceAccountInfo for AddTrustedEndpointSvcAccount {
    fn get_unparsed_id(&self) -> String {
        self.0.svc_account_id.clone()
    }
}
impl GetServiceAccountInfo for RemoveTrustedEndpointSvcAccount {
    fn get_unparsed_id(&self) -> String {
        self.0.svc_account_id.clone()
    }
}
impl GetServiceAccountInfo for RemoveDataproxyAttributeSvcAccount {
    fn get_unparsed_id(&self) -> String {
        self.0.svc_account_id.clone()
    }
}

impl RemoveDataproxyAttributeSvcAccount {
    pub fn get_user_request(self) -> RemoveDataProxyAttributeUserRequest {
        RemoveDataProxyAttributeUserRequest {
            user_id: self.0.svc_account_id,
            dataproxy_id: self.0.dataproxy_id,
            attribute_name: self.0.attribute_name,
        }
    }
}

// Helper trait to get infos needed for proxy interaction
pub(crate) trait GetEndpointInteractionInfos {
    // Return request strings
    fn get_unparsed_ids(&self) -> (String, String);
    // Return service account ulid and endpoint ulid
    fn get_ids(&self) -> Result<(DieselUlid, DieselUlid)> {
        let (service_account, endpoint) = self.get_unparsed_ids();
        let endpoint_id = DieselUlid::from_str(&endpoint)?;
        let service_account = DieselUlid::from_str(&service_account)?;
        Ok((service_account, endpoint_id))
    }
    /// Returns service_account
    async fn get_service_account(service_account_id: &DieselUlid, client: &Client) -> Result<User> {
        let service_account = User::get(*service_account_id, client)
            .await?
            .ok_or_else(|| anyhow!("Service account not found"))?;
        Ok(service_account)
    }
    // Returns service_account permissions
    fn get_permissions(user: &User) -> Result<(DieselUlid, ObjectMapping<DbPermissionLevel>)> {
        let perms = user
            .attributes
            .0
            .permissions
            .iter()
            .next()
            .ok_or_else(|| anyhow!("Expected exactly one permission for service account"))?;
        let (id, map) = perms.pair();
        Ok((*id, *map))
    }
}

impl GetEndpointInteractionInfos for CreateDataProxyTokenSVCAccount {
    fn get_unparsed_ids(&self) -> (String, String) {
        (self.0.svc_account_id.clone(), self.0.endpoint_id.clone())
    }
}
impl GetEndpointInteractionInfos for GetS3CredentialsSVCAccount {
    fn get_unparsed_ids(&self) -> (String, String) {
        (self.0.svc_account_id.clone(), self.0.endpoint_id.clone())
    }
}
impl GetEndpointInteractionInfos for CreateS3CredsSvcAccount {
    fn get_unparsed_ids(&self) -> (String, String) {
        (self.0.svc_account_id.clone(), self.0.endpoint_id.clone())
    }
}
impl GetEndpointInteractionInfos for DeleteS3CredsSvcAccount {
    fn get_unparsed_ids(&self) -> (String, String) {
        (self.0.svc_account_id.clone(), self.0.endpoint_id.clone())
    }
}
