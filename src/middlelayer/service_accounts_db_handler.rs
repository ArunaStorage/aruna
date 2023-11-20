use std::str::FromStr;
use std::sync::Arc;

use super::db_handler::DatabaseHandler;
use super::service_account_request_types::{
    CreateServiceAccountToken, SetServiceAccountPermission,
};
use crate::auth::permission_handler::PermissionHandler;
use crate::database::crud::CrudDb;
use crate::database::dsls::object_dsl::Object;
use crate::database::dsls::user_dsl::{User, UserAttributes};
use crate::database::enums::ObjectMapping;
use crate::middlelayer::service_account_request_types::CreateServiceAccount;
use crate::middlelayer::token_request_types::CreateToken;
use crate::utils::conversions::convert_token_to_proto;
use anyhow::{anyhow, Result};
use aruna_rust_api::api::storage::models::v2::permission::ResourceId;
use aruna_rust_api::api::storage::models::v2::{Permission, Token};
use aruna_rust_api::api::storage::services::v2::CreateApiTokenRequest;
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use postgres_types::Json;

impl DatabaseHandler {
    pub async fn create_service_account(&self, request: CreateServiceAccount) -> Result<User> {
        let user_id = DieselUlid::generate();
        let (res_id, perm) = request.get_permissions()?;
        let client = self.database.get_client().await?;
        let mut user = User {
            id: user_id,
            display_name: format!("SERVICE_ACCOUNT#{}", user_id),
            email: String::new(),
            attributes: Json(UserAttributes {
                global_admin: false,
                service_account: true,
                tokens: DashMap::default(),
                trusted_endpoints: DashMap::default(),
                custom_attributes: vec![],
                external_ids: vec![],
                permissions: DashMap::from_iter([(res_id, perm)]),
            }),
            active: true,
        };
        user.create(&client).await?;
        Ok(user)
    }

    pub async fn create_service_account_token(
        &self,
        authorizer: Arc<PermissionHandler>,
        request: CreateServiceAccountToken,
    ) -> Result<(Option<Token>, String)> {
        let id = <DieselUlid as std::str::FromStr>::from_str(&request.0.svc_account_id)?;
        let client = self.database.get_client().await?;
        let service_account = User::get(id, &client)
            .await?
            .ok_or_else(|| anyhow!("User not found"))?;
        let (id, level) = request.get_permissions()?;
        let perms = service_account
            .attributes
            .0
            .permissions
            .iter()
            .next()
            .ok_or_else(|| anyhow!("No permissions found"))?;
        let (existing_id, existing_level) = perms.pair();
        let permission = if (&id != existing_id) || (&level != existing_level) {
            return Err(anyhow!("Unauthorized"));
        } else {
            Some(Permission {
                permission_level: existing_level.into_inner().into(),
                resource_id: Some(match existing_level {
                    ObjectMapping::PROJECT(_) => ResourceId::ProjectId(existing_id.to_string()),
                    ObjectMapping::COLLECTION(_) => {
                        ResourceId::CollectionId(existing_id.to_string())
                    }
                    ObjectMapping::DATASET(_) => ResourceId::DatasetId(existing_id.to_string()),
                    ObjectMapping::OBJECT(_) => ResourceId::ObjectId(existing_id.to_string()),
                }),
            })
        };
        let expires_at = request.0.expires_at;
        // Create token
        let (token_ulid, token) = self
            .create_token(
                &id,
                authorizer.token_handler.get_current_pubkey_serial() as i32,
                CreateToken(CreateApiTokenRequest {
                    name: request.0.name,
                    permission,
                    expires_at: expires_at.clone(),
                }),
            )
            .await?;

        // Update service account with token
        service_account
            .attributes
            .0
            .tokens
            .insert(token_ulid, token.clone());
        self.cache
            .update_user(&service_account.id, service_account.clone());

        // Sign token
        let token_secret = authorizer.token_handler.sign_user_token(
            &service_account.id,
            &token_ulid,
            expires_at,
        )?;

        Ok((
            Some(convert_token_to_proto(&token_ulid, token)),
            token_secret,
        ))
    }
    pub async fn set_service_account_permission(
        &self,
        request: SetServiceAccountPermission,
    ) -> Result<User> {
        let (id, level) = request.get_permissions()?;
        let service_account_id = DieselUlid::from_str(&request.0.svc_account_id)?;
        let client = self.database.get_client().await?;
        let object = Object::get(id, &client)
            .await?
            .ok_or_else(|| anyhow!("Resource not found"))?;
        let user = self
            .add_permission_to_user(service_account_id, id, &object.name, level, false)
            .await?;
        Ok(user)
    }
}
