use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use super::db_handler::DatabaseHandler;
use super::endpoints_request_types::GetEP;
use super::service_account_request_types::{
    CreateServiceAccountToken, DeleteServiceAccount, DeleteServiceAccountToken,
    DeleteServiceAccountTokens, GetS3CredentialsSVCAccount, SetServiceAccountPermission,
};
use crate::auth::permission_handler::PermissionHandler;
use crate::auth::token_handler::{Action, Intent};
use crate::database::crud::CrudDb;
use crate::database::dsls::object_dsl::Object;
use crate::database::dsls::user_dsl::{User, UserAttributes};
use crate::database::enums::{DataProxyFeature, ObjectMapping, ObjectType};
use crate::middlelayer::service_account_request_types::CreateServiceAccount;
use crate::middlelayer::token_request_types::CreateToken;
use crate::utils::conversions::convert_token_to_proto;
use anyhow::{anyhow, Result};
use aruna_rust_api::api::dataproxy::services::v2::dataproxy_user_service_client::DataproxyUserServiceClient;
use aruna_rust_api::api::dataproxy::services::v2::GetCredentialsRequest;
use aruna_rust_api::api::notification::services::v2::EventVariant;
use aruna_rust_api::api::storage::models::v2::permission::ResourceId;
use aruna_rust_api::api::storage::models::v2::{Permission, Token};
use aruna_rust_api::api::storage::services::v2::get_endpoint_request::Endpoint;
use aruna_rust_api::api::storage::services::v2::{
    CreateApiTokenRequest, GetEndpointRequest, GetS3CredentialsSvcAccountResponse,
};
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use postgres_types::Json;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue};
use tonic::transport::{Channel, ClientTlsConfig};

impl DatabaseHandler {
    pub async fn create_service_account(&self, request: CreateServiceAccount) -> Result<User> {
        let user_id = DieselUlid::generate();
        let (res_id, perm) = request.get_permissions()?;
        let client = self.database.get_client().await?;
        let res = Object::get(res_id, &client)
            .await?
            .ok_or_else(|| anyhow!("Project not found"))?;
        match res.object_type {
            ObjectType::PROJECT => (),
            _ => return Err(anyhow!("Service accounts must have project permissions")),
        }
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
        self.cache.update_user(&user_id, user.clone());
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
            .ok_or_else(|| anyhow!("Error retrieving service_account permission"))?;
        let (project_id, _) = perms.pair();
        if &id != project_id {
            let sub_resources = Object::fetch_subresources_by_id(project_id, &client).await?;
            if !sub_resources.iter().any(|sub_id| &id == sub_id) {
                return Err(anyhow!("Specified permission id is not a sub resource of associated service_account project"));
            }
        }
        let permission = Some(Permission {
            permission_level: level.into_inner().into(),
            resource_id: Some(match level {
                ObjectMapping::PROJECT(_) => ResourceId::ProjectId(id.to_string()),
                ObjectMapping::COLLECTION(_) => ResourceId::CollectionId(id.to_string()),
                ObjectMapping::DATASET(_) => ResourceId::DatasetId(id.to_string()),
                ObjectMapping::OBJECT(_) => ResourceId::ObjectId(id.to_string()),
            }),
        });
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

        // Check if resource for new permission exists and is a Project
        let mut client = self.database.get_client().await?;
        let object = Object::get(id, &client)
            .await?
            .ok_or_else(|| anyhow!("Resource not found"))?;
        match object.object_type {
            ObjectType::PROJECT => (),
            _ => return Err(anyhow!("Resource is not a project")),
        }

        // Start transaction for service account permission update
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();

        // Update service account in transaction to avoid broken state if something fails
        User::remove_all_tokens(transaction_client, &service_account_id).await?;
        User::remove_all_user_permissions(transaction_client, &service_account_id).await?;

        let svc_account = User::add_user_permission(
            transaction_client,
            &service_account_id,
            HashMap::from_iter([(object.id, level)]),
        )
        .await?;

        // Commit transaction and update cache
        transaction.commit().await?;
        self.cache.update_user(&svc_account.id, svc_account.clone());

        // Try to emit user updated notification(s)
        if let Err(err) = self
            .natsio_handler
            .register_user_event(&svc_account, EventVariant::Updated)
            .await
        {
            // Log error (rollback transaction and return)
            log::error!("{}", err);
            //transaction.rollback().await?;
            return Err(anyhow::anyhow!("Notification emission failed"));
        }

        // Return updated service account
        Ok(svc_account)
    }

    pub async fn delete_service_account_token(
        &self,
        request: DeleteServiceAccountToken,
    ) -> Result<()> {
        let (service_account_id, token_id) = request.get_ids()?;
        let client = self.database.get_client().await?;
        let service_account =
            User::remove_user_token(&client, &service_account_id, &token_id).await?;
        self.cache.update_user(&service_account_id, service_account);
        Ok(())
    }

    pub async fn delete_service_account_tokens(
        &self,
        request: DeleteServiceAccountTokens,
    ) -> Result<()> {
        let service_account_id = request.get_id()?;
        let client = self.database.get_client().await?;
        let service_account = User::remove_all_tokens(&client, &service_account_id).await?;
        self.cache.update_user(&service_account_id, service_account);
        Ok(())
    }

    pub async fn delete_service_account(&self, request: DeleteServiceAccount) -> Result<()> {
        let service_account_id = request.get_id()?;
        let client = self.database.get_client().await?;
        let service_account = User::get(service_account_id, &client)
            .await?
            .ok_or_else(|| anyhow!("User not found"))?;
        service_account.delete(&client).await?;
        self.cache.remove_user(&service_account_id);
        Ok(())
    }

    pub async fn get_credentials_svc_account(
        &self,
        authorizer: Arc<PermissionHandler>,
        request: GetS3CredentialsSVCAccount,
    ) -> Result<GetS3CredentialsSvcAccountResponse> {
        let (service_account_id, endpoint_id) = request.get_ids()?;

        // CreateSecret Token for dataproxy
        let dataproxy_token = authorizer.token_handler.sign_dataproxy_slt(
            &service_account_id,
            None, // Token_Id of user token; None if OIDC
            Some(Intent {
                target: endpoint_id,
                action: Action::CreateSecrets,
            }),
        )?;
        // Add endpoint to svc account
        self.add_endpoint_to_user(service_account_id, endpoint_id)
            .await?;

        // Request S3 credentials from Dataproxy
        let endpoint = self
            .get_endpoint(GetEP(GetEndpointRequest {
                endpoint: Some(Endpoint::EndpointId(endpoint_id.to_string())),
            }))
            .await?;
        let mut endpoint_host_url: String = String::new();
        let mut endpoint_s3_url: String = String::new();
        for endpoint_config in endpoint.host_config.0 .0 {
            match endpoint_config.feature {
                DataProxyFeature::GRPC => endpoint_host_url = endpoint_config.url,
                DataProxyFeature::S3 => endpoint_s3_url = endpoint_config.url,
            }
            if !endpoint_s3_url.is_empty() && !endpoint_host_url.is_empty() {
                break;
            }
        }

        // Check if dataproxy host url is tls
        let dp_endpoint = if endpoint_host_url.starts_with("https") {
            Channel::from_shared(endpoint_host_url)?.tls_config(ClientTlsConfig::new())?
        } else {
            Channel::from_shared(endpoint_host_url)?
        };

        // Request credentials from proxy
        let mut dp_conn = DataproxyUserServiceClient::connect(dp_endpoint).await?;
        let mut credentials_request = tonic::Request::new(GetCredentialsRequest {});
        credentials_request.metadata_mut().append(
            AsciiMetadataKey::from_bytes("Authorization".as_bytes())?,
            AsciiMetadataValue::try_from(format!("Bearer {}", dataproxy_token))?,
        );

        let response = dp_conn
            .get_credentials(credentials_request)
            .await?
            .into_inner();
        Ok(GetS3CredentialsSvcAccountResponse {
            s3_access_key: response.access_key,
            s3_secret_key: response.secret_key,
            s3_endpoint_url: endpoint_s3_url,
        })
    }
}
