use crate::auth::permission_handler::PermissionHandler;
use crate::auth::token_handler::{Action, Intent};
use crate::database::dsls::endpoint_dsl::Endpoint;
use crate::database::dsls::object_dsl::Object;
use crate::database::dsls::workspaces_dsl::WorkspaceTemplate;
use crate::middlelayer::token_request_types::CreateToken;
use crate::middlelayer::workspace_request_types::{CreateTemplate, CreateWorkspace};
use crate::{database::crud::CrudDb, middlelayer::db_handler::DatabaseHandler};
use anyhow::{anyhow, Ok, Result};
use aruna_rust_api::api::dataproxy::services::v2::{GetCredentialsRequest, GetCredentialsResponse};
use aruna_rust_api::api::storage::models::v2::{Permission, PermissionLevel};
use aruna_rust_api::api::storage::services::v2::CreateApiTokenRequest;
use diesel_ulid::DieselUlid;
use std::str::FromStr;
use std::sync::Arc;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue};
use tonic::Request;

impl DatabaseHandler {
    pub async fn create_workspace_template(
        &self,
        request: CreateTemplate,
        owner: DieselUlid,
    ) -> Result<String> {
        let client = self.database.get_client().await?;
        let mut template = request.get_template(owner)?;
        template.create(&client).await?;
        Ok(request.0.name)
    }
    pub async fn create_workspace(
        &self,
        authorizer: Arc<PermissionHandler>,
        request: CreateWorkspace,
        endpoint: String,
    ) -> Result<(DieselUlid, String, String, String)> // (ProjectID, Token, AccessKey, SecretKey)
    {
        let mut client = self.database.get_client().await?;
        let endpoint_id = DieselUlid::from_str(&endpoint)?;
        let endpoint = Endpoint::get(endpoint_id, &client)
            .await?
            .ok_or_else(|| anyhow!("Endpoint not found"))?;
        let template = WorkspaceTemplate::get_by_name(request.get_name(), &client)
            .await?
            .ok_or_else(|| anyhow!("WorkspaceTemplate not found"))?;

        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        let mut workspace = CreateWorkspace::make_project(template, endpoint_id);

        workspace.create(transaction_client).await?;
        // Create service account
        let user = CreateWorkspace::create_service_account(endpoint_id, workspace.id);
        // Create token
        let (token_ulid, token) = self
            .create_token(
                &user.id,
                authorizer.token_handler.get_current_pubkey_serial() as i32,
                CreateToken(CreateApiTokenRequest {
                    name: user.display_name,
                    permission: Some(Permission {
                        permission_level: PermissionLevel::Append as i32,
                        resource_id: Some(aruna_rust_api::api::storage::models::v2::permission::ResourceId::ProjectId(workspace.id.to_string())),
                    }),
                    expires_at: None,
                }),
            )
            .await?;
        // Update service account
        user.attributes.0.tokens.insert(token_ulid, token);
        // Sign token
        let token_secret = authorizer
            .token_handler
            .sign_user_token(&user.id, &token_ulid, None)?;

        // Create creds
        let slt = authorizer.token_handler.sign_dataproxy_slt(
            &user.id,
            Some(token_ulid.to_string()),
            Some(Intent {
                target: endpoint_id,
                action: Action::CreateSecrets,
            }),
        )?;
        let mut credentials_request = Request::new(GetCredentialsRequest {});
        credentials_request.metadata_mut().append(
            AsciiMetadataKey::from_bytes("Authorization".as_bytes())?,
            AsciiMetadataValue::try_from(format!("Bearer {}", slt))?,
        );
        let (
            ..,
            GetCredentialsResponse {
                access_key,
                secret_key,
            },
        ) = DatabaseHandler::get_credentials(authorizer.clone(), user.id, None, endpoint).await?;

        Ok((workspace.id, access_key, secret_key, token_secret))
    }

    pub async fn delete_workspace(&self, workspace_id: DieselUlid) -> Result<()> {
        let client = self.database.get_client().await?;
        let workspace = Object::get(workspace_id, &client)
            .await?
            .ok_or_else(|| anyhow!("Workspace not found"))?;
        workspace.delete(&client).await?;
        Ok(())
    }
}
