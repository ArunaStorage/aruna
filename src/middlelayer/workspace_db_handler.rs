use crate::database::dsls::endpoint_dsl::Endpoint;
use crate::database::dsls::user_dsl::{APIToken, User, UserAttributes};
use crate::database::dsls::workspaces_dsl::WorkspaceTemplate;
use crate::database::dsls::Empty;
use crate::database::enums::DbPermissionLevel;
use crate::middlelayer::workspace_request_types::{CreateTemplate, CreateWorkspace};
use crate::{database::crud::CrudDb, middlelayer::db_handler::DatabaseHandler};
use anyhow::{anyhow, Result};
use diesel_ulid::DieselUlid;
use postgres_types::Json;
use std::str::FromStr;

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
        request: CreateWorkspace,
        endpoint: String,
    ) -> Result<(DieselUlid, String, String, String)> // (ProjectID, Token, AccessKey, SecretKey)
    {
        let mut client = self.database.get_client().await?;
        let endpoint_id = DieselUlid::from_str(&endpoint)?;
        let template = WorkspaceTemplate::get_by_name(request.get_name(), &client)
            .await?
            .ok_or_else(|| anyhow!("WorkspaceTemplate not found"))?;

        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        let mut workspace = CreateWorkspace::make_project(template, endpoint_id);

        workspace.create(transaction_client).await?;
        // TODO:
        // - Create service account
        let mut user = CreateWorkspace::create_service_account(endpoint_id);
        // - Create token
        let token_id = todo!();
        let token: APIToken = todo!();
        user.attributes.0.tokens.insert(token_id, token);
        // - Create creds

        todo!()
    }
}

