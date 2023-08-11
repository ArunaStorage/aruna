use std::collections::HashMap;

use crate::database::dsls::user_dsl::User;
use crate::database::{crud::CrudDb, dsls::user_dsl::APIToken};
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::token_request_types::DeleteToken;
use anyhow::{anyhow, Result};
use diesel_ulid::DieselUlid;

use super::token_request_types::CreateToken;

impl DatabaseHandler {
    pub async fn create_token(
        &self,
        user_id: &DieselUlid,
        pubkey_serial: i32,
        request: CreateToken,
    ) -> Result<(DieselUlid, APIToken)> {
        // Init database transaction
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let client = transaction.client();

        // Extract request parameter
        let name = request.0.name;
        let expiry = request.0.expires_at;
        let permission = request.0.permission;

        // Generate APIToken and add to user
        let token_ulid = DieselUlid::generate();
        let token = request.build_token(pubkey_serial, name, expiry, permission)?;

        User::add_user_token(client, user_id, HashMap::from([(token_ulid, token)])).await?;

        // Return token_id and token
        Ok((token_ulid, token))
    }

    pub async fn delete_token(&self, user_id: DieselUlid, request: DeleteToken) -> Result<User> {
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let client = transaction.client();
        let token_id = request.get_token_id()?;
        User::remove_user_token(client, &user_id, &token_id).await?;
        let user = User::get(user_id, client)
            .await?
            .ok_or_else(|| anyhow!("User not found"))?;
        transaction.commit().await?;
        Ok(user)
    }
}
