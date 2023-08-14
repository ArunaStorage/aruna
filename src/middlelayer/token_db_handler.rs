use std::collections::HashMap;

use crate::database::dsls::user_dsl::User;
use crate::database::{crud::CrudDb, dsls::user_dsl::APIToken};
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::token_request_types::{CreateToken, DeleteToken};
use anyhow::{anyhow, Result};
use diesel_ulid::DieselUlid;

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

        // Generate APIToken and add to user
        let token_ulid = DieselUlid::generate();
        let token = request.build_token(pubkey_serial)?;

        // Add token to user attributes
        User::add_user_token(client, user_id, HashMap::from([(token_ulid, &token)])).await?;

        // Return token_id and token
        Ok((token_ulid, token))
    }

    pub async fn delete_token(&self, user_id: DieselUlid, request: DeleteToken) -> Result<User> {
        let client = self.database.get_client().await?;
        let token_id = request.get_token_id()?;
        User::remove_user_token(&client, &user_id, &token_id).await?;
        let user = User::get(user_id, &client)
            .await?
            .ok_or_else(|| anyhow!("User not found"))?;
        Ok(user)
    }
    pub async fn delete_all_tokens(&self, user_id: DieselUlid) -> Result<User> {
        let client = self.database.get_client().await?;
        User::remove_all_tokens(&client, &user_id).await?;
        let user = User::get(user_id, &client)
            .await?
            .ok_or_else(|| anyhow!("User not found"))?;
        Ok(user)
    }
}
