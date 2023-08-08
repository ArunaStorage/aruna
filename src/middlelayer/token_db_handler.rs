use crate::database::crud::CrudDb;
use crate::database::dsls::user_dsl::User;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::token_request_types::DeleteToken;
use anyhow::{anyhow, Result};
use diesel_ulid::DieselUlid;

impl DatabaseHandler {
    pub async fn create_token(&self) {
        todo!()
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
