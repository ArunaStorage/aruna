use crate::database::crud::CrudDb;
use crate::database::dsls::user_dsl::{APIToken, User};
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::token_request_types::{CreateToken, DeleteToken};
use ahash::{HashMap, HashMapExt};
use anyhow::{anyhow, Result};
use chrono::Utc;
use diesel_ulid::DieselUlid;

impl DatabaseHandler {
    pub async fn create_token(&self, user_id: DieselUlid, request: CreateToken) -> Result<User> {
        let client = self.database.get_client().await?;
        let object_id = request.get_resource()?;
        let user_rights = request.get_rights()?;
        let expires_at = request.get_expiry()?;
        let token_id = DieselUlid::generate();
        let token = APIToken {
            pub_key: 0,
            name: request.0.name,
            created_at: Utc::now().naive_local(),
            expires_at,
            object_id,
            user_rights,
        };
        let mut map = HashMap::new();
        map.insert(token_id, token);
        User::add_user_token(&client, &user_id, map).await?;
        let user = User::get(user_id, &client)
            .await?
            .ok_or_else(|| anyhow!("User not found"))?;
        Ok(user)
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
