use crate::database::crud::CrudDb;
use crate::database::dsls::user_dsl::{User, UserAttributes};
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::user_request_types::{
    ActivateUser, DeactivateUser, RegisterUser, UpdateUserEmail, UpdateUserName,
};
use anyhow::{anyhow, Result};
use diesel_ulid::DieselUlid;
use postgres_types::Json;
use tokio_postgres::GenericClient;

impl DatabaseHandler {
    pub async fn register_user(
        &self,
        request: RegisterUser,
        external_id: Option<String>,
    ) -> Result<(DieselUlid, User)> {
        let client = self.database.get_client().await?;
        let user_id = DieselUlid::generate();
        let new_attributes = UserAttributes {
            global_admin: false,
            service_account: false,
            tokens: Default::default(),
            trusted_endpoints: Default::default(),
            custom_attributes: vec![],
            permissions: Default::default(),
        };
        let user = User {
            id: user_id,
            display_name: request.get_display_name(),
            external_id,
            email: request.get_email(),
            attributes: Json(new_attributes),
            active: false,
        };
        user.create(client.client()).await?;
        Ok((user_id, user))
    }
    pub async fn deactivate_user(&self, request: DeactivateUser) -> Result<(DieselUlid, User)> {
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let client = transaction.client();
        let id = request.get_id()?;
        User::deactivate_user(client, &id).await?;
        let user = User::get(id, client)
            .await?
            .ok_or_else(|| anyhow!("User not found"))?;
        transaction.commit().await?;
        Ok((id, user))
    }
    pub async fn activate_user(&self, request: ActivateUser) -> Result<(DieselUlid, User)> {
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let client = transaction.client();
        let id = request.get_id()?;
        User::activate_user(client, &id).await?;
        let user = User::get(id, client)
            .await?
            .ok_or_else(|| anyhow!("User not found"))?;
        transaction.commit().await?;
        Ok((id, user))
    }
    pub async fn update_display_name(
        &self,
        request: UpdateUserName,
        user_id: DieselUlid,
    ) -> Result<User> {
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let client = transaction.client();
        let name = request.get_name();
        User::update_display_name(client, &user_id, name).await?;
        let user = User::get(user_id, client)
            .await?
            .ok_or_else(|| anyhow!("User not found"))?;
        transaction.commit().await?;
        Ok(user)
    }
    pub async fn update_email(
        &self,
        request: UpdateUserEmail,
        user_id: DieselUlid,
    ) -> Result<User> {
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let client = transaction.client();
        let email = request.get_email();
        User::update_email(client, &user_id, email).await?;
        let user = User::get(user_id, client)
            .await?
            .ok_or_else(|| anyhow!("User not found"))?;
        transaction.commit().await?;
        Ok(user)
    }
}
