use crate::database::enums::PermissionLevels;
use anyhow::Result;
use diesel_ulid::DieselUlid;
use postgres_from_row::FromRow;
use postgres_types::Json;
use serde::{Deserialize, Serialize};
use tokio_postgres::Client;

use super::crud::{CrudDb, PrimaryKey};

#[derive(Debug, FromRow)]
pub struct User {
    pub id: DieselUlid,
    pub display_name: String,
    pub email: String,
    pub attributes: Json<UserAttributes>,
    pub active: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd)]
pub struct UserAttributes {
    pub global_admin: bool,
    pub service_account: bool,
    pub custom_attributes: Vec<CustomAttributes>,
    pub permissions: Vec<Permission>,
}
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd)]
pub struct CustomAttributes {
    pub attribute_name: String,
    pub attribute_value: String,
}
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd)]
pub struct Permission {
    pub resource_id: DieselUlid,
    pub permission_level: PermissionLevels,
}

#[async_trait::async_trait]
impl CrudDb for User {
    //ToDo: Rust Doc
    async fn create(&self, client: &Client) -> Result<()> {
        let query = "INSERT INTO users 
          (id, display_name, email, attributes, active) 
        VALUES 
          ($1, $2, $3, $4, $5);";

        let prepared = client.prepare(query).await?;

        client
            .query(
                &prepared,
                &[
                    &self.id,
                    &self.display_name,
                    &self.email,
                    &self.attributes,
                    &self.active,
                ],
            )
            .await?;
        Ok(())
    }

    //ToDo: Rust Doc
    async fn get(id: impl PrimaryKey, client: &Client) -> Result<Option<Self>> {
        let query = "SELECT * FROM users WHERE id = $1";
        let prepared = client.prepare(query).await?;

        Ok(client
            .query_opt(&prepared, &[&id])
            .await?
            .map(|e| User::from_row(&e)))
    }

    //ToDo: Rust Doc
    async fn all(client: &Client) -> Result<Vec<Self>> {
        let query = "SELECT * FROM users";
        let prepared = client.prepare(query).await?;
        let rows = client.query(&prepared, &[]).await?;
        Ok(rows.iter().map(User::from_row).collect::<Vec<_>>())
    }

    //ToDo: Rust Doc
    async fn delete(&self, id: impl PrimaryKey, client: &Client) -> Result<()> {
        let query = "DELETE FROM users WHERE id = $1";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&id]).await?;
        Ok(())
    }
}
