use crate::database::crud::{CrudDb, PrimaryKey};
use anyhow::Result;
use chrono::NaiveDateTime;
use diesel_ulid::DieselUlid;
use postgres_from_row::FromRow;
use tokio_postgres::Client;

use super::enums::PermissionLevels;

#[derive(FromRow, Debug)]
pub struct APIToken {
    pub id: DieselUlid,
    pub user_id: DieselUlid,
    pub pub_key: DieselUlid,
    pub name: String,
    pub created_at: NaiveDateTime,
    pub used_at: NaiveDateTime,
    pub expires_at: Option<NaiveDateTime>,
    pub object_id: DieselUlid,
    pub user_rights: PermissionLevels,
}

#[async_trait::async_trait]
impl CrudDb for APIToken {
    async fn create(&self, client: &Client) -> Result<()> {
        let query = "INSERT INTO api_tokens (id, user_id, pub_key, name, created_at, used_at, expires_at, object_id, user_rights) VALUES (
            $1, $2, $3, $4, $5, $6, $7
        );";

        let prepared = client.prepare(query).await?;

        client
            .query(
                &prepared,
                &[
                    &self.id,
                    &self.user_id,
                    &self.pub_key,
                    &self.name,
                    &self.expires_at,
                    &self.object_id,
                    &self.user_rights,
                ],
            )
            .await?;
        Ok(())
    }
    async fn get(id: impl PrimaryKey, client: &Client) -> Result<Option<Self>> {
        let query = "SELECT * FROM api_tokens WHERE id = $1";
        let prepared = client.prepare(query).await?;
        Ok(client
            .query_opt(&prepared, &[&id])
            .await?
            .map(|e| APIToken::from_row(&e)))
    }
    async fn all(client: &Client) -> Result<Vec<Self>> {
        let query = "SELECT * FROM api_tokens";
        let prepared = client.prepare(query).await?;
        let rows = client.query(&prepared, &[]).await?;
        Ok(rows.iter().map(APIToken::from_row).collect::<Vec<_>>())
    }

    async fn delete(&self, id: impl PrimaryKey, client: &Client) -> Result<()> {
        let query = "DELETE FROM api_tokens WHERE id = $1";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&id]).await?;
        Ok(())
    }
}
