use crate::database::crud::{CrudDb, PrimaryKey};
use anyhow::Result;
use diesel_ulid::DieselUlid;
use postgres_from_row::FromRow;
use postgres_types::{FromSql, Json, ToSql};
use tokio_postgres::Client;

use super::object_dsl::KeyValues;

#[derive(FromSql, ToSql, Debug, FromRow)]
pub struct WorkspaceTemplate {
    pub id: DieselUlid,
    pub name: String,
    pub owner: DieselUlid,
    pub prefix: String,
    pub key_values: Json<KeyValues>,
}

#[async_trait::async_trait]
impl CrudDb for WorkspaceTemplate {
    async fn create(&mut self, client: &Client) -> Result<()> {
        let query = "INSERT INTO workspaces (id, name, owner, prefix, key_values) VALUES (
            $1, $2, $3, $4, $5
        ) RETURNING *;";

        let prepared = client.prepare(query).await?;

        let row = client
            .query_one(
                &prepared,
                &[
                    &self.id,
                    &self.name,
                    &self.owner,
                    &self.prefix,
                    &self.key_values,
                ],
            )
            .await?;

        *self = WorkspaceTemplate::from_row(&row);
        Ok(())
    }
    async fn get(id: impl PrimaryKey, client: &Client) -> Result<Option<Self>> {
        let query = "SELECT * FROM workspaces WHERE id = $1";
        let prepared = client.prepare(query).await?;
        Ok(client
            .query_opt(&prepared, &[&id])
            .await?
            .map(|e| WorkspaceTemplate::from_row(&e)))
    }
    async fn all(client: &Client) -> Result<Vec<Self>> {
        let query = "SELECT * FROM workspaces";
        let prepared = client.prepare(query).await?;
        let rows = client.query(&prepared, &[]).await?;
        Ok(rows
            .iter()
            .map(WorkspaceTemplate::from_row)
            .collect::<Vec<_>>())
    }
    async fn delete(&self, client: &Client) -> Result<()> {
        let query = "DELETE FROM workspaces WHERE id = $1";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&self.id]).await?;
        Ok(())
    }
}

impl WorkspaceTemplate {
    pub async fn get_by_name(name: String, client: &Client) -> Result<Option<Self>> {
        let query = "SELECT * FROM workspaces WHERE name = $1";
        let prepared = client.prepare(query).await?;
        Ok(client
            .query_opt(&prepared, &[&name])
            .await?
            .map(|e| WorkspaceTemplate::from_row(&e)))
    }
}
