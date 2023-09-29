use anyhow::Result;
use diesel_ulid::DieselUlid;
use postgres_from_row::FromRow;
use tokio_postgres::Client;

use super::super::crud::{CrudDb, PrimaryKey};

#[derive(Debug, FromRow)]
pub struct IdentityProvider {
    pub id: DieselUlid,
    pub name: String,
    pub url: String,
}

#[async_trait::async_trait]
impl CrudDb for IdentityProvider {
    //ToDo: Rust Doc
    async fn create(&mut self, client: &Client) -> Result<()> {
        let query = "INSERT INTO identity_providers 
          (id, name, url) 
        VALUES 
          ($1, $2, $3);";

        let prepared = client.prepare(query).await?;

        client
            .query(&prepared, &[&self.id, &self.name, &self.url])
            .await?;
        Ok(())
    }

    //ToDo: Rust Doc
    async fn get(id: impl PrimaryKey, client: &Client) -> Result<Option<Self>> {
        let query = "SELECT * FROM identity_providers WHERE id = $1";
        let prepared = client.prepare(query).await?;

        Ok(client
            .query_opt(&prepared, &[&id])
            .await?
            .map(|e| IdentityProvider::from_row(&e)))
    }

    //ToDo: Rust Doc
    async fn all(client: &Client) -> Result<Vec<Self>> {
        let query = "SELECT * FROM identity_providers";
        let prepared = client.prepare(query).await?;
        let rows = client.query(&prepared, &[]).await?;
        Ok(rows
            .iter()
            .map(IdentityProvider::from_row)
            .collect::<Vec<_>>())
    }

    //ToDo: Rust Doc
    async fn delete(&self, client: &Client) -> Result<()> {
        let query = "DELETE FROM identity_providers WHERE id = $1";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&self.id]).await?;
        Ok(())
    }
}
