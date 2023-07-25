use crate::database::crud::{CrudDb, PrimaryKey};
use anyhow::Result;
use postgres_from_row::FromRow;
use tokio_postgres::Client;

#[derive(FromRow, Debug)]
pub struct RelationType {
    pub relation_name: String,
}

#[async_trait::async_trait]
impl CrudDb for RelationType {
    async fn create(&self, client: &Client) -> Result<()> {
        let query = "INSERT INTO relation_types (id, relation_name) VALUES ($1, $2);";
        let prepared = client.prepare(query).await?;
        client.query(&prepared, &[&self.relation_name]).await?;
        Ok(())
    }
    async fn get(id: impl PrimaryKey, client: &Client) -> Result<Option<Self>> {
        let query = "SELECT * FROM relation_types WHERE id = $1";
        let prepared = client.prepare(query).await?;
        Ok(client
            .query_opt(&prepared, &[&id])
            .await?
            .map(|e| RelationType::from_row(&e)))
    }
    async fn all(client: &Client) -> Result<Vec<Self>> {
        let query = "SELECT * FROM relation_types";
        let prepared = client.prepare(query).await?;
        let rows = client.query(&prepared, &[]).await?;
        Ok(rows.iter().map(RelationType::from_row).collect::<Vec<_>>())
    }

    async fn delete(&self, client: &Client) -> Result<()> {
        let query = "DELETE FROM relation_types WHERE relation_name = $1";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&self.relation_name]).await?;
        Ok(())
    }
}
impl RelationType {
    pub async fn get_by_name(name: String, client: &Client) -> Result<Option<RelationType>> {
        let query = "SELECT * FROM relation_types WHERE relation_name = $1";
        let prepared = client.prepare(query).await?;
        Ok(client
            .query_opt(&prepared, &[&name])
            .await?
            .map(|e| RelationType::from_row(&e)))
    }
}
