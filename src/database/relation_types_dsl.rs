use crate::database::crud::{CrudDb, PrimaryKey};
use anyhow::Result;
use postgres_from_row::FromRow;
use tokio_postgres::Client;

#[derive(FromRow, Debug)]
pub struct RelationTypes {
    pub id: i16,
    pub relation_name: String,
}

#[async_trait::async_trait]
impl CrudDb for RelationTypes {
    async fn create(&self, client: &Client) -> Result<()> {
        let query = "INSERT INTO relation_types (id, relation_name) VALUES ($1, $2);";
        let prepared = client.prepare(query).await?;
        client
            .query(&prepared, &[&self.id, &self.relation_name])
            .await?;
        Ok(())
    }
    async fn get(id: impl PrimaryKey, client: &Client) -> Result<Option<Self>> {
        // This needs to be dynamic for (origin/target)/(did/pid)
        let query = "SELECT * FROM relation_types WHERE id = $1";
        let prepared = client.prepare(query).await?;
        Ok(client
            .query_opt(&prepared, &[&id])
            .await?
            .map(|e| RelationTypes::from_row(&e)))
    }
    async fn all(client: &Client) -> Result<Vec<Self>> {
        let query = "SELECT * FROM relation_types";
        let prepared = client.prepare(query).await?;
        let rows = client.query(&prepared, &[]).await?;
        Ok(rows.iter().map(RelationTypes::from_row).collect::<Vec<_>>())
    }

    async fn delete(&self, id: impl PrimaryKey, client: &Client) -> Result<()> {
        let query = "DELETE FROM relation_types WHERE id = $1";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&id]).await?;
        Ok(())
    }
}
