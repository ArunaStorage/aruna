use anyhow::Result;
use async_nats::jetstream::consumer::Config;
use diesel_ulid::DieselUlid;
use postgres_from_row::FromRow;
use postgres_types::Json;
use tokio_postgres::Client;

use super::crud::{CrudDb, PrimaryKey};

#[derive(FromRow, Debug, PartialEq, Eq)]
pub struct StreamConsumer {
    pub id: DieselUlid,
    pub user_id: Option<DieselUlid>,
    pub config: Json<Config>,
}

#[async_trait::async_trait]
impl CrudDb for StreamConsumer {
    //ToDo: Rust Doc
    async fn create(&self, client: &Client) -> Result<()> {
        let query = "INSERT INTO stream_consumers 
          (id, user_id, config) 
        VALUES 
          ($1, $2, $3);";

        let prepared = client.prepare(query).await?;

        client
            .query(&prepared, &[&self.id, &self.user_id, &self.config])
            .await?;
        Ok(())
    }

    //ToDo: Rust Doc
    async fn get(id: impl PrimaryKey, client: &Client) -> Result<Option<Self>> {
        let query = "SELECT * FROM stream_consumers WHERE id = $1";
        let prepared = client.prepare(query).await?;

        Ok(client
            .query_opt(&prepared, &[&id])
            .await?
            .map(|e| StreamConsumer::from_row(&e)))
    }

    //ToDo: Rust Doc
    async fn all(client: &Client) -> Result<Vec<Self>> {
        let query = "SELECT * FROM stream_consumers";
        let prepared = client.prepare(query).await?;
        let rows = client.query(&prepared, &[]).await?;
        Ok(rows
            .iter()
            .map(StreamConsumer::from_row)
            .collect::<Vec<_>>())
    }

    //ToDo: Rust Doc
    async fn delete(&self, client: &Client) -> Result<()> {
        let query = "DELETE FROM stream_consumers WHERE id = $1";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&self.id]).await?;
        Ok(())
    }
}
