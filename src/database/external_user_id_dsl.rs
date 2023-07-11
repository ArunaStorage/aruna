use anyhow::Result;
use diesel_ulid::DieselUlid;
use postgres_from_row::FromRow;
use tokio_postgres::Client;

use super::crud::{CrudDb, PrimaryKey};

#[derive(Debug, FromRow)]
pub struct ExternalUserId {
    pub id: DieselUlid,
    pub external_id: String,
    pub user_id: DieselUlid,
    pub idp_id: DieselUlid,
}

#[async_trait::async_trait]
impl CrudDb for ExternalUserId {
    //ToDo: Rust Doc
    async fn create(&self, client: &Client) -> Result<()> {
        let query = "INSERT INTO external_user_ids 
        (id, external_id, user_id, idp_id) 
      VALUES 
        ($1, $2, $3, $4);";

        let prepared = client.prepare(query).await?;

        client
            .query(
                &prepared,
                &[&self.id, &self.external_id, &self.user_id, &self.idp_id],
            )
            .await?;
        Ok(())
    }

    //ToDo: Rust Doc
    async fn get(id: impl PrimaryKey, client: &Client) -> Result<Option<Self>> {
        let query = "SELECT * FROM external_user_ids WHERE id = $1";
        let prepared = client.prepare(query).await?;

        Ok(client
            .query_opt(&prepared, &[&id])
            .await?
            .map(|e| ExternalUserId::from_row(&e)))
    }

    //ToDo: Rust Doc
    async fn all(client: &Client) -> Result<Vec<Self>> {
        let query = "SELECT * FROM external_user_ids";
        let prepared = client.prepare(query).await?;
        let rows = client.query(&prepared, &[]).await?;
        Ok(rows
            .iter()
            .map(ExternalUserId::from_row)
            .collect::<Vec<_>>())
    }

    //ToDo: Rust Doc
    async fn delete(&self, id: impl PrimaryKey, client: &Client) -> Result<()> {
        let query = "DELETE FROM external_user_ids WHERE id = $1";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&id]).await?;
        Ok(())
    }
}
