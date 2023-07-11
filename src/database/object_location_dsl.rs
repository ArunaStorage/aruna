use crate::database::crud::{CrudDb, PrimaryKey};
use anyhow::Result;
use diesel_ulid::DieselUlid;
use postgres_from_row::FromRow;
use tokio_postgres::Client;

#[derive(FromRow, Debug)]
pub struct ObjectLocation {
    pub id: DieselUlid,
    pub endpoint_id: DieselUlid,
    pub object_id: DieselUlid,
    pub is_primary: bool,
}

#[async_trait::async_trait]
impl CrudDb for ObjectLocation {
    async fn create(&self, client: &Client) -> Result<()> {
        let query = "INSERT INTO object_locations (id, endpoint_id, object_id, is_primary) VALUES (
            $1, $2, $3, $4
        );";

        let prepared = client.prepare(query).await?;

        client
            .query(
                &prepared,
                &[
                    &self.id,
                    &self.endpoint_id,
                    &self.object_id,
                    &self.is_primary,
                ],
            )
            .await?;
        Ok(())
    }
    async fn get(id: impl PrimaryKey, client: &Client) -> Result<Option<Self>> {
        let query = "SELECT * FROM object_locations WHERE id = $1";
        let prepared = client.prepare(query).await?;
        Ok(client
            .query_opt(&prepared, &[&id])
            .await?
            .map(|e| ObjectLocation::from_row(&e)))
    }
    async fn all(client: &Client) -> Result<Vec<Self>> {
        let query = "SELECT * FROM object_locations";
        let prepared = client.prepare(query).await?;
        let rows = client.query(&prepared, &[]).await?;
        Ok(rows
            .iter()
            .map(ObjectLocation::from_row)
            .collect::<Vec<_>>())
    }

    async fn delete(&self, id: impl PrimaryKey, client: &Client) -> Result<()> {
        let query = "DELETE FROM object_locations WHERE id = $1";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&id]).await?;
        Ok(())
    }
}
