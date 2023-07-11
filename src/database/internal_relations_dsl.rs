use crate::database::crud::{CrudDb, PrimaryKey};
use anyhow::Result;
use diesel_ulid::DieselUlid;
use postgres_from_row::FromRow;
//use postgres_types::ToSql;
use tokio_postgres::Client;

#[derive(FromRow, Debug)]
pub struct InternalRelations {
    pub id: DieselUlid,
    pub origin_pid: DieselUlid,
    pub origin_did: DieselUlid,
    pub type_id: i32,
    pub target_pid: DieselUlid,
    pub target_did: DieselUlid,
}

#[async_trait::async_trait]
impl CrudDb for InternalRelations {
    async fn create(&self, client: &Client) -> Result<()> {
        let query = "INSERT INTO internal_relations (id, origin_pid, origin_did, type_id, target_pid, target_did) VALUES (
            $1, $2, $3, $4, $5, $6
        );";

        let prepared = client.prepare(query).await?;

        client
            .query(
                &prepared,
                &[
                    &self.id,
                    &self.origin_pid,
                    &self.origin_did,
                    &self.type_id,
                    &self.target_pid,
                    &self.target_did,
                ],
            )
            .await?;
        Ok(())
    }
    async fn get(id: impl PrimaryKey, client: &Client) -> Result<Option<Self>> {
        // This needs to be dynamic for (origin/target)/(did/pid)
        let query = "SELECT * FROM internal_relations WHERE id = $1";
        let prepared = client.prepare(query).await?;
        Ok(client
            .query_opt(&prepared, &[&id])
            .await?
            .map(|e| InternalRelations::from_row(&e)))
    }
    async fn all(client: &Client) -> Result<Vec<Self>> {
        let query = "SELECT * FROM internal_relations";
        let prepared = client.prepare(query).await?;
        let rows = client.query(&prepared, &[]).await?;
        Ok(rows
            .iter()
            .map(InternalRelations::from_row)
            .collect::<Vec<_>>())
    }

    async fn delete(&self, id: impl PrimaryKey, client: &Client) -> Result<()> {
        let query = "DELETE FROM internal_relations WHERE id = $1";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&id]).await?;
        Ok(())
    }
}
