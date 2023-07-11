use crate::database::crud::{CrudDb, PrimaryKey};
use anyhow::Result;
use diesel_ulid::DieselUlid;
use postgres_from_row::FromRow; //use postgres_types::ToSql;
use tokio_postgres::Client;

#[derive(FromRow, Debug)]
pub struct InternalRelation {
    pub id: DieselUlid,
    pub origin_pid: DieselUlid,
    pub type_id: i32,
    pub target_pid: DieselUlid,
    pub is_persistent: bool,
}

#[async_trait::async_trait]
impl CrudDb for InternalRelation {
    async fn create(&self, client: &Client) -> Result<()> {
        let query = "INSERT INTO internal_relations (id, origin_pid, origin_did, type_id, target_pid, target_did, is_persistent) VALUES (
            $1, $2, $3, $4, $5, $6, $7
        );";

        let prepared = client.prepare(query).await?;

        client
            .query(
                &prepared,
                &[
                    &self.id,
                    &self.origin_pid,
                    &self.type_id,
                    &self.target_pid,
                    &self.is_persistent,
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
            .map(|e| InternalRelation::from_row(&e)))
    }
    async fn all(client: &Client) -> Result<Vec<Self>> {
        let query = "SELECT * FROM internal_relations";
        let prepared = client.prepare(query).await?;
        let rows = client.query(&prepared, &[]).await?;
        Ok(rows
            .iter()
            .map(InternalRelation::from_row)
            .collect::<Vec<_>>())
    }

    async fn delete(&self, id: impl PrimaryKey, client: &Client) -> Result<()> {
        let query = "DELETE FROM internal_relations WHERE id = $1";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&id]).await?;
        Ok(())
    }
}

impl InternalRelation {
    /// Updates relations to latest origin_pid
    pub async fn update_to(old: DieselUlid, new: DieselUlid, client: &Client) -> Result<()> {
        let query = "UPDATE internal_relations 
            SET origin_pid = $2 
            WHERE origin_pid = $1;";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&old, &new]).await?;
        Ok(())
    }
    /// Updates relations to latest target_pid
    pub async fn update_from(old: DieselUlid, new: DieselUlid, client: &Client) -> Result<()> {
        let query = "UPDATE internal_relations 
            SET target_pid = $2 
            WHERE target_pid = $1;";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&old, &new]).await?;
        Ok(())
    }
}
