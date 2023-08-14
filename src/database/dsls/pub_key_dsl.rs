use crate::database::crud::{CrudDb, PrimaryKey};
use anyhow::Result;
use diesel_ulid::DieselUlid;
use postgres_from_row::FromRow;
use tokio_postgres::Client;

#[derive(FromRow, Debug)]
pub struct PubKey {
    pub id: i16,
    pub proxy: Option<DieselUlid>,
    pub pubkey: String,
}

#[async_trait::async_trait]
impl CrudDb for PubKey {
    async fn create(&self, client: &Client) -> Result<()> {
        let query =
            "INSERT INTO pub_keys (id, proxy, pubkey) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING;";
        let prepared = client.prepare(query).await?;
        client
            .query(&prepared, &[&self.id, &self.proxy, &self.pubkey])
            .await?;
        Ok(())
    }
    async fn get(id: impl PrimaryKey, client: &Client) -> Result<Option<Self>> {
        // This needs to be dynamic for (origin/target)/(did/pid)
        let query = "SELECT * FROM pub_keys WHERE id = $1";
        let prepared = client.prepare(query).await?;
        Ok(client
            .query_opt(&prepared, &[&id])
            .await?
            .map(|e| PubKey::from_row(&e)))
    }
    async fn all(client: &Client) -> Result<Vec<Self>> {
        let query = "SELECT * FROM pub_keys";
        let prepared = client.prepare(query).await?;
        let rows = client.query(&prepared, &[]).await?;
        Ok(rows.iter().map(PubKey::from_row).collect::<Vec<_>>())
    }

    async fn delete(&self, client: &Client) -> Result<()> {
        let query = "DELETE FROM pub_keys WHERE id = $1";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&self.id]).await?;
        Ok(())
    }
}

impl PubKey {
    pub async fn get_max_id(client: &Client) -> Result<i16> {
        let query = "SELECT MAX(id) FROM pub_keys;";
        let prepared = client.prepare(query).await?;
        let res = client.query_opt(&prepared, &[]).await?.map(|r| r.get(0));
        Ok(res.unwrap_or(0))
    }
}

impl Eq for PubKey {}
impl PartialEq for PubKey {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.pubkey == other.pubkey && self.proxy == other.proxy
    }
}
