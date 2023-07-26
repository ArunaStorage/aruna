use crate::database::crud::{CrudDb, PrimaryKey};
use anyhow::Result;
//use aruna_rust_api::api::storage::models::v1::EndpointStatus;
use crate::database::enums::{DataProxyFeature, EndpointStatus};
use diesel_ulid::DieselUlid;
use postgres_from_row::FromRow;
use postgres_types::Json;
use serde::{Deserialize, Serialize};
use tokio_postgres::Client;

#[derive(FromRow, Debug)]
pub struct Endpoint {
    pub id: DieselUlid,
    pub name: String,
    pub host_config: Json<HostConfigs>,
    pub documentation_object: DieselUlid,
    pub is_public: bool,
    pub status: EndpointStatus,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, PartialOrd)]
pub struct HostConfigs(pub Vec<HostConfig>);

#[derive(Serialize, Deserialize, FromRow, Debug, Clone, PartialEq, PartialOrd)]
pub struct HostConfig {
    pub url: String,
    pub is_primary: bool,
    pub ssl: bool,
    pub public: bool,
    pub feature: DataProxyFeature,
}

#[async_trait::async_trait]
impl CrudDb for Endpoint {
    async fn create(&self, client: &Client) -> Result<()> {
        let query = "INSERT INTO endpoints (id, name, host_config, documentation_object, is_public, status) VALUES (
            $1, $2, $3, $4, $5, $6
        );";

        let prepared = client.prepare(query).await?;

        client.query(&prepared, &[&self.id]).await?;
        Ok(())
    }
    async fn get(id: impl PrimaryKey, client: &Client) -> Result<Option<Self>> {
        let query = "SELECT * FROM endpoints WHERE id = $1";
        let prepared = client.prepare(query).await?;
        Ok(client
            .query_opt(&prepared, &[&id])
            .await?
            .map(|e| Endpoint::from_row(&e)))
    }
    async fn all(client: &Client) -> Result<Vec<Self>> {
        let query = "SELECT * FROM endpoints ";
        let prepared = client.prepare(query).await?;
        let rows = client.query(&prepared, &[]).await?;
        Ok(rows.iter().map(Endpoint::from_row).collect::<Vec<_>>())
    }

    async fn delete(&self, client: &Client) -> Result<()> {
        let query = "DELETE FROM endpoints WHERE id = $1";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&self.id]).await?;
        Ok(())
    }
}
