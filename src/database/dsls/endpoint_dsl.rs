use crate::database::crud::{CrudDb, PrimaryKey};
use crate::database::enums::{DataProxyFeature, EndpointStatus, EndpointVariant};
use anyhow::Result;
use diesel_ulid::DieselUlid;
use itertools::Itertools;
use postgres_from_row::FromRow;
use postgres_types::Json;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use tokio_postgres::Client;

#[derive(FromRow, Debug, Clone)]
pub struct Endpoint {
    pub id: DieselUlid,
    pub name: String,
    pub host_config: Json<HostConfigs>,
    pub endpoint_variant: EndpointVariant,
    pub documentation_object: DieselUlid,
    pub is_public: bool,
    pub is_default: bool,
    pub status: EndpointStatus,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, PartialOrd, Hash, Eq)]
pub struct HostConfigs(pub Vec<HostConfig>);

#[derive(Serialize, Deserialize, FromRow, Debug, Clone, PartialEq, PartialOrd, Hash, Eq)]
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
        let query = "INSERT INTO endpoints (id, name, host_config, endpoint_variant, documentation_object, is_public, is_default, status) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8
        );";

        let prepared = client.prepare(query).await?;

        client
            .query(
                &prepared,
                &[
                    &self.id,
                    &self.name,
                    &self.host_config,
                    &self.endpoint_variant,
                    &self.documentation_object,
                    &self.is_public,
                    &self.is_default,
                    &self.status,
                ],
            )
            .await?;
        Ok(())
    }
    async fn get(id: impl PrimaryKey, client: &Client) -> Result<Option<Self>> {
        let query = "SELECT * FROM endpoints WHERE id = $1;";
        let prepared = client.prepare(query).await?;
        Ok(client
            .query_opt(&prepared, &[&id])
            .await?
            .map(|e| Endpoint::from_row(&e)))
    }
    async fn all(client: &Client) -> Result<Vec<Self>> {
        let query = "SELECT * FROM endpoints ;";
        let prepared = client.prepare(query).await?;
        let rows = client.query(&prepared, &[]).await?;
        Ok(rows.iter().map(Endpoint::from_row).collect::<Vec<_>>())
    }

    async fn delete(&self, client: &Client) -> Result<()> {
        let query = "DELETE FROM endpoints WHERE id = $1;";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&self.id]).await?;
        Ok(())
    }
}

impl Endpoint {
    pub async fn get_by_name(name: String, client: &Client) -> Result<Option<Self>> {
        let query = "SELECT * FROM endpoints WHERE name = $1;";
        let prepared = client.prepare(query).await?;
        Ok(client
            .query_opt(&prepared, &[&name])
            .await?
            .map(|e| Endpoint::from_row(&e)))
    }

    pub async fn delete_by_id(id: &DieselUlid, client: &Client) -> Result<()> {
        let query = "DELETE FROM endpoints WHERE id = $1;";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&id]).await?;
        Ok(())
    }
    pub async fn get_default(client: &Client) -> Result<Option<Endpoint>> {
        let query = "SELECT * FROM endpoints WHERE is_default = true;";
        let prepared = client.prepare(query).await?;
        Ok(client
            .query_opt(&prepared, &[])
            .await?
            .map(|e| Endpoint::from_row(&e)))
    }
}
impl Eq for Endpoint {}
impl PartialEq for Endpoint {
    fn eq(&self, other: &Self) -> bool {
        let self_config: HashSet<_> = self.host_config.0 .0.iter().cloned().collect();
        let other_config: HashSet<_> = other.host_config.0 .0.iter().cloned().collect();
        self.id == other.id
            && self.name == other.name
            && self.is_default == other.is_default
            && self.is_public == other.is_public
            && self.endpoint_variant == other.endpoint_variant
            && self.documentation_object == other.documentation_object
            && self.status == other.status
            && self_config.iter().all(|c| other_config.iter().contains(c))
    }
}
