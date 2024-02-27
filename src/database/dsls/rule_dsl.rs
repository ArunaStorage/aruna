use crate::database::crud::{CrudDb, PrimaryKey};
use anyhow::Result;
use diesel_ulid::DieselUlid;
use postgres_from_row::FromRow;
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use tokio_postgres::Client;

#[derive(FromRow, FromSql, Debug, Clone, ToSql, Serialize, Deserialize)]
pub struct RuleBinding {
    pub rule_id: DieselUlid,
    pub origin_id: DieselUlid,
    pub object_id: DieselUlid,
    pub cascading: bool,
}

#[derive(FromRow, FromSql, Debug, Clone, ToSql)]
pub struct Rule {
    pub rule_id: DieselUlid,
    pub rule_expressions: String,
    pub description: String,
    pub owner_id: DieselUlid,
    pub is_public: bool,
}

#[async_trait::async_trait]
impl CrudDb for Rule {
    async fn create(&mut self, client: &Client) -> Result<()> {
        todo!()
    }
    async fn get(id: impl PrimaryKey, client: &Client) -> Result<Option<Self>> {
        todo!()
    }
    async fn all(client: &Client) -> Result<Vec<Self>> {
        todo!()
    }
    async fn delete(&self, client: &Client) -> Result<()> {
        todo!()
    }
}
impl Rule {
    pub async fn update(&self, client: &Client) -> Result<()> {
        todo!()
    }
}

#[async_trait::async_trait]
impl CrudDb for RuleBinding {
    async fn create(&mut self, client: &Client) -> Result<()> {
        todo!()
    }
    async fn get(id: impl PrimaryKey, client: &Client) -> Result<Option<Self>> {
        todo!()
    }
    async fn all(client: &Client) -> Result<Vec<Self>> {
        todo!()
    }
    async fn delete(&self, client: &Client) -> Result<()> {
        todo!()
    }
}

impl RuleBinding {
    pub async fn delete_by(
        resource_id: &DieselUlid,
        policy_id: &DieselUlid,
        client: &Client,
    ) -> Result<()> {
        todo!()
    }
}
