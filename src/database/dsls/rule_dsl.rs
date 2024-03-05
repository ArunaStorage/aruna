use crate::database::crud::{CrudDb, PrimaryKey};
use anyhow::{anyhow, Result};
use diesel_ulid::DieselUlid;
use postgres_from_row::FromRow;
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use tokio_postgres::Client;

#[derive(FromRow, FromSql, Debug, Clone, ToSql, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct RuleBinding {
    pub rule_id: DieselUlid,
    pub origin_id: DieselUlid,
    pub object_id: DieselUlid,
    pub cascading: bool,
}

#[derive(FromRow, FromSql, Debug, Clone, ToSql, PartialEq, Eq)]
pub struct Rule {
    pub id: DieselUlid,
    pub rule_expressions: String,
    pub description: String,
    pub owner_id: DieselUlid,
    pub is_public: bool,
}

#[async_trait::async_trait]
impl CrudDb for Rule {
    async fn create(&mut self, client: &Client) -> Result<()> {
        let query = "INSERT INTO rules 
        (id, rule_expressions, description, owner_id, is_public)
        VALUES ($1, $2, $3, $4, $5) RETURNING *;";

        let prepared = client.prepare(query).await?;

        let row = client
            .query_one(
                &prepared,
                &[
                    &self.id,
                    &self.rule_expressions,
                    &self.description,
                    &self.owner_id,
                    &self.is_public,
                ],
            )
            .await?;

        *self = Rule::from_row(&row);
        Ok(())
    }
    async fn get(id: impl PrimaryKey, client: &Client) -> Result<Option<Self>> {
        let query = "SELECT * FROM rules WHERE id = $1";
        let prepared = client.prepare(query).await?;
        Ok(client
            .query_opt(&prepared, &[&id])
            .await?
            .map(|e| Rule::from_row(&e)))
    }
    async fn all(client: &Client) -> Result<Vec<Self>> {
        let query = "SELECT * FROM rules";
        let prepared = client.prepare(query).await?;
        let rows = client.query(&prepared, &[]).await?;
        Ok(rows.iter().map(Rule::from_row).collect::<Vec<_>>())
    }
    async fn delete(&self, client: &Client) -> Result<()> {
        let query = "DELETE FROM rules WHERE id = $1";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&self.id]).await?;
        Ok(())
    }
}
impl Rule {
    pub async fn update(&self, client: &Client) -> Result<()> {
        let query = "UPDATE rules 
        SET rule_expressions = $2, description = $3, owner_id = $4, is_public = $5 
        WHERE id = $1";
        let prepared = client.prepare(query).await?;
        client
            .execute(
                &prepared,
                &[
                    &self.id,
                    &self.rule_expressions,
                    &self.description,
                    &self.owner_id,
                    &self.is_public,
                ],
            )
            .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl CrudDb for RuleBinding {
    async fn create(&mut self, client: &Client) -> Result<()> {
        let query = "INSERT INTO rule_bindings 
        (rule_id, origin_id, object_id, cascading)
        VALUES ($1, $2, $3, $4) RETURNING *;";

        let prepared = client.prepare(query).await?;

        let row = client
            .query_one(
                &prepared,
                &[
                    &self.rule_id,
                    &self.origin_id,
                    &self.object_id,
                    &self.cascading,
                ],
            )
            .await?;

        *self = RuleBinding::from_row(&row);
        Ok(())
    }
    async fn get(_id: impl PrimaryKey, _client: &Client) -> Result<Option<Self>> {
        // Always fails, because schema has defined 3 primary keys, and trait requires to return just one object, which is not always one.
        // Instead of returning only the first object, failing seems better, because this is unexpected when using get
        Err(anyhow!("Cannot get unique entry"))
    }
    async fn all(client: &Client) -> Result<Vec<Self>> {
        let query = "SELECT * FROM rule_bindings";
        let prepared = client.prepare(query).await?;
        let rows = client.query(&prepared, &[]).await?;
        Ok(rows.iter().map(RuleBinding::from_row).collect::<Vec<_>>())
    }
    async fn delete(&self, client: &Client) -> Result<()> {
        let query = "DELETE FROM rule_bindings WHERE rule_id = $1 AND origin_id = $2";
        let prepared = client.prepare(query).await?;
        client
            .execute(&prepared, &[&self.rule_id, &self.origin_id])
            .await?;
        Ok(())
    }
}

impl RuleBinding {
    pub async fn delete_by(
        rule_id: DieselUlid,
        origin_id: DieselUlid,
        client: &Client,
    ) -> Result<()> {
        let query = "DELETE FROM rule_bindings WHERE rule_id = $1 AND origin_id = $2";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&rule_id, &origin_id]).await?;
        Ok(())
    }
}
