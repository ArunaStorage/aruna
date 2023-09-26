use crate::database::crud::{CrudDb, PrimaryKey};
use crate::database::enums::ObjectMapping;
use crate::utils::database_utils::create_multi_query;
use anyhow::anyhow;
use anyhow::Result;
use aruna_rust_api::api::storage::models::v2::generic_resource::Resource;
use aruna_rust_api::api::storage::models::v2::relation::Relation;
use chrono::NaiveDateTime;
use diesel_ulid::DieselUlid;
use postgres_from_row::FromRow;
use postgres_types::{FromSql, Json, ToSql};
use serde::{Deserialize, Serialize};
use tokio_postgres::Client;

#[derive(FromRow, Debug, Clone)]
pub struct Hook {
    pub id: DieselUlid,
    pub name: String,
    pub description: String,
    pub project_ids: Vec<DieselUlid>,
    pub owner: DieselUlid,
    pub trigger_type: TriggerType,
    pub trigger_key: String,
    pub trigger_value: String,
    pub timeout: NaiveDateTime,
    pub hook: Json<HookVariant>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum HookVariant {
    Internal(InternalHook),
    External(ExternalHook),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExternalHook {
    pub url: String,
    pub credentials: Option<Credentials>,
    pub template: TemplateVariant,
    pub method: Method,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Method {
    PUT,
    POST,
}

#[allow(non_camel_case_types)]
#[derive(Clone, Debug, Serialize, Deserialize, FromSql, ToSql, PartialEq, Eq)]
pub enum TriggerType {
    HOOK_ADDED,
    OBJECT_CREATED,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum InternalHook {
    AddLabel { key: String, value: String },
    AddHook { key: String, value: String },
    CreateRelation { relation: Relation },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Credentials {
    pub token: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TemplateVariant {
    Basic,
    Custom(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BasicTemplate {
    pub hook_id: DieselUlid,
    pub object: Resource,
    pub secret: String,
    pub download: String,
    pub pubkey_serial: i32,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HookStatusValues {
    pub name: String,
    pub status: HookStatusVariant,
    pub trigger_type: TriggerType,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum HookStatusVariant {
    RUNNING,
    FINISHED,
    ERROR(String),
}

#[derive(Clone, Debug, FromSql, FromRow)]
pub struct HookWithAssociatedProject {
    pub id: DieselUlid,
    pub name: String,
    pub description: String,
    pub project_ids: Vec<DieselUlid>,
    pub owner: DieselUlid,
    pub trigger_type: TriggerType,
    pub trigger_key: String,
    pub trigger_value: String,
    pub timeout: NaiveDateTime,
    pub hook: Json<HookVariant>,
    pub project_id: DieselUlid,
}

#[async_trait::async_trait]
impl CrudDb for Hook {
    async fn create(&mut self, client: &Client) -> Result<()> {
        let query = "INSERT INTO hooks (id, name, description, project_ids, owner, trigger_type, trigger_key, trigger_value, timeout, hook) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
        ) RETURNING *;";

        let prepared = client.prepare(query).await?;

        let row = client
            .query_one(
                &prepared,
                &[
                    &self.id,
                    &self.name,
                    &self.description,
                    &self.project_ids,
                    &self.owner,
                    &self.trigger_type,
                    &self.trigger_key,
                    &self.trigger_value,
                    &self.timeout,
                    &self.hook,
                ],
            )
            .await?;

        *self = Hook::from_row(&row);
        Ok(())
    }

    async fn get(id: impl PrimaryKey, client: &Client) -> Result<Option<Self>> {
        let query = "SELECT * FROM hooks WHERE id = $1";
        let prepared = client.prepare(query).await?;
        Ok(client
            .query_opt(&prepared, &[&id])
            .await?
            .map(|e| Hook::from_row(&e)))
    }

    async fn all(client: &Client) -> Result<Vec<Self>> {
        let query = "SELECT * FROM hooks";
        let prepared = client.prepare(query).await?;
        let rows = client.query(&prepared, &[]).await?;
        Ok(rows.iter().map(Hook::from_row).collect::<Vec<_>>())
    }

    async fn delete(&self, client: &Client) -> Result<()> {
        let query = "DELETE FROM hooks WHERE id = $1;";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&self.id]).await?;
        Ok(())
    }
}
impl Hook {
    pub async fn list_hooks(project_id: &DieselUlid, client: &Client) -> Result<Vec<Hook>> {
        let ids = vec![project_id];
        let query = "SELECT * FROM hooks WHERE $1 = ANY (project_ids)";
        let prepared = client.prepare(query).await?;
        let rows = client.query(&prepared, &[&ids]).await?;
        Ok(rows.iter().map(Hook::from_row).collect::<Vec<_>>())
    }
    pub async fn list_owned(owner: &DieselUlid, client: &Client) -> Result<Vec<Hook>> {
        let query = "SELECT * FROM hooks WHERE owner = $1";
        let prepared = client.prepare(query).await?;
        let rows = client.query(&prepared, &[owner]).await?;
        Ok(rows.iter().map(Hook::from_row).collect::<Vec<_>>())
    }
    pub async fn delete_by_id(hook_id: &DieselUlid, client: &Client) -> Result<()> {
        let query = "DELETE FROM hooks WHERE id = $1;";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[hook_id]).await?;
        Ok(())
    }
    pub async fn get_project_from_hook(
        hook_id: &DieselUlid,
        client: &Client,
    ) -> Result<Vec<DieselUlid>> {
        let query = "SELECT * FROM hooks WHERE id = $1";
        let prepared = client.prepare(query).await?;
        let hook = client
            .query_opt(&prepared, &[hook_id])
            .await?
            .map(|e| Hook::from_row(&e))
            .ok_or_else(|| anyhow!("Hook not found"))?;
        Ok(hook.project_ids)
    }
    pub async fn get_hooks_for_projects(
        project_ids: &Vec<DieselUlid>,
        client: &Client,
    ) -> Result<Vec<HookWithAssociatedProject>> {
        let query = "SELECT q.id, q.name, q.description, q.project_id, q.owner, q.trigger_type, q.trigger_key, q.trigger_value, q.timeout, q.hook, 
        (
	        SELECT UNNEST(p_ids) 
            INTERSECT 
            SELECT UNNEST(arr)
        ) AS project_id
        FROM (
            SELECT project_id AS p_ids, 
            ARRAY$1::uuid[] AS arr, *
            FROM hooks
            ) q
        WHERE p_ids && arr;".to_string();
        let prepared = client.prepare(&query).await?;
        let hooks = client
            .query(&prepared, &[project_ids])
            .await?
            .iter()
            .map(HookWithAssociatedProject::from_row)
            .collect();
        Ok(hooks)
    }
}
