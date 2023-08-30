use crate::database::crud::{CrudDb, PrimaryKey};
use crate::utils::database_utils::create_multi_query;
use anyhow::anyhow;
use anyhow::Result;
use aruna_rust_api::api::storage::models::v2::generic_resource::Resource;
use chrono::NaiveDateTime;
use diesel_ulid::DieselUlid;
use postgres_from_row::FromRow;
use postgres_types::{FromSql, Json, ToSql};
use serde::{Deserialize, Serialize};
use tokio_postgres::Client;

#[derive(FromRow, Debug, Clone)]
pub struct Hook {
    pub id: DieselUlid,
    pub project_id: DieselUlid,
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
}

#[allow(non_camel_case_types)]
#[derive(Clone, Debug, Serialize, Deserialize, FromSql, ToSql, PartialEq, Eq)]
pub enum TriggerType {
    HOOK_ADDED,
    OBJECT_CREATED,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum InternalHook {
    AddLabel {
        key: String,
        value: String,
    },
    AddHook {
        key: String,
        value: String,
    },
    CreateRelation {
        target_id: DieselUlid,
        relation_type: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Credentials {
    pub token: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TemplateVariant {
    BasicTemplate,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BasicTemplate {
    pub hook_id: DieselUlid,
    pub object: Resource,
    pub secret: String,
    pub download: String,
    pub upload: String,
}

#[async_trait::async_trait]
impl CrudDb for Hook {
    async fn create(&mut self, client: &Client) -> Result<()> {
        let query = "INSERT INTO hooks (id, project_id, trigger_type, trigger_key, trigger_value, timeout, hook) VALUES (
            $1, $2, $3, $4, $5, $6, $7
        ) RETURNING *;";

        let prepared = client.prepare(query).await?;

        let row = client
            .query_one(
                &prepared,
                &[
                    &self.id,
                    &self.project_id,
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
        let query = "SELECT * FROM hooks WHERE project_id = $1";
        let prepared = client.prepare(query).await?;
        let rows = client.query(&prepared, &[project_id]).await?;
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
    ) -> Result<DieselUlid> {
        let query = "SELECT * FROM hooks WHERE id = $1";
        let prepared = client.prepare(query).await?;
        let hook = client
            .query_opt(&prepared, &[hook_id])
            .await?
            .map(|e| Hook::from_row(&e))
            .ok_or_else(|| anyhow!("Hook not found"))?;
        Ok(hook.project_id)
    }
    pub async fn get_hooks_for_projects(
        project_ids: &Vec<DieselUlid>,
        client: &Client,
    ) -> Result<Vec<Hook>> {
        let query_one = "SELECT * FROM hooks WHERE id IN";
        let mut inserts = Vec::<&(dyn ToSql + Sync)>::new();
        for id in project_ids {
            inserts.push(id);
        }
        let query_two = create_multi_query(&inserts);
        let query = format!("{}{}", query_one, query_two);
        let prepared = client.prepare(&query).await?;
        let hooks = client
            .query(&prepared, &inserts)
            .await?
            .iter()
            .map(Hook::from_row)
            .collect();
        Ok(hooks)
    }
}
