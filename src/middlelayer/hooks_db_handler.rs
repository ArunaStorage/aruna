use crate::caching::cache::Cache;
use crate::database::dsls::hook_dsl::{Hook, InternalHook, TriggerType};
use crate::database::dsls::object_dsl::ObjectWithRelations;
use crate::database::enums::ObjectMapping;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::hooks_request_types::CreateHook;
use crate::{database::crud::CrudDb, middlelayer::hooks_request_types::Callback};
use anyhow::{anyhow, Result};
use aruna_rust_api::api::hooks::services::v2::{HookCallbackRequest, ListHooksRequest};
use diesel_ulid::DieselUlid;
use std::str::FromStr;
use std::sync::Arc;

use super::create_request_types::Parent;

impl DatabaseHandler {
    pub async fn create_hook(&self, request: CreateHook) -> Result<Hook> {
        let client = self.database.get_client().await?;
        let mut hook = request.get_hook()?;
        hook.create(&client).await?;
        Ok(hook)
    }
    pub async fn list_hook(&self, request: ListHooksRequest) -> Result<Vec<Hook>> {
        let client = self.database.get_client().await?;
        let project_id = DieselUlid::from_str(&request.project_id)?;
        let hooks = Hook::list_hooks(&project_id, &client).await?;
        Ok(hooks)
    }
    pub async fn delete_hook(&self, hook_id: DieselUlid) -> Result<()> {
        let client = self.database.get_client().await?;
        Hook::delete_by_id(&hook_id, &client).await?;
        Ok(())
    }
    pub async fn get_project_by_hook(&self, hook_id: &DieselUlid) -> Result<DieselUlid> {
        let client = self.database.get_client().await?;
        let project_id = Hook::get_project_from_hook(hook_id, &client).await?;
        Ok(project_id)
    }
    pub async fn hook_callback(&self, request: HookCallbackRequest) -> Result<()> {
        //let callback: Callback = request.try_into()?;
        return Err(anyhow!("Hook callback not implemented"));
    }

    // TODO : TRANSACTIONS!
    pub async fn trigger_on_creation(&self, cache: Arc<Cache>, parent: Parent) -> Result<()> {
        let client = self.database.get_client().await?;
        let parent_id = parent.get_id()?;
        let parents = cache.upstream_dfs_iterative(&parent_id)?;
        let mut projects: Vec<DieselUlid> = Vec::new();
        for branch in parents {
            projects.append(
                &mut branch
                    .iter()
                    .filter_map(|parent| match parent {
                        ObjectMapping::PROJECT(id) => Some(*id),
                        _ => None,
                    })
                    .collect(),
            );
        }
        let hooks = Hook::get_hooks_for_projects(&projects, &client)
            .await?
            .into_iter()
            .filter(|h| h.trigger_type == TriggerType::OBJECT_CREATED)
            .collect();

        self.hook_action(cache.clone(), hooks).await?;
        return Err(anyhow!("Hook trigger not implemented"));
    }
    pub async fn trigger_on_append_hook(
        &self,
        cache: Arc<Cache>,
        object: ObjectWithRelations,
    ) -> Result<()> {
        return Err(anyhow!("Hook trigger not implemented"));
    }

    async fn hook_action(&self, cache: Arc<Cache>, hooks: Vec<Hook>) -> Result<()> {
        for hook in hooks {
            match hook.hook.0 {
                crate::database::dsls::hook_dsl::HookVariant::Internal(internal_hook) => {
                    match internal_hook {
                        crate::database::dsls::hook_dsl::InternalHook::AddLabel { key, value } => {}
                        crate::database::dsls::hook_dsl::InternalHook::AddHook { key, value } => {}
                        crate::database::dsls::hook_dsl::InternalHook::CreateRelation {
                            target_id,
                            relation_type,
                        } => {}
                    }
                }
                crate::database::dsls::hook_dsl::HookVariant::External(external_hook) => {}
            }
        }
        Ok(())
    }
}
