use crate::caching::structs::CachedRule;
use crate::database::{crud::CrudDb, dsls::object_dsl::Object};
use crate::middlelayer::rule_request_types::{CreateRuleBinding, DeleteRuleBinding, UpdateRule};
use anyhow::{anyhow, Result};
use cel_interpreter::Value;
use diesel_ulid::DieselUlid;
use std::sync::Arc;
use tokio_postgres::Client;

use super::{db_handler::DatabaseHandler, rule_request_types::CreateRule};

impl DatabaseHandler {
    pub async fn evaluate_policies(
        &self,
        affected: &Vec<DieselUlid>,
        transaction_client: &Client,
    ) -> Result<()> {
        // Policy evaluation:
        for id in affected {
            if let Some(bindings) = self.cache.get_rule_bindings(id).map(|b| b.clone()) {
                for binding in bindings.clone().iter() {
                    if let Some(rule) = self.cache.get_rule(&binding.rule_id) {
                        let mut ctx = cel_interpreter::Context::default();
                        let current_state =
                            Object::get_object_with_relations(id, &transaction_client).await?;
                        ctx.add_variable("object", current_state)
                            .map_err(|e| anyhow!(e.to_string()))?;
                        let value = rule.compiled.execute(&ctx).map_err(|e| {
                            anyhow!(format!("Policy evaluation error: {}", e.to_string()))
                        })?;
                        if value != Value::Bool(true) {
                            return Err(anyhow!(format!(
                                "Policy {} evaluated false",
                                rule.rule.rule_id
                            )));
                        }
                    } else {
                        return Err(anyhow!("Rules and Bindings are out of sync"));
                    };
                }
            }
        }
        Ok(())
    }

    pub async fn create_rule(
        &self,
        request: CreateRule,
        user_id: DieselUlid,
    ) -> Result<DieselUlid> {
        let client = self.database.get_client().await?;
        let mut rule = request.build_rule(user_id)?;
        let id = rule.rule.rule_id;
        rule.rule.create(&client).await?;
        self.cache.insert_rule(&id, rule.clone());
        Ok(id)
    }

    pub async fn update_rule(
        &self,
        request: UpdateRule,
        rule: Arc<CachedRule>,
    ) -> Result<CachedRule> {
        let client = self.database.get_client().await?;
        let updated = request.merge(&rule)?;
        updated.rule.update(&client).await?;
        Ok(updated)
    }

    pub async fn delete_rule(&self, rule: &CachedRule) -> Result<()> {
        let client = self.database.get_client().await?;
        rule.rule.delete(&client).await?;
        self.cache.delete_rule(&rule.rule.rule_id);
        Ok(())
    }

    pub async fn create_rule_binding(&self, request: CreateRuleBinding) -> Result<()> {
        let client = self.database.get_client().await?;
        let mut binding = request.get_binding()?;
        binding.create(&client).await?;
        let resource_ids = if request.0.cascading {
            self.cache.get_subresources(&binding.origin_id)?
        } else {
            vec![binding.origin_id]
        };
        self.cache.insert_rule_binding(resource_ids, binding);
        Ok(())
    }
    pub async fn delete_rule_binding(&self, request: DeleteRuleBinding) -> Result<()> {
        todo!()
    }
}
