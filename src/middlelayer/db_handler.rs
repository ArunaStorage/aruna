use crate::{
    caching::cache::Cache,
    database::{connection::Database, dsls::object_dsl::Object},
    hooks::hook_handler::HookMessage,
    notification::natsio_handler::NatsIoHandler,
};
use anyhow::{anyhow, Result};
use async_channel::Sender;
use cel_interpreter::Value;
use diesel_ulid::DieselUlid;
use std::sync::Arc;
use tokio_postgres::Client;

pub struct DatabaseHandler {
    pub database: Arc<Database>,
    pub natsio_handler: Arc<NatsIoHandler>,
    pub cache: Arc<Cache>,
    pub hook_sender: Sender<HookMessage>,
}

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
                        ctx.add_variable("object", current_state);
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
}
