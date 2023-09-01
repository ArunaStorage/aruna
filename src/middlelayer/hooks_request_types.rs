use crate::auth::permission_handler::PermissionHandler;
use crate::caching::cache::Cache;
use crate::database::dsls::hook_dsl::{ExternalHook, Hook, InternalHook, TriggerType};
use crate::database::dsls::object_dsl::{KeyValue, KeyValues};
use anyhow::{anyhow, Result};
use aruna_rust_api::api::hooks::services::v2::HookCallbackRequest;
use aruna_rust_api::api::hooks::services::v2::{
    hook::HookType, CreateHookRequest, Hook as APIHook,
};
use aruna_rust_api::api::hooks::services::v2::{internal_hook::InternalAction, AddHook, AddLabel};
use chrono::NaiveDateTime;
use diesel_ulid::DieselUlid;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;

pub struct CreateHook(pub CreateHookRequest);

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct Callback(pub HookCallbackRequest);

impl CreateHook {
    fn get_trigger(&self) -> Result<(TriggerType, String, String)> {
        match self.0.trigger.clone() {
            Some(trigger) => match trigger.trigger_type() {
                aruna_rust_api::api::hooks::services::v2::TriggerType::HookAdded => {
                    Ok((TriggerType::HOOK_ADDED, trigger.key, trigger.value))
                }
                aruna_rust_api::api::hooks::services::v2::TriggerType::ObjectCreated => {
                    Ok((TriggerType::OBJECT_CREATED, trigger.key, trigger.value))
                }
                _ => Err(anyhow!("Invalid trigger type")),
            },
            None => Err(anyhow!("No trigger defined")),
        }
    }
    fn get_timeout(&self) -> Result<NaiveDateTime> {
        NaiveDateTime::from_timestamp_millis(self.0.timeout as i64)
            .ok_or_else(|| anyhow!("Invalid timeout provided"))
    }
    pub fn get_project_id(&self) -> Result<DieselUlid> {
        Ok(DieselUlid::from_str(&self.0.project_id)?)
    }
    pub fn get_hook(&self) -> Result<Hook> {
        match &self.0.hook {
            Some(APIHook {
                hook_type: Some(HookType::ExternalHook(external_hook)),
            }) => {
                let (trigger_type, trigger_key, trigger_value) = self.get_trigger()?;
                Ok(Hook {
                    id: DieselUlid::generate(),
                    project_id: self.get_project_id()?,
                    trigger_type,
                    trigger_key,
                    trigger_value,
                    timeout: self.get_timeout()?,
                    hook: postgres_types::Json(
                        crate::database::dsls::hook_dsl::HookVariant::External(ExternalHook {
                            url: external_hook.url.clone(),
                            credentials: external_hook.credentials.clone().map(|c| {
                                crate::database::dsls::hook_dsl::Credentials { token: c.token }
                            }),
                            template:
                                crate::database::dsls::hook_dsl::TemplateVariant::BasicTemplate,
                            method: crate::database::dsls::hook_dsl::Method::PUT,
                        }),
                    ),
                })
            }
            Some(APIHook {
                hook_type: Some(HookType::InternalHook(internal_hook)),
            }) => {
                let (trigger_type, trigger_key, trigger_value) = self.get_trigger()?;
                let internal_hook = match &internal_hook.internal_action {
                    Some(InternalAction::AddLabel(AddLabel { key, value })) => {
                        InternalHook::AddHook {
                            key: key.clone(),
                            value: value.clone(),
                        }
                    }
                    Some(InternalAction::AddHook(AddHook { key, value })) => {
                        InternalHook::AddLabel {
                            key: key.clone(),
                            value: value.clone(),
                        }
                    }
                    Some(InternalAction::AddRelation(relation)) => InternalHook::CreateRelation {
                        relation: relation
                            .relation
                            .clone()
                            .ok_or_else(|| anyhow!("No relation provided"))?,
                    },
                    _ => return Err(anyhow!("Invalid internal action")),
                };
                Ok(Hook {
                    id: DieselUlid::generate(),
                    project_id: self.get_project_id()?,
                    trigger_type,
                    trigger_key,
                    trigger_value,
                    timeout: self.get_timeout()?,
                    hook: postgres_types::Json(
                        crate::database::dsls::hook_dsl::HookVariant::Internal(internal_hook),
                    ),
                })
            }
            _ => Err(anyhow!("Invalid hook provided")),
        }
    }
}

impl Callback {
    pub fn get_keyvals(&self) -> Result<(KeyValues, KeyValues)> {
        let add = (&self.0.add_key_values).try_into()?;
        let rm = (&self.0.remove_key_values).try_into()?;
        Ok((add, rm))
    }

    pub fn verify_secret(
        &self,
        authorizer: Arc<PermissionHandler>,
        cache: Arc<Cache>,
    ) -> Result<()> {
        let (hook_id, object_id) = self.get_ids()?;
        let pubkey_serial = self.0.pubkey_serial.parse()?;
        let secret = self.0.secret;
        authorizer.token_handler.verify_hook_secret(
            cache.clone(),
            secret,
            object_id,
            hook_id,
            pubkey_serial,
        )?;
        Ok(())
    }

    pub fn get_ids(&self) -> Result<(DieselUlid, DieselUlid)> {
        Ok((
            DieselUlid::from_str(&self.0.hook_id)?,
            DieselUlid::from_str(&self.0.object_id)?,
        ))
    }
}
