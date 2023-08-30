use crate::database::dsls::hook_dsl::{ExternalHook, Hook, InternalHook, TriggerType};
use crate::database::dsls::internal_relation_dsl::{
    INTERNAL_RELATION_VARIANT_BELONGS_TO, INTERNAL_RELATION_VARIANT_METADATA,
    INTERNAL_RELATION_VARIANT_ORIGIN, INTERNAL_RELATION_VARIANT_POLICY,
};
use crate::database::dsls::object_dsl::KeyValue;
use anyhow::{anyhow, Result};
use aruna_rust_api::api::hooks::services::v2::{
    hook::HookType, CreateHookRequest, Hook as APIHook, InternalAction,
};
use chrono::NaiveDateTime;
use diesel_ulid::DieselUlid;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

pub struct CreateHook(pub CreateHookRequest);

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct Callback {
    pub success: bool, // to enforce error before timer runs out
    pub add_key_values: Vec<KeyValue>,
    pub remove_key_values: Vec<KeyValue>,
    pub secret: String,
    pub hook_id: DieselUlid,
    pub object_id: DieselUlid,
    pub pubkey_serial: i32,
}

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
                        }),
                    ),
                })
            }
            Some(APIHook {
                hook_type: Some(HookType::InternalHook(internal_hook)),
            }) => {
                let (trigger_type, trigger_key, trigger_value) = self.get_trigger()?;
                let internal_hook = match internal_hook.internal_action() {
                    InternalAction::AddHook => InternalHook::AddHook {
                        key: internal_hook.target_id.clone(),
                        value: internal_hook.value.clone(),
                    },
                    InternalAction::AddLabel => InternalHook::AddLabel {
                        key: internal_hook.target_id.clone(),
                        value: internal_hook.value.clone(),
                    },
                    InternalAction::CreateRelation => InternalHook::CreateRelation {
                        target_id: DieselUlid::from_str(&internal_hook.target_id)?,
                        relation_type: match internal_hook.value.as_str() {
                            INTERNAL_RELATION_VARIANT_BELONGS_TO => {
                                INTERNAL_RELATION_VARIANT_BELONGS_TO.to_string()
                            }
                            INTERNAL_RELATION_VARIANT_METADATA => {
                                INTERNAL_RELATION_VARIANT_METADATA.to_string()
                            }
                            INTERNAL_RELATION_VARIANT_POLICY => {
                                INTERNAL_RELATION_VARIANT_POLICY.to_string()
                            }
                            INTERNAL_RELATION_VARIANT_ORIGIN => {
                                INTERNAL_RELATION_VARIANT_ORIGIN.to_string()
                            }
                            _ => {
                                return Err(anyhow!(
                                    "Invalid relation type for InternalHook::CreateRelation"
                                ))
                            }
                        },
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
