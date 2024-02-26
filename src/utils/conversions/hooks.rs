use aruna_rust_api::api::{
    hooks::services::v2::{
        filter::FilterVariant, hook::HookType, internal_hook::InternalAction, AddHook, AddLabel,
        Credentials, ExternalHook, Filter as APIFilter, Hook as APIHook, HookInfo, InternalHook,
        Trigger,
    },
    storage::models::v2::{KeyValue, Relation},
};

use crate::database::dsls::{
    hook_dsl::{Filter, Hook},
    object_dsl::KeyValueVariant,
};

impl From<Hook> for HookInfo {
    fn from(hook: Hook) -> HookInfo {
        let trigger = Some(hook.as_trigger());
        HookInfo {
            name: hook.name.clone(),
            description: hook.description.clone(),
            hook_id: hook.id.to_string(),
            hook: Some(hook.as_api_hook()),
            trigger,
            timeout: hook.timeout.timestamp_millis() as u64,
            project_ids: hook.project_ids.iter().map(|id| id.to_string()).collect(),
        }
    }
}
impl Hook {
    fn as_trigger(&self) -> Trigger {
        Trigger {
            trigger_type: match self.trigger.0.variant {
                crate::database::dsls::hook_dsl::TriggerVariant::HOOK_ADDED => 1,
                crate::database::dsls::hook_dsl::TriggerVariant::RESOURCE_CREATED => 2,
                crate::database::dsls::hook_dsl::TriggerVariant::LABEL_ADDED => 3,
                crate::database::dsls::hook_dsl::TriggerVariant::STATIC_LABEL_ADDED => 4,
                crate::database::dsls::hook_dsl::TriggerVariant::HOOK_STATUS_CHANGED => 5,
                crate::database::dsls::hook_dsl::TriggerVariant::OBJECT_FINISHED => 6,
            },
            filters: self
                .clone()
                .trigger
                .0
                .filter
                .into_iter()
                .map(|f| match f {
                    Filter::Name(name) => APIFilter {
                        filter_variant: Some(FilterVariant::Name(name)),
                    },
                    Filter::KeyValue(kv) => APIFilter {
                        filter_variant: Some(FilterVariant::KeyValue(KeyValue {
                            key: kv.key,
                            value: kv.value,
                            variant: match kv.variant {
                                KeyValueVariant::HOOK => 3,
                                KeyValueVariant::LABEL => 1,
                                KeyValueVariant::STATIC_LABEL => 2,
                                KeyValueVariant::HOOK_STATUS => 4,
                            },
                        })),
                    },
                })
                .collect(),
        }
    }
    fn as_api_hook(&self) -> APIHook {
        match &self.hook.0 {
            crate::database::dsls::hook_dsl::HookVariant::Internal(internal_hook) => {
                let internal_action = match internal_hook {
                    crate::database::dsls::hook_dsl::InternalHook::AddLabel { key, value } => {
                        InternalAction::AddLabel(AddLabel {
                            key: key.clone(),
                            value: value.clone(),
                        })
                    }
                    crate::database::dsls::hook_dsl::InternalHook::AddHook { key, value } => {
                        InternalAction::AddHook(AddHook {
                            key: key.clone(),
                            value: value.clone(),
                        })
                    }

                    crate::database::dsls::hook_dsl::InternalHook::CreateRelation { relation } => {
                        InternalAction::AddRelation(Relation {
                            relation: Some(relation.clone()),
                        })
                    }
                };
                APIHook {
                    hook_type: Some(HookType::InternalHook(InternalHook {
                        internal_action: Some(internal_action),
                    })),
                }
            }
            crate::database::dsls::hook_dsl::HookVariant::External(external_hook) => {
                let custom_template = match &external_hook.template {
                    crate::database::dsls::hook_dsl::TemplateVariant::Basic => None,
                    crate::database::dsls::hook_dsl::TemplateVariant::Custom(string) => {
                        Some(string.clone())
                    }
                };
                APIHook {
                    hook_type: Some(HookType::ExternalHook(ExternalHook {
                        url: external_hook.url.clone(),
                        credentials: external_hook
                            .credentials
                            .clone()
                            .map(|c| Credentials { token: c.token }),
                        custom_template,
                        method: (&external_hook.method).into(),
                    })),
                }
            }
        }
    }
}
