use crate::database::crud::{CrudDb, PrimaryKey};
use crate::database::dsls::object_dsl::Object;
use crate::database::dsls::relation_type_dsl::RelationType;
use anyhow::Result;
use aruna_rust_api::api::storage::models::v2::generic_resource::Resource;
use chrono::NaiveDateTime;
use diesel_ulid::DieselUlid;
use postgres_from_row::FromRow;
use postgres_types::Json;
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
    pub credentials: Credentials,
    pub template: TemplateVariant, // TODO: Create templates
}

#[allow(non_camel_case_types)]
#[derive(Clone, Debug, Serialize, Deserialize)]
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
        relation_type: RelationType,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Credentials {
    pub token: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TemplateVariant {
    BasicTemplate(BasicTemplate),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BasicTemplate {
    pub hook_id: DieselUlid,
    pub object: Resource,
    pub secret: String,
    pub download: String,
    pub upload: String,
}
