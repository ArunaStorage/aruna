pub mod endpoint_dsl;
pub mod external_user_id_dsl;
pub mod hook_dsl;
pub mod identity_provider_dsl;
pub mod internal_relation_dsl;
pub mod license_dsl;
pub mod notification_dsl;
pub mod object_dsl;
pub mod persistent_notification_dsl;
pub mod pub_key_dsl;
pub mod relation_type_dsl;
pub mod rule_dsl;
pub mod stats_dsl;
pub mod user_dsl;
pub mod workspaces_dsl;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Empty {}
