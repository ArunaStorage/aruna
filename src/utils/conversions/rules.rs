use aruna_rust_api::api::storage::services::v2::Rule;

use crate::caching::structs::CachedRule;

impl From<&CachedRule> for Rule {
    fn from(value: &CachedRule) -> Self {
        Rule {
            id: value.rule.rule_id.to_string(),
            rule: value.rule.rule_expressions.to_string(),
            description: value.rule.description.to_string(),
            public: value.rule.is_public,
            owner: value.rule.owner_id.to_string(),
        }
    }
}
