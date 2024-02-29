use crate::database::dsls::rule_dsl::RuleBinding;
use crate::{caching::structs::CachedRule, database::dsls::rule_dsl::Rule};
use anyhow::{anyhow, Result};
use aruna_rust_api::api::storage::services::v2::{
    CreateRuleBindingRequest, CreateRuleRequest, DeleteRuleBindingRequest, DeleteRuleRequest,
    UpdateRuleRequest,
};
use cel_parser::Expression;
use diesel_ulid::DieselUlid;
use std::str::FromStr;

pub struct CreateRule(pub CreateRuleRequest);
pub struct UpdateRule(pub UpdateRuleRequest);
pub struct DeleteRule(pub DeleteRuleRequest);
#[derive(Clone)]
pub struct CreateRuleBinding(pub CreateRuleBindingRequest);
pub struct DeleteRuleBinding(pub DeleteRuleBindingRequest);

impl CreateRule {
    pub fn build_rule(&self, user_id: DieselUlid) -> Result<CachedRule> {
        let compiled = self.compile_rule()?;
        let rule = Rule {
            id: DieselUlid::generate(),
            rule_expressions: self.0.rule.clone(),
            description: self.0.description.clone(),
            owner_id: user_id,
            is_public: self.0.public,
        };
        Ok(CachedRule { rule, compiled })
    }
    fn compile_rule(&self) -> Result<Expression> {
        cel_parser::parse(&self.0.rule).map_err(|e| anyhow!(e.to_string()))
    }
}

impl UpdateRule {
    pub fn get_id(&self) -> Result<DieselUlid> {
        Ok(DieselUlid::from_str(&self.0.id)?)
    }
    pub fn merge(&self, existing: &CachedRule) -> Result<CachedRule> {
        let rule_expressions = if !self.0.rule.is_empty() {
            self.0.rule.clone()
        } else {
            existing.rule.rule_expressions.clone()
        };
        let description = if !self.0.description.is_empty() {
            self.0.description.clone()
        } else {
            existing.rule.description.clone()
        };
        let is_public = self.0.public;
        let compiled =  cel_parser::parse(&rule_expressions).map_err(|e| anyhow!(e.to_string()))?;
        let rule = Rule {
            id: existing.rule.id,
            rule_expressions,
            description,
            owner_id: existing.rule.owner_id,
            is_public,
        };
        Ok(CachedRule {
            rule,
            compiled,
        })
    }
}

impl DeleteRule {
    pub fn get_id(&self) -> Result<DieselUlid> {
        Ok(DieselUlid::from_str(&self.0.id)?)
    }
}

impl CreateRuleBinding {
    pub fn get_rule_id(&self) -> Result<DieselUlid> {
        Ok(DieselUlid::from_str(&self.0.rule_id)?)
    }
    pub fn get_resource_id(&self) -> Result<DieselUlid> {
        Ok(DieselUlid::from_str(&self.0.object_id)?)
    }
    pub fn get_binding(&self) -> Result<RuleBinding> {
        let origin_id = self.get_resource_id()?;
        Ok(RuleBinding {
            rule_id: self.get_rule_id()?,
            origin_id,
            object_id: origin_id,
            cascading: self.0.cascading,
        })
    }
}

impl DeleteRuleBinding {
    pub fn get_ids(&self) -> Result<(DieselUlid, DieselUlid)> {
        let res_id = DieselUlid::from_str(&self.0.object_id)?;
        let rule_id = DieselUlid::from_str(&self.0.rule_id)?;
        Ok((res_id, rule_id))
    }
}
