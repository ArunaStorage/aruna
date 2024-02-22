use crate::config::RuleTarget;
use crate::CONFIG;
use anyhow::anyhow;
use anyhow::Result;
use cel_parser::ast::Expression;

pub(super) struct RuleEngine {
    object_expressions: Vec<Expression>,
    bundle_expressions: Vec<Expression>,
    object_package_expressions: Vec<Expression>,
    replication_inbound_expressions: Vec<Expression>,
    replication_outbound_expressions: Vec<Expression>,
}

impl RuleEngine {
    pub fn new() -> Result<Self> {
        let mut object_expressions = Vec::new();
        let mut bundle_expressions = Vec::new();
        let mut object_package_expressions = Vec::new();
        let mut replication_inbound_expressions = Vec::new();
        let mut replication_outbound_expressions = Vec::new();
        for rule in CONFIG.rules {
            match rule.target {
                RuleTarget::ALL => {
                    let expr = cel_parser::parse(rule.rule.as_str())
                        .map_err(|e| anyhow!("error parsing rule: {}", e))?;
                    object_expressions.push(expr.clone());
                    bundle_expressions.push(expr.clone());
                    object_package_expressions.push(expr.clone());
                    replication_inbound_expressions.push(expr.clone());
                    replication_outbound_expressions.push(expr);
                }
                RuleTarget::OBJECT => {
                    object_expressions.push(
                        cel_parser::parse(rule.rule.as_str())
                            .map_err(|e| anyhow!("error parsing rule: {}", e))?,
                    );
                }
                RuleTarget::BUNDLE => {
                    bundle_expressions.push(
                        cel_parser::parse(rule.rule.as_str())
                            .map_err(|e| anyhow!("error parsing rule: {}", e))?,
                    );
                }
                RuleTarget::OBJECTPACKAGE => {
                    object_package_expressions.push(
                        cel_parser::parse(rule.rule.as_str())
                            .map_err(|e| anyhow!("error parsing rule: {}", e))?,
                    );
                }
                RuleTarget::REPLICATIONIN => {
                    replication_inbound_expressions.push(
                        cel_parser::parse(rule.rule.as_str())
                            .map_err(|e| anyhow!("error parsing rule: {}", e))?,
                    );
                }
                RuleTarget::REPLICATIONOUT => {
                    replication_outbound_expressions.push(
                        cel_parser::parse(rule.rule.as_str())
                            .map_err(|e| anyhow!("error parsing rule: {}", e))?,
                    );
                }
            }
        }
        Ok(RuleEngine {
            object_expressions,
            bundle_expressions,
            object_package_expressions,
            replication_inbound_expressions,
            replication_outbound_expressions,
        })
    }
}
