use crate::config::RuleTarget;
use crate::CONFIG;
use anyhow::anyhow;
use anyhow::Result;
use cel_interpreter::Context;
use cel_interpreter::ResolveResult;
use cel_interpreter::Value;
use cel_parser::ast::Expression;
use tracing::error;

use super::rule_structs::BundleRuleInput;
use super::rule_structs::ObjectRuleInput;
use super::rule_structs::PackageObjectRuleInput;
use super::rule_structs::ReplicationIncomingRuleInput;
use super::rule_structs::ReplicationOutgoingRuleInput;
use super::rule_structs::RootRuleInput;

pub trait AddExpression {
    fn add_expression(self, other: Self) -> Self;
    fn run(&self, ctx: &Context) -> ResolveResult;
}

impl AddExpression for Expression {
    fn add_expression(self, other: Self) -> Self {
        Expression::And(Box::new(self), Box::new(other))
    }
    fn run(&self, ctx: &Context) -> ResolveResult {
        Value::resolve(&self, ctx)
    }
}

pub(super) struct RuleEngine {
    root: Expression,
    obj: Expression,
    bundle: Expression,
    obj_package: Expression,
    repl_in: Expression,
    repl_out: Expression,
}

impl RuleEngine {
    pub fn new() -> Result<Self> {
        let mut root_expressions = Expression::Atom(cel_parser::Atom::Bool(true));
        let mut object_expressions = Expression::Atom(cel_parser::Atom::Bool(true));
        let mut bundle_expressions = Expression::Atom(cel_parser::Atom::Bool(true));
        let mut object_package_expressions = Expression::Atom(cel_parser::Atom::Bool(true));
        let mut replication_inbound_expressions = Expression::Atom(cel_parser::Atom::Bool(true));
        let mut replication_outbound_expressions = Expression::Atom(cel_parser::Atom::Bool(true));
        for rule in CONFIG.rules {
            match rule.target {
                RuleTarget::ROOT => {
                    let expr = cel_parser::parse(rule.rule.as_str())
                        .map_err(|e| anyhow!("error parsing rule: {}", e))?;
                    root_expressions = root_expressions.add_expression(expr);
                }
                RuleTarget::OBJECT => {
                    object_expressions.add_expression(
                        cel_parser::parse(rule.rule.as_str())
                            .map_err(|e| anyhow!("error parsing rule: {}", e))?,
                    );
                }
                RuleTarget::BUNDLE => {
                    bundle_expressions.add_expression(
                        cel_parser::parse(rule.rule.as_str())
                            .map_err(|e| anyhow!("error parsing rule: {}", e))?,
                    );
                }
                RuleTarget::OBJECTPACKAGE => {
                    object_package_expressions.add_expression(
                        cel_parser::parse(rule.rule.as_str())
                            .map_err(|e| anyhow!("error parsing rule: {}", e))?,
                    );
                }
                RuleTarget::REPLICATIONIN => {
                    replication_inbound_expressions.add_expression(
                        cel_parser::parse(rule.rule.as_str())
                            .map_err(|e| anyhow!("error parsing rule: {}", e))?,
                    );
                }
                RuleTarget::REPLICATIONOUT => {
                    replication_outbound_expressions.add_expression(
                        cel_parser::parse(rule.rule.as_str())
                            .map_err(|e| anyhow!("error parsing rule: {}", e))?,
                    );
                }
            }
        }
        Ok(RuleEngine {
            root: root_expressions,
            obj: object_expressions,
            bundle: bundle_expressions,
            obj_package: object_package_expressions,
            repl_in: replication_inbound_expressions,
            repl_out: replication_outbound_expressions,
        })
    }

    #[tracing::instrument(level = "trace", skip(self, ctx))]
    pub fn evaluate_root(&self, ctx: RootRuleInput) -> Result<bool> {
        let mut context = Context::default();
        context.add_variable("input", ctx).map_err(|e| {
            error!(error = ?e, "error adding variable");
            anyhow!("error adding variable: {}", e)
        })?;
        let result = self.root.run(&context).map_err(|e| {
            error!(error = ?e, "error running context");
            anyhow!("error running context: {}", e)
        })?;
        match result {
            Value::Bool(b) => Ok(b),
            _ => {
                error!("expected bool, got {:?}", result);
                Err(anyhow!("expected bool, got {:?}", result)
            )},
        }
    }

    pub fn evaluate_object(&self, ctx: ObjectRuleInput) -> Result<bool> {
        let mut context = Context::default();
        context.add_variable("input", ctx).map_err(|e| {
            error!(error = ?e, "error adding variable");
            anyhow!("error adding variable: {}", e)
        })?;
        let result = self.obj.run(&context).map_err(|e| {
            error!(error = ?e, "error running context");
            anyhow!("error running context: {}", e)
        })?;
        match result {
            Value::Bool(b) => Ok(b),
            _ => Err(anyhow!("expected bool, got {:?}", result)),
        }
    }

    pub fn evaluate_package(&self, ctx: PackageObjectRuleInput) -> Result<bool> {
        let mut context = Context::default();
        context.add_variable("input", ctx).map_err(|e| {
            error!(error = ?e, "error adding variable");
            anyhow!("error adding variable: {}", e)
        })?;
        let result = self.obj_package.run(&context).map_err(|e| {
            error!(error = ?e, "error running context");
            anyhow!("error running context: {}", e)
        })?;
        match result {
            Value::Bool(b) => Ok(b),
            _ => Err(anyhow!("expected bool, got {:?}", result)),
        }
    }


    pub fn evaluate_bundle(&self, ctx: BundleRuleInput) -> Result<bool> {
        let mut context = Context::default();
        context.add_variable("input", ctx).map_err(|e| {
            error!(error = ?e, "error adding variable");
            anyhow!("error adding variable: {}", e)
        })?;
        let result = self.bundle.run(&context).map_err(|e| {
            error!(error = ?e, "error running context");
            anyhow!("error running context: {}", e)
        })?;
        match result {
            Value::Bool(b) => Ok(b),
            _ => Err(anyhow!("expected bool, got {:?}", result)),
        }
    }

    pub fn evaluate_incoming_replication(&self, ctx: ReplicationIncomingRuleInput) -> Result<bool> {
        let mut context = Context::default();
        context.add_variable("input", ctx).map_err(|e| {
            error!(error = ?e, "error adding variable");
            anyhow!("error adding variable: {}", e)
        })?;
        let result = self.bundle.run(&context).map_err(|e| {
            error!(error = ?e, "error running context");
            anyhow!("error running context: {}", e)
        })?;
        match result {
            Value::Bool(b) => Ok(b),
            _ => Err(anyhow!("expected bool, got {:?}", result)),
        }
    }

    pub fn evaluate_outgoing_replication(&self, ctx: ReplicationOutgoingRuleInput) -> Result<bool> {
        let mut context = Context::default();
        context.add_variable("input", ctx).map_err(|e| {
            error!(error = ?e, "error adding variable");
            anyhow!("error adding variable: {}", e)
        })?;
        let result = self.bundle.run(&context).map_err(|e| {
            error!(error = ?e, "error running context");
            anyhow!("error running context: {}", e)
        })?;
        match result {
            Value::Bool(b) => Ok(b),
            _ => Err(anyhow!("expected bool, got {:?}", result)),
        }
    }
}
