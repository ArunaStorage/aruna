use crate::{error::ArunaError, logerr, storage::store::Store};
use ahash::RandomState;
use rhai::{Engine, AST};
use std::{
    collections::HashMap,
    sync::RwLock,
};

pub(crate) struct RuleEngine {
    rhai_engine: RwLock<Engine>,
    // Compiled rules with project mappings
    rules: RwLock<HashMap<u32, rhai::AST, RandomState>>,
}

impl RuleEngine {
    pub fn new(store: &Store) -> Result<Self, ArunaError> {
        let mut engine = rhai::Engine::new();
        engine.build_type::<crate::models::models::Resource>();
        let rules = store
            .get_uncompiled_rules()?
            .into_iter()
            .map(|(k, v)| -> Result<(u32, AST), ArunaError> {
                Ok((
                    k,
                    engine.compile(v).map_err(|_| {
                        ArunaError::DatabaseError("Could not compile rule".to_string())
                    })?,
                ))
            })
            .collect::<Result<HashMap<u32, AST, RandomState>, ArunaError>>()?;

        Ok(Self {
            rhai_engine: RwLock::new(engine),
            rules: RwLock::new(rules),
        })
    }

    pub fn eval(&self, project_idx: u32) -> bool {
        if let Some(rule) = self.rules.read().expect("Poison error").get(&project_idx) {
            self.rhai_engine
                .read()
                .expect("Poison error")
                .eval_ast(rule)
                .inspect_err(logerr!())
                .unwrap_or_else(|_| false)
        } else {
            false
        }
    }

    pub fn add_rule(&self, project_idx: u32, rule: String) -> Result<(), ArunaError> {
        let ast = self.rhai_engine.read().expect("Poison error").compile(rule)?;
        self.rules.write().expect("Poison error").insert(project_idx, ast);
        Ok(())
    }
}
