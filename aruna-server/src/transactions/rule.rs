use crate::{
    constants::relation_types::HAS_PART,
    error::ArunaError,
    logerr,
    models::{
        models::{RawRelation, Relation, Resource},
        requests::Direction,
    },
    storage::store::Store,
};
use ahash::RandomState;
use heed::RoTxn;
use rhai::{Engine, EvalAltResult, Scope, AST};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};
use ulid::Ulid;

pub(crate) struct RuleEngine {
    rhai_engine: RwLock<Engine>,
    // Compiled rules with project mappings
    rules: RwLock<HashMap<u32, rhai::AST, RandomState>>,
}

impl RuleEngine {
    pub fn new(store: Arc<Store>) -> Result<Self, ArunaError> {
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

        // TODO: Limit scope to project only
        let store_clone = store.clone();
        engine.register_fn("get_resource", move |id: Ulid| -> Option<Resource> {
            let rtxn = store_clone.read_txn().ok()?;
            let idx = store_clone.get_idx_from_ulid(&id, &rtxn)?;
            store_clone.get_node(&rtxn, idx)
        });

        let store_clone = store.clone();
        engine.register_fn(
            "get_existing_relations",
            move |id: Ulid, direction: Direction| -> Result<Vec<Relation>, ArunaError> {
                let rtxn = store_clone.read_txn()?;
                let Some(idx) = store_clone.get_idx_from_ulid(&id, &rtxn) else {
                    return Ok(vec![]);
                };
                store_clone.get_relations(
                    idx,
                    &[HAS_PART],
                    match direction {
                        Direction::Incoming => petgraph::Direction::Incoming,
                        Direction::Outgoing => petgraph::Direction::Outgoing,
                    },
                    &rtxn,
                )
            },
        );

        Ok(Self {
            rhai_engine: RwLock::new(engine),
            rules: RwLock::new(rules),
        })
    }

    pub fn eval_rule(
        &self,
        store: &Store,
        _read_txn: &RoTxn,
        resource_idx: u32,
        resource: Resource,
        new_relations: Vec<Relation>,
    ) -> Result<bool, ArunaError> {
        let project = store.get_project(&resource_idx)?;
        if let Some(rule) = self.rules.read().expect("Poison error").get(&project) {
            let mut scope = Scope::new();
            scope.push("resource", resource);
            scope.push("new_relations", new_relations);
            self.rhai_engine
                .read()
                .expect("Poison error")
                .eval_ast_with_scope(&mut scope, rule)
                .inspect_err(logerr!())
                .unwrap_or_else(|_| Ok(false))
        } else {
            Ok(true)
        }
    }

    pub fn add_rule(&self, project_idx: u32, rule: String) -> Result<(), ArunaError> {
        let ast = self
            .rhai_engine
            .read()
            .expect("Poison error")
            .compile(rule)?;
        self.rules
            .write()
            .expect("Poison error")
            .insert(project_idx, ast);
        Ok(())
    }
}
