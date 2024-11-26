use crate::{
    constants::relation_types::HAS_PART,
    error::ArunaError,
    logerr,
    models::{
        models::{EdgeType, NodeVariant, Permission, Relation, Resource},
        requests::Direction,
    },
    storage::store::{Store, WriteTxn},
};
use ahash::RandomState;
use heed::RoTxn;
use petgraph::Graph;
use rhai::{Engine, NativeCallContext, Scope, AST};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};
use ulid::Ulid;

use super::request::Requester;

pub struct RuleEngine {
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
        let store_clone = store.clone();
        engine.register_fn(
            "authorize",
            move |requester: Requester, min_perm: Permission, source: Ulid| -> bool {
                let Some(user_id) = requester.get_id() else {
                    return false;
                };
                let source = source.clone();
                let Ok(perm) = store_clone.get_permissions(&source, &user_id) else {
                    return false;
                };
                perm >= min_perm
            },
        );

        Ok(Self {
            rhai_engine: RwLock::new(engine),
            rules: RwLock::new(rules),
        })
    }

    pub fn eval_rule(
        &self,
        store: Arc<Store>,
        graph: &Graph<NodeVariant, EdgeType>,
        wtxn: Arc<Mutex<WriteTxn>>,
        requester: Requester,
        id: Ulid
    ) -> Result<bool, ArunaError> {
        let project = store.get_project(graph, &wtxn.lock().unwrap().get_txn(), id)?;
        if let Some(rule) = self.rules.read().expect("Poison error").get(&project) {
            let store_clone = store.clone();
            let mut engine = self.rhai_engine.write().expect("Poison error");
            let requester_clone = requester.clone();
            let rtxn = wtxn.clone();
            engine.register_fn(
                "get_resource",
                move |ctx: NativeCallContext, id: Ulid| -> Option<Resource> {
                    let requester_clone = requester_clone.clone();
                    let Ok(has_permission): Result<bool, _> =
                        ctx.call_native_fn("authorize", (requester_clone, Permission::Read, id))
                    else {
                        return None;
                    };
                    if !has_permission {
                        return None;
                    }
                    let rtxn = rtxn.lock().unwrap().get_txn();
                    let idx = store_clone.get_idx_from_ulid(&id, &rtxn)?;
                    if !store_clone.is_part_of_project(project, idx) {
                        return None;
                    }

                    store_clone.get_node(&rtxn, idx)
                },
            );

            let store_clone = store.clone();
            engine.register_fn(
                "get_existing_relations",
                move |ctx: NativeCallContext, id: Ulid, direction: Direction| -> Result<Vec<Relation>, ArunaError> {
                    let requester_clone = requester.clone();
                    let Ok(has_permission): Result<bool, _> =
                        ctx.call_native_fn("authorize", (requester_clone, Permission::Read, id))
                    else {
                        return Ok(vec![]);
                    };
                    if !has_permission {
                        return Ok(vec![]);
                    }
                    let rtxn = store_clone.read_txn()?;
                    let Some(idx) = store_clone.get_idx_from_ulid(&id, &rtxn) else {
                        return Ok(vec![]);
                    };

                    if !store_clone.is_part_of_project(project, idx)
                        || (idx == project && Direction::Incoming == direction)
                    {
                        return Ok(vec![]);
                    }
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
            let mut scope = Scope::new();
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
