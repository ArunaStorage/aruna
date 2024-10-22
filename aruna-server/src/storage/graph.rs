use std::{
    cmp::min,
    collections::{HashMap, VecDeque},
};

use heed::PutFlags;
use tracing::error;

use crate::{
    error::ArunaError,
    logerr,
    models::{
        EdgeType, Permission, RawRelation, 
        relation_types::{HAS_PART, OWNED_BY_USER, OWNS_PROJECT, PERMISSION_ADMIN, PERMISSION_NONE, SHARES_PERMISSION}
    },
    requests::transaction::Requester,
    storage::viewstore::{NodeDb, RelationDb, NODE_DB_NAME, RELATION_DB_NAME},
};
use ahash::RandomState;
use heed::Env;
use petgraph::{
    graph::NodeIndex,
    Direction::{self, Incoming, Outgoing},
};
use ulid::Ulid;

pub struct ArunaGraph {
    graph: petgraph::graph::Graph<NodeVariantId, EdgeType>,
    node_idx: HashMap<Ulid, NodeIndex, RandomState>,
}

impl ArunaGraph {
    pub fn from_env(env: Env) -> Result<Self, ArunaError> {
        let mut graph = petgraph::graph::Graph::new();
        let mut node_idx = HashMap::default();
        let read_txn = env.read_txn()?;

        // Create all nodes
        let node_db: Option<NodeDb> = env.open_database(&read_txn, Some(NODE_DB_NAME))?;
        if let Some(db) = node_db {
            for entry in db.iter(&read_txn)? {
                let (id, node_variant) = entry?;
                let node = match node_variant {
                    NodeVariantValue::Resource(_) => NodeVariantId::Resource(id),
                    NodeVariantValue::User(_) => NodeVariantId::User(id),
                    NodeVariantValue::Token(_) => NodeVariantId::Token(id),
                    NodeVariantValue::ServiceAccount(_) => NodeVariantId::ServiceAccount(id),
                    NodeVariantValue::Group(_) => NodeVariantId::Group(id),
                    NodeVariantValue::Realm(_) => NodeVariantId::Realm(id),
                };
                let idx = graph.add_node(node);
                node_idx.insert(id, idx);
            }
        }

        // Create all relations
        let relation_db: Option<RelationDb> =
            env.open_database(&read_txn, Some(RELATION_DB_NAME))?;

        if let Some(db) = relation_db {
            for entry in db.iter(&read_txn)? {
                let (
                    RawRelation {
                        source,
                        target,
                        edge_type,
                    },
                    _,
                ) = entry?;
                let source_idx = node_idx
                    .get(source.get_ref())
                    .ok_or_else(|| ArunaError::GraphError("Entry not found in idx".to_string()))?;
                let target_idx = node_idx
                    .get(target.get_ref())
                    .ok_or_else(|| ArunaError::GraphError("Entry not found in idx".to_string()))?;
                graph.add_edge(*source_idx, *target_idx, edge_type);
            }
        }

        Ok(Self { graph, node_idx })
    }

    pub fn new() -> Self {
        Self {
            graph: petgraph::graph::Graph::new(),
            node_idx: HashMap::default(),
        }
    }

    pub fn get_relations(
        &self,
        id: NodeVariantId,
        filter: Vec<EdgeType>,
        direction: Direction,
    ) -> Vec<RawRelation> {
        let Some(node_idx) = self.node_idx.get(id.get_ref()) else {
            return vec![];
        };
        self.graph
            .edges_directed(*node_idx, direction)
            .filter_map(|e| {
                if filter.contains(e.weight()) {
                    match self.graph.node_weight(e.source()) {
                        None => None,
                        Some(variant) => match direction {
                            Outgoing => Some(RawRelation {
                                target: variant.clone(),
                                source: id.clone(),
                                edge_type: *e.weight(),
                            }),
                            Incoming => Some(RawRelation {
                                source: variant.clone(),
                                target: id.clone(),
                                edge_type: *e.weight(),
                            }),
                        },
                    }
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn get_parents(&self, id: &Ulid) -> Vec<Ulid> {
        let Some(node_idx) = self.node_idx.get(id) else {
            return vec![];
        };

        self.graph
            .edges_directed(*node_idx, Incoming)
            .filter_map(|e| {
                if e.weight() == &0 {
                    match self.graph.node_weight(e.source()) {
                        None => None,
                        Some(variant) => {
                            if let NodeVariantId::Resource(id) = variant {
                                Some(*id)
                            } else {
                                None
                            }
                        }
                    }
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn get_children(&self, id: &Ulid) -> Vec<Ulid> {
        let Some(node_idx) = self.node_idx.get(id) else {
            return vec![];
        };

        self.graph
            .edges_directed(*node_idx, Outgoing)
            .filter_map(|e| {
                if e.weight() == &0 {
                    match self.graph.node_weight(e.target()) {
                        None => None,
                        Some(variant) => {
                            if let NodeVariantId::Resource(id) = variant {
                                Some(*id)
                            } else {
                                None
                            }
                        }
                    }
                } else {
                    None
                }
            })
            .collect()
    }

    /// Perform a breadth-first search for incoming edges to find the highest permission
    /// 1. Start at the resource node with a theoretical maximum permission
    /// 2. Iterate over all incoming edges
    /// 3. If the edge is a colored as a "permission" edge add the source node with the minimum permission to the queue
    /// 4. If the edge is a colored as a "hierarchy related non permission" edge add the source node with the previous current perm to the queue
    /// 5. If the source node is the target node, update the highest permission to the maximum of the current permission and the previous highest permission
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_permissions(
        &self,
        resource_id: &Ulid,
        identity: &Ulid,
    ) -> Result<Permission, ArunaError> {
        // Resource could be: Group, User, Projects, Folder, Object || TODO: Realm, ServiceAccount, Hooks, etc.

        let node_idx = self.node_idx.get(resource_id).ok_or_else(|| {
            error!(?resource_id, "Resource not found");
            ArunaError::Unauthorized
        })?;
        let Some(target_idx) = self.node_idx.get(identity) else {
            // If the identity is not found, return None permission
            // to prevent leaking information
            error!(?resource_id, "Identity not found");
            return Err(ArunaError::Unauthorized);
        };
        let mut highest_perm: Option<u32> = None;
        let mut queue = VecDeque::new();
        queue.push_back((*node_idx, u32::MAX));
        while let Some((idx, current_perm)) = queue.pop_front() {
            // Iterate over all incoming edges
            for edge in self.graph.edges_directed(idx, Incoming) {
                match edge.weight() {
                    // If the edge is a "permission related" edge
                    &HAS_PART | &OWNS_PROJECT | &SHARES_PERMISSION => {
                        queue.push_back((edge.source(), current_perm));
                        if &edge.source() == target_idx {
                            if let Some(perm) = highest_perm.as_mut() {
                                if current_perm > *perm {
                                    *perm = current_perm;
                                }
                            } else {
                                highest_perm = Some(current_perm)
                            }
                        }
                    }
                    // If the edge is an explicit permission edge
                    got_perm @ PERMISSION_NONE..=PERMISSION_ADMIN => {
                        let perm_possible = min(*got_perm, current_perm);

                        queue.push_back((edge.source(), perm_possible));

                        if &edge.source() == target_idx {
                            if let Some(perm) = highest_perm.as_mut() {
                                if perm_possible > *perm {
                                    *perm = perm_possible;
                                }
                            } else {
                                highest_perm = Some(perm_possible)
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        match highest_perm {
            None => {
                error!("No valid permission path found");
                Err(ArunaError::Forbidden("Permission denied".to_string()))
            }
            Some(p) => Ok(Permission::try_from(p).inspect_err(logerr!())?),
        }
    }

    pub async fn add_relation(
        &mut self,
        source: NodeVariantId,
        target: NodeVariantId,
        edge_type: EdgeType,
        db_env: Env,
    ) -> Result<(), ArunaError> {
        let source_idx = self
            .node_idx
            .get(source.get_ref())
            .ok_or_else(|| ArunaError::InvalidParameter {
                name: source.get_ref().to_string(),
                error: "Relation source does not exist".to_string(),
            })
            .inspect_err(logerr!())?;
        let target_idx = self
            .node_idx
            .get(target.get_ref())
            .ok_or_else(|| ArunaError::InvalidParameter {
                name: target.get_ref().to_string(),
                error: "Relation target does not exist".to_string(),
            })
            .inspect_err(logerr!())?;

        let relation = RawRelation {
            source,
            target,
            edge_type,
        };

        // Persist
        let mut wtxn = db_env.write_txn().inspect_err(logerr!())?;
        let db: RelationDb = db_env
            .create_database(&mut wtxn, Some(RELATION_DB_NAME))
            .inspect_err(logerr!())?;
        db.put_with_flags(&mut wtxn, PutFlags::NO_OVERWRITE, &relation, &())
            .inspect_err(logerr!())?;
        wtxn.commit().inspect_err(logerr!())?;

        // Store in graph
        self.graph.add_edge(*source_idx, *target_idx, edge_type);
        Ok(())
    }

    pub fn add_node(&mut self, node: NodeVariantId) {
        let id = *node.get_ref();
        let idx = self.graph.add_node(node);
        self.node_idx.insert(id, idx);
    }

    pub fn get_requester_from_aruna_token(&self, token_id: Ulid) -> Option<Requester> {
        let idx = self.node_idx.get(&token_id)?;

        for edge in self.graph.edges_directed(*idx, Outgoing) {
            match *edge.weight() {
                PERMISSION_NONE..=SHARES_PERMISSION => {
                    match self.graph.node_weight(edge.target()) {
                        Some(NodeVariantId::User(id)) => {
                            return Some(Requester::User {
                                user_id: *id,
                                auth_method: crate::requests::transaction::AuthMethod::Aruna(
                                    token_id,
                                ),
                            })
                        }
                        Some(NodeVariantId::ServiceAccount(service_account_id)) => {
                            let service_account_idx = self.node_idx.get(&token_id)?;
                            let group_id = self
                                .graph
                                .edges_directed(*service_account_idx, Outgoing)
                                .find_map(|relation| {
                                    if let PERMISSION_NONE..=PERMISSION_ADMIN = *relation.weight() {
                                        //PERMISSION_NONE..=PERMISSION_ADMIN =>
                                        self.graph
                                            .node_weight(edge.target())
                                            .map(|id| *id.get_ref())
                                    } else {
                                        None
                                    }
                                })?;
                            return Some(Requester::ServiceAccount {
                                service_account_id: *service_account_id,
                                token_id,
                                group_id,
                            });
                        }
                        _ => continue,
                    }
                }
                OWNED_BY_USER => {
                    return Some(Requester::User {
                        user_id: self
                            .graph
                            .node_weight(edge.target())
                            .map(|id| *id.get_ref())?,
                        auth_method: crate::requests::transaction::AuthMethod::Aruna(token_id),
                    });
                }
                _ => continue,
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        graph::{ArunaGraph, NodeVariantId},
        models::Permission,
    };
    use ulid::Ulid;

    #[test]
    fn test_get_parents() {
        let mut viewstore = ArunaGraph::new();
        let parent_id = Ulid::new();
        let child_id = Ulid::new();
        viewstore.add_node(NodeVariantId::Resource(parent_id));
        viewstore.add_node(NodeVariantId::Resource(child_id));
        viewstore.graph.add_edge(
            viewstore.node_idx[&parent_id],
            viewstore.node_idx[&child_id],
            0,
        );

        let parents = viewstore.get_parents(&child_id);
        assert_eq!(parents.len(), 1);
        assert_eq!(parents[0], parent_id);
    }

    #[test]
    fn get_permissions() {
        let mut viewstore = ArunaGraph::new();
        let object_ulid = Ulid::new();
        // Create a graph
        // Object <--0-- Folder <--1-- Project <--2-- Group <--3-- User <--4-- Token
        let object_idx = viewstore
            .graph
            .add_node(NodeVariantId::Resource(object_ulid));
        let folder_idx = viewstore
            .graph
            .add_node(NodeVariantId::Resource(Ulid::new()));
        viewstore.graph.add_edge(folder_idx, object_idx, 0); // 0
        let project_idx = viewstore
            .graph
            .add_node(NodeVariantId::Resource(Ulid::new()));
        viewstore.graph.add_edge(project_idx, folder_idx, 0); // 1
        let group_idx = viewstore.graph.add_node(NodeVariantId::Group(Ulid::new()));
        viewstore.graph.add_edge(group_idx, project_idx, 1); // 2
        let user_idx = viewstore.graph.add_node(NodeVariantId::User(Ulid::new()));
        viewstore.graph.add_edge(user_idx, group_idx, 3); // 3 Permission Read
        let token_ulid = Ulid::new();
        let token_idx = viewstore.graph.add_node(NodeVariantId::Token(token_ulid));
        viewstore.graph.add_edge(token_idx, user_idx, 4); // 4 Permission Append

        viewstore.node_idx.insert(object_ulid, object_idx);
        viewstore.node_idx.insert(token_ulid, token_idx);

        let permission = viewstore
            .get_permissions(&object_ulid, &token_ulid)
            .unwrap();
        assert_eq!(permission, Permission::Read);
    }
}
