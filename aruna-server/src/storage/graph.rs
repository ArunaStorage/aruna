use std::{cmp::min, collections::VecDeque};

use crate::{
    error::ArunaError,
    logerr,
    models::{EdgeType, NodeVariant, Permission, RawRelation},
};
use heed::{types::SerdeBincode, Database, RoTxn};
use milli::{ObkvCodec, BEU32};
use petgraph::{
    visit::EdgeRef,
    Direction::{self, Incoming, Outgoing},
    Graph,
};
use tracing::error;

pub trait IndexHelper {
    fn as_u32(&self) -> u32;
}

impl IndexHelper for petgraph::graph::NodeIndex {
    fn as_u32(&self) -> u32 {
        self.index() as u32
    }
}

#[tracing::instrument(level = "trace", skip(rtxn, relations, documents))]
pub fn load_graph(
    rtxn: &RoTxn<'_>,
    relations: &Database<BEU32, SerdeBincode<RawRelation>>,
    documents: &Database<BEU32, ObkvCodec>,
) -> Result<petgraph::graph::Graph<NodeVariant, EdgeType>, ArunaError> {
    let mut graph = petgraph::graph::Graph::new();

    documents.iter(rtxn)?.try_for_each(|entry| {
        let (_id, obkv) = entry.map_err(|e| ArunaError::ServerError(format!("{e}")))?;

        let node_type = obkv.get(1).ok_or_else(|| {
            ArunaError::ServerError("Node type not found in document".to_string())
        })?;

        let value = serde_json::from_slice::<serde_json::Number>(node_type).map_err(|_| {
            ArunaError::ConversionError {
                from: "&[u8]".to_string(),
                to: "serde_json::Number".to_string(),
            }
        })?;
        let variant = NodeVariant::try_from(value)?;

        graph.add_node(variant);

        Ok::<(), ArunaError>(())
    })?;

    relations.iter(&rtxn)?.try_for_each(|entry| {
        let (_idx, relation) = entry.map_err(|e| ArunaError::ServerError(format!("{e}")))?;
        graph.add_edge(
            relation.source.into(),
            relation.target.into(),
            relation.edge_type,
        );
        Ok::<(), ArunaError>(())
    })?;
    Ok(graph)
}

pub fn get_relations(
    graph: &Graph<NodeVariant, EdgeType>,
    idx: u32,
    filter: &[EdgeType],
    direction: Direction,
) -> Vec<RawRelation> {
    let idx = idx.into();
    graph
        .edges_directed(idx, direction)
        .filter_map(|e| {
            if filter.contains(e.weight()) {
                match direction {
                    Direction::Outgoing => Some(RawRelation {
                        target: e.target().as_u32(),
                        source: idx.as_u32(),
                        edge_type: *e.weight(),
                    }),
                    Direction::Incoming => Some(RawRelation {
                        source: e.target().as_u32(),
                        target: idx.as_u32(),
                        edge_type: *e.weight(),
                    }),
                }
            } else {
                None
            }
        })
        .collect()
}

pub fn get_relatives(
    graph: &Graph<NodeVariant, EdgeType>,
    idx: u32,
    direction: Direction,
) -> Vec<u32> {
    let idx = idx.into();
    graph
        .edges_directed(idx, direction)
        .filter_map(|e| {
            if e.weight() == &0 {
                match graph.node_weight(e.source()) {
                    None => None,
                    Some(variant) => match variant {
                        NodeVariant::ResourceProject
                        | NodeVariant::ResourceFolder
                        | NodeVariant::ResourceObject => Some(idx.as_u32()),
                        _ => None,
                    },
                }
            } else {
                None
            }
        })
        .collect()
}

pub fn get_parents(graph: &Graph<NodeVariant, EdgeType>, idx: u32) -> Vec<u32> {
    get_relatives(graph, idx, Incoming)
}

pub fn get_children(graph: &Graph<NodeVariant, EdgeType>, idx: u32) -> Vec<u32> {
    get_relatives(graph, idx, Outgoing)
}

/// Perform a breadth-first search for incoming edges to find the highest permission
/// 1. Start at the resource node with a theoretical maximum permission
/// 2. Iterate over all incoming edges
/// 3. If the edge is a colored as a "permission" edge add the source node with the minimum permission to the queue
/// 4. If the edge is a colored as a "hierarchy related non permission" edge add the source node with the previous current perm to the queue
/// 5. If the source node is the target node, update the highest permission to the maximum of the current permission and the previous highest permission
#[tracing::instrument(level = "trace", skip(graph))]
pub fn get_permissions(
    graph: &Graph<NodeVariant, EdgeType>,
    resource_id: u32,
    identity: u32,
) -> Result<Permission, ArunaError> {
    use crate::constants::relation_types::*;
    // Resource could be: Group, User, Projects, Folder, Object || TODO: Realm, ServiceAccount, Hooks, etc.

    let mut highest_perm: Option<u32> = None;
    let mut queue = VecDeque::new();
    queue.push_back((resource_id.into(), u32::MAX));
    while let Some((idx, current_perm)) = queue.pop_front() {
        // Iterate over all incoming edges
        for edge in graph.edges_directed(idx, Incoming) {
            match edge.weight() {
                // If the edge is a "permission related" edge
                &HAS_PART | &OWNS_PROJECT | &SHARES_PERMISSION => {
                    queue.push_back((edge.source(), current_perm));
                    if edge.source().as_u32() == identity {
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

                    if edge.source().as_u32() == identity {
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

pub fn check_node_variant(
    graph: &Graph<NodeVariant, EdgeType>,
    idx: u32,
    variant: &NodeVariant,
) -> bool {
    graph.node_weight(idx.into()) == Some(variant)
}
