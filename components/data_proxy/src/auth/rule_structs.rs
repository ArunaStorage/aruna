use crate::structs::Object;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserRuleInfo {
    pub user_id: String,
    pub attributes: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectHierarchyRuleInfo {
    pub object: Option<Object>,
    pub dataset: Option<Object>,
    pub collection: Option<Object>,
    pub project: Object,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestInfo {
    pub method: String,
    pub header: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleInfo {
    pub id: String,
    pub expires: i64,
}

/// ------- INPUTS ---------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RootRuleInput {
    pub user: UserRuleInfo,
    pub request: RequestInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectRuleInput {
    pub user: UserRuleInfo,
    pub object_hierarchy: ObjectHierarchyRuleInfo,
    pub request: RequestInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageObjectRuleInput {
    pub user: UserRuleInfo,
    pub object: Object,
    pub parents: Vec<Object>,
    pub request: RequestInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleRuleInput {
    pub user: UserRuleInfo,
    pub objects: Vec<Object>,
    pub request: RequestInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationIncomingRuleInput {
    pub objects: Vec<Object>,
    pub target_proxy_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationOutgoingRuleInput {
    pub objects: Object,
    pub target_proxy_id: String,
}
