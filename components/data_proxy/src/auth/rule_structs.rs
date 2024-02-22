use crate::structs::Object;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserPolicyInfo {
    pub user_id: String,
    pub attributes: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectHierarchyPolicyInfo {
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
pub struct PolicyInput {
    pub user: UserPolicyInfo,
    pub object_hierarchy: ObjectHierarchyPolicyInfo,
    pub request: RequestInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectsPolicyInput {
    pub user: UserPolicyInfo,
    pub object: Object,
    pub parents: Vec<Object>,
    pub request: RequestInfo,
}
