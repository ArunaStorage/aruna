use crate::structs::Object;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use anyhow::Result;
use anyhow::anyhow;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserRuleInfo {
    pub user_id: String,
    pub permissions: Vec<(String, String)>,
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
    pub bundle: BundleInfo,
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

#[derive(Debug, Default)]
pub struct RootRuleInputBuilder {
    user_id: String,
    permissions: Vec<(String, String)>,
    attributes: HashMap<String, String>,
    method: String,
    header: HashMap<String, String>,
}

impl RootRuleInputBuilder {

    pub fn new() -> Self {
        Self::default()
    }

    pub fn user_id(mut self, user_id: String) -> Self {
        self.user_id = user_id;
        self
    }

    pub fn permissions(mut self, permissions: Vec<(String, String)>) -> Self {
        self.permissions = permissions;
        self
    }

    pub fn attributes(mut self, attributes: HashMap<String, String>) -> Self {
        self.attributes = attributes;
        self
    }

    pub fn method(mut self, method: String) -> Self {
        self.method = method;
        self
    }

    pub fn header(mut self, header: HashMap<String, String>) -> Self {
        self.header = header;
        self
    }

    pub fn build(self) -> Result<RootRuleInput> {
        if self.user_id.is_empty() {
            return Err(anyhow!("user_id is required"));
        }

        if self.method.is_empty() {
            return Err(anyhow!("method is required"));
        }

        Ok(RootRuleInput {
            user: UserRuleInfo {
                user_id: self.user_id,
                permissions: self.permissions,
                attributes: self.attributes,
            },
            request: RequestInfo {
                method: self.method,
                header: self.header,
            },
        })
    }
}

#[derive(Debug, Default)]
pub struct ObjectRuleInputBuilder {
    user_id: String,
    permissions: Vec<(String, String)>,
    attributes: HashMap<String, String>,
    method: String,
    header: HashMap<String, String>,
    object: Option<Object>,
    dataset: Option<Object>,
    collection: Option<Object>,
    project: Option<Object>,
}

impl ObjectRuleInputBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn user_id(mut self, user_id: String) -> Self {
        self.user_id = user_id;
        self
    }

    pub fn permissions(mut self, permissions: Vec<(String, String)>) -> Self {
        self.permissions = permissions;
        self
    }

    pub fn attributes(mut self, attributes: HashMap<String, String>) -> Self {
        self.attributes = attributes;
        self
    }

    pub fn method(mut self, method: String) -> Self {
        self.method = method;
        self
    }

    pub fn header(mut self, header: HashMap<String, String>) -> Self {
        self.header = header;
        self
    }

    pub fn object(mut self, object: Option<Object>) -> Self {
        self.object = object;
        self
    }

    pub fn dataset(mut self, dataset: Option<Object>) -> Self {
        self.dataset = dataset;
        self
    }

    pub fn collection(mut self, collection: Option<Object>) -> Self {
        self.collection = collection;
        self
    }

    pub fn project(mut self, project: Object) -> Self {
        self.project = Some(project);
        self
    }

    pub fn build(self) -> Result<ObjectRuleInput> {

        if self.user_id.is_empty() {
            return Err(anyhow!("user_id is required"));
        }

        if self.method.is_empty() {
            return Err(anyhow!("method is required"));
        }

        if self.project.is_none() {
            return Err(anyhow!("project is required"));
        }

        Ok(ObjectRuleInput {
            user: UserRuleInfo {
                user_id: self.user_id,
                permissions: self.permissions,
                attributes: self.attributes,
            },
            object_hierarchy: ObjectHierarchyRuleInfo {
                object: self.object,
                dataset: self.dataset,
                collection: self.collection,
                project: self.project.ok_or_else(|| anyhow!("project is required"))?,
            },
            request: RequestInfo {
                method: self.method,
                header: self.header,
            },
        })
    }
}

#[derive(Debug, Default)]
pub struct PackageObjectRuleInputBuilder {
    user_id: String,
    permissions: Vec<(String, String)>,
    attributes: HashMap<String, String>,
    method: String,
    header: HashMap<String, String>,
    object: Option<Object>,
    parents: Vec<Object>,
}

impl PackageObjectRuleInputBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn user_id(mut self, user_id: String) -> Self {
        self.user_id = user_id;
        self
    }

    pub fn permissions(mut self, permissions: Vec<(String, String)>) -> Self {
        self.permissions = permissions;
        self
    }

    pub fn attributes(mut self, attributes: HashMap<String, String>) -> Self {
        self.attributes = attributes;
        self
    }

    pub fn method(mut self, method: String) -> Self {
        self.method = method;
        self
    }

    pub fn header(mut self, header: HashMap<String, String>) -> Self {
        self.header = header;
        self
    }

    pub fn object(mut self, object: Option<Object>) -> Self {
        self.object = object;
        self
    }

    pub fn parents(mut self, parents: Vec<Object>) -> Self {
        self.parents = parents;
        self
    }

    pub fn build(self) -> Result<PackageObjectRuleInput> {
        if self.user_id.is_empty() {
            return Err(anyhow!("user_id is required"));
        }

        if self.method.is_empty() {
            return Err(anyhow!("method is required"));
        }

        if self.object.is_none() {
            return Err(anyhow!("object is required"));
        }

        Ok(PackageObjectRuleInput {
            user: UserRuleInfo {
                user_id: self.user_id,
                permissions: self.permissions,
                attributes: self.attributes,
            },
            object: self.object.ok_or_else(|| anyhow!("object is required"))?,
            parents: self.parents,
            request: RequestInfo {
                method: self.method,
                header: self.header,
            },
        })
    }
}

#[derive(Debug, Default)]
pub struct BundleRuleInputBuilder {
    user_id: String,
    permissions: Vec<(String, String)>,
    attributes: HashMap<String, String>,
    method: String,
    header: HashMap<String, String>,
    objects: Vec<Object>,
    bundle_id: String,
    expires: Option<i64>,
}

impl BundleRuleInputBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn user_id(mut self, user_id: String) -> Self {
        self.user_id = user_id;
        self
    }

    pub fn permissions(mut self, permissions: Vec<(String, String)>) -> Self {
        self.permissions = permissions;
        self
    }

    pub fn attributes(mut self, attributes: HashMap<String, String>) -> Self {
        self.attributes = attributes;
        self
    }

    pub fn method(mut self, method: String) -> Self {
        self.method = method;
        self
    }

    pub fn header(mut self, header: HashMap<String, String>) -> Self {
        self.header = header;
        self
    }

    pub fn objects(mut self, objects: Vec<Object>) -> Self {
        self.objects = objects;
        self
    }

    pub fn bundle_id(mut self, bundle_id: String) -> Self {
        self.bundle_id = bundle_id;
        self
    }

    pub fn expires(mut self, expires: Option<i64>) -> Self {
        self.expires = expires;
        self
    }

    pub fn build(self) -> Result<BundleRuleInput> {
        if self.user_id.is_empty() {
            return Err(anyhow!("user_id is required"));
        }

        if self.method.is_empty() {
            return Err(anyhow!("method is required"));
        }

        if self.objects.is_empty() {
            return Err(anyhow!("objects is required"));
        }

        if self.bundle_id.is_empty() {
            return Err(anyhow!("bundle_id is required"));
        }

        Ok(BundleRuleInput {
            user: UserRuleInfo {
                user_id: self.user_id,
                permissions: self.permissions,
                attributes: self.attributes,
            },
            objects: self.objects,
            request: RequestInfo {
                method: self.method,
                header: self.header,
            },
            bundle: BundleInfo {
                id: self.bundle_id,
                expires: self.expires.ok_or_else(|| anyhow!("expires is required"))?,
            },
        })
    }
}


#[derive(Debug, Default)]
pub struct ReplicationIncomingRuleInputBuilder {
    objects: Vec<Object>,
    target_proxy_id: String,
}

impl ReplicationIncomingRuleInputBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn objects(mut self, objects: Vec<Object>) -> Self {
        self.objects = objects;
        self
    }

    pub fn target_proxy_id(mut self, target_proxy_id: String) -> Self {
        self.target_proxy_id = target_proxy_id;
        self
    }

    pub fn build(self) -> Result<ReplicationIncomingRuleInput> {
        if self.objects.is_empty() {
            return Err(anyhow!("objects is required"));
        }

        if self.target_proxy_id.is_empty() {
            return Err(anyhow!("target_proxy_id is required"));
        }

        Ok(ReplicationIncomingRuleInput {
            objects: self.objects,
            target_proxy_id: self.target_proxy_id,
        })
    }
}

#[derive(Debug, Default)]
pub struct ReplicationOutgoingRuleInputBuilder {
    object: Object,
    target_proxy_id: String,
}

impl ReplicationOutgoingRuleInputBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn object(mut self, object: Object) -> Self {
        self.objects = object;
        self
    }

    pub fn target_proxy_id(mut self, target_proxy_id: String) -> Self {
        self.target_proxy_id = target_proxy_id;
        self
    }

    pub fn build(self) -> Result<ReplicationOutgoingRuleInput> {
        if self.object.is_empty() {
            return Err(anyhow!("objects is required"));
        }

        if self.target_proxy_id.is_empty() {
            return Err(anyhow!("target_proxy_id is required"));
        }

        Ok(ReplicationOutgoingRuleInput {
            objects: self.objects,
            target_proxy_id: self.target_proxy_id,
        })
    }
}