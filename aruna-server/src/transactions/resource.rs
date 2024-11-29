use super::{
    controller::Controller,
    request::{Request, Requester, SerializedResponse},
};
use crate::{
    constants::relation_types::{self},
    context::{BatchPermission, Context},
    error::ArunaError,
    logerr,
    models::{
        models::{DataLocation, NodeVariant, Resource, SyncingStatus},
        requests::{
            CreateProjectRequest, CreateProjectResponse, CreateResourceBatchRequest, CreateResourceBatchResponse, CreateResourceRequest, CreateResourceResponse, GetInner, GetResourcesRequest, GetResourcesResponse, Parent, ResourceUpdateRequests, ResourceUpdateResponses, UpdateResourceAuthorsResponse, UpdateResourceDescriptionResponse, UpdateResourceIdentifiersResponse, UpdateResourceLabelsResponse, UpdateResourceLicenseResponse, UpdateResourceNameResponse, UpdateResourceTitleResponse, UpdateResourceVisibilityResponse
        },
    },
    storage::{
        graph::{get_parent, get_related_user_or_groups, has_relation},
        store::{Store, WriteTxn},
    },
    transactions::request::WriteRequest,
};
use ahash::RandomState;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use tracing::{error, info};
use ulid::Ulid;

impl Request for CreateProjectRequest {
    type Response = CreateProjectResponse;
    fn get_context(&self) -> Context {
        Context::Permission {
            min_permission: crate::models::models::Permission::Write,
            source: self.group_id,
        }
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        let request_tx = CreateProjectRequestTx {
            req: self,
            project_id: Ulid::new(),
            requester: requester.ok_or_else(|| ArunaError::Unauthorized)?,
            created_at: Utc::now().timestamp_millis(),
        };

        let response = controller.transaction(Ulid::new().0, &request_tx).await?;

        Ok(bincode::deserialize(&response)?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateProjectRequestTx {
    req: CreateProjectRequest,
    requester: Requester,
    project_id: Ulid,
    created_at: i64,
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for CreateProjectRequestTx {
    async fn execute(
        &self,
        associated_event_id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, crate::error::ArunaError> {
        controller.authorize(&self.requester, &self.req).await?;

        let time = DateTime::from_timestamp_millis(self.created_at).ok_or_else(|| {
            ArunaError::ConversionError {
                from: "i64".to_string(),
                to: "Chrono::DateTime".to_string(),
            }
        })?;

        let mut project = Resource {
            id: self.project_id,
            name: self.req.name.clone(),
            description: self.req.description.clone(),
            title: self.req.title.clone(),
            revision: 0,
            variant: crate::models::models::ResourceVariant::Project,
            labels: self.req.labels.clone(),
            identifiers: self.req.identifiers.clone(),
            content_len: 0,
            count: 0,
            visibility: self.req.visibility.clone(),
            created_at: time,
            last_modified: time,
            authors: self.req.authors.clone(),
            license_tag: self.req.license_tag.clone(),
            locked: false,
            location: vec![], // TODO: Locations and DataProxies
            hashes: vec![],
        };

        let group_id = self.req.group_id;
        let realm_id = self.req.realm_id;

        let endpoint = self.req.data_endpoint.clone();

        let requester = self.requester.clone();

        let store = controller.get_store();
        Ok(tokio::task::spawn_blocking(move || {
            // Create project

            let mut wtxn = store.write_txn()?;

            // Get group idx
            let Some(group_idx) = store.get_idx_from_ulid(&group_id, wtxn.get_txn()) else {
                return Err(ArunaError::NotFound(group_id.to_string()));
            };
            // Get the realm
            let Some(realm_idx) = store.get_idx_from_ulid(&realm_id, wtxn.get_txn()) else {
                error!("Realm not found: {}", realm_id);
                return Err(ArunaError::NotFound(group_id.to_string()));
            };

            // Validate that the data endpoint is part of the realm
            let endpont_id = if let Some(data_endpoint) = endpoint {
                let Some(data_endpoint_idx) =
                    store.get_idx_from_ulid(&data_endpoint, wtxn.get_txn())
                else {
                    error!("Data endpoint not found: {}", data_endpoint);
                    return Err(ArunaError::NotFound(data_endpoint.to_string()));
                };

                if !has_relation(
                    wtxn.get_ro_graph(),
                    realm_idx,
                    data_endpoint_idx,
                    relation_types::REALM_USES_COMPONENT,
                ) {
                    error!("Realm does not use this data endpoint");
                    return Err(ArunaError::InvalidParameter {
                        name: "data_endpoint".to_string(),
                        error: "Realm does not use this data endpoint".to_string(),
                    });
                }

                let status = requester
                    .get_impersonator()
                    .map(|impersonator| {
                        if impersonator == data_endpoint {
                            SyncingStatus::Finished
                        } else {
                            SyncingStatus::Pending
                        }
                    })
                    .unwrap_or(SyncingStatus::Pending);

                project.location = vec![DataLocation {
                    endpoint_id: data_endpoint,
                    status,
                }];

                Some(data_endpoint_idx)
            } else {
                None
            };

            // Check that name is unique
            if !store
                .filtered_universe(
                    Some(&format!("name='{}' AND variant=0", project.name)),
                    wtxn.get_txn(),
                )?
                .is_empty()
            {
                return Err(ArunaError::ConflictParameter {
                    name: "name".to_string(),
                    error: "Project with this name already exists".to_string(),
                });
            }

            // Create project
            let project_idx = store.create_node(&mut wtxn, &project)?;

            // Add relation group --OWNS_PROJECT--> project
            store.create_relation(
                &mut wtxn,
                group_idx,
                project_idx,
                relation_types::OWNS_PROJECT,
            )?;

            // Add relation realm --PROJECT_PART_OF_REALM--> project
            // TODO: Check if this should be a relation or a field
            store.create_relation(
                &mut wtxn,
                realm_idx,
                project_idx,
                relation_types::PROJECT_PART_OF_REALM,
            )?;

            match project.visibility {
                crate::models::models::VisibilityClass::Public
                | crate::models::models::VisibilityClass::PublicMetadata => {
                    store.add_public_resources_universe(&mut wtxn, &[project_idx])?;
                }
                crate::models::models::VisibilityClass::Private => {
                    store.add_read_permission_universe(&mut wtxn, group_idx, &[project_idx])?;
                }
            };

            // Notify the endpoint if user wants to add a data endpoint
            let additional_affected = if let Some(endpoint_idx) = endpont_id {
                vec![endpoint_idx]
            } else {
                vec![]
            };

            // Affected nodes: Group, Realm, Project
            wtxn.commit(
                associated_event_id,
                &[realm_idx, group_idx, project_idx],
                &additional_affected,
            )?;
            // Create admin group, add user to admin group
            Ok::<_, ArunaError>(bincode::serialize(&CreateProjectResponse {
                resource: project,
            })?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}

impl Request for CreateResourceRequest {
    type Response = CreateResourceResponse;
    fn get_context(&self) -> Context {
        Context::Permission {
            min_permission: crate::models::models::Permission::Write,
            source: self.parent_id,
        }
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        // Check the variant before running consensus
        if self.variant == crate::models::models::ResourceVariant::Project {
            return Err(ArunaError::InvalidParameter {
                name: "variant".to_string(),
                error: "Wrong request, use CreateProjectRequest to create projects".to_string(),
            });
        }

        let request_tx = CreateResourceRequestTx {
            req: self,
            resource_id: Ulid::new(),
            requester: requester
                .ok_or_else(|| ArunaError::Unauthorized)
                .inspect_err(logerr!())?,
            created_at: Utc::now().timestamp_millis(),
        };

        let response = controller.transaction(Ulid::new().0, &request_tx).await?;

        Ok(bincode::deserialize(&response).inspect_err(logerr!())?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateResourceRequestTx {
    req: CreateResourceRequest,
    requester: Requester,
    resource_id: Ulid,
    created_at: i64,
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for CreateResourceRequestTx {
    async fn execute(
        &self,
        associated_event_id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, crate::error::ArunaError> {
        info!("Executing CreateResourceRequestTx");

        controller.authorize(&self.requester, &self.req).await?;

        let time = DateTime::from_timestamp_millis(self.created_at).ok_or_else(|| {
            ArunaError::ConversionError {
                from: "i64".to_string(),
                to: "Chrono::DateTime".to_string(),
            }
        })?;

        let resource = Resource {
            id: self.resource_id,
            name: self.req.name.clone(),
            description: self.req.description.clone(),
            title: self.req.title.clone(),
            revision: 0,
            variant: self.req.variant.clone(),
            labels: self.req.labels.clone(),
            identifiers: self.req.identifiers.clone(),
            content_len: 0,
            count: 0,
            visibility: self.req.visibility.clone(),
            created_at: time,
            last_modified: time,
            authors: self.req.authors.clone(),
            license_tag: self.req.license_tag.clone(),
            locked: false,
            location: vec![], // TODO: Locations and DataProxies
            hashes: vec![],
        };

        let parent_id = self.req.parent_id;

        let store = controller.get_store();
        Ok(tokio::task::spawn_blocking(move || {
            // Create resource

            let mut wtxn = store.write_txn()?;

            let Some(parent_idx) = store.get_idx_from_ulid(&parent_id, wtxn.get_txn()) else {
                return Err(ArunaError::NotFound(parent_id.to_string()));
            };

            // Make sure that the parent is a folder or project
            let raw_parent_node = store
                .get_raw_node(wtxn.get_txn(), parent_idx)
                .expect("Idx exist but no node in documents -> corrupted database");

            let variant: NodeVariant = serde_json::from_slice::<u8>(
                raw_parent_node
                    .get(1)
                    .expect("Missing variant -> corrupted database"),
            )
            .inspect_err(logerr!())?
            .try_into()
            .inspect_err(logerr!())?;

            if !matches!(
                variant,
                NodeVariant::ResourceProject | NodeVariant::ResourceFolder
            ) {
                return Err(ArunaError::InvalidParameter {
                    name: "parent_id".to_string(),
                    error: "Wrong parent, must be folder or project".to_string(),
                });
            }

            // Check for unique name 1. Get all resources with the same name
            // 0 Project, 1 Folder, 2 Object -> < 3
            let universe = store.filtered_universe(
                Some(&format!("name='{}' AND variant < 3", resource.name)),
                wtxn.get_txn(),
            )?;

            // 2. Check if any of the resources in the universe have the same parent as a parent
            for idx in universe {
                // We need to use this because we have a lock on the store
                if get_parent(wtxn.get_ro_graph(), idx) == Some(parent_idx) {
                    return Err(ArunaError::ConflictParameter {
                        name: "name".to_string(),
                        error: "Resource with this name already exists in this hierarchy"
                            .to_string(),
                    });
                }
            }

            // Create resource
            let resource_idx = store.create_node(&mut wtxn, &resource)?;

            // Add relation parent --HAS_PART--> resource
            store.create_relation(
                &mut wtxn,
                parent_idx,
                resource_idx,
                relation_types::HAS_PART,
            )?;

            match resource.visibility {
                crate::models::models::VisibilityClass::Public
                | crate::models::models::VisibilityClass::PublicMetadata => {
                    store.add_public_resources_universe(&mut wtxn, &[resource_idx])?;
                }
                crate::models::models::VisibilityClass::Private => {
                    let groups = get_related_user_or_groups(wtxn.get_ro_graph(), parent_idx)?;
                    for group_idx in groups {
                        store.add_read_permission_universe(
                            &mut wtxn,
                            group_idx,
                            &[resource_idx],
                        )?;
                    }
                }
            };

            // Affected nodes: Group, Project
            wtxn.commit(associated_event_id, &[parent_idx, resource_idx], &[])?;
            Ok::<_, ArunaError>(bincode::serialize(&CreateResourceResponse { resource })?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}

impl Request for CreateResourceBatchRequest {
    type Response = CreateResourceBatchResponse;
    fn get_context(&self) -> Context {
        Context::PermissionBatch(
            self.resources
                .iter()
                .filter_map(|r| {
                    if let Parent::ID(id) = r.parent {
                        Some(BatchPermission {
                            min_permission: crate::models::models::Permission::Write,
                            source: id,
                        })
                    } else {
                        None
                    }
                })
                .collect(),
        )
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        // Check the variant before running consensus
        let mut resource_ids = Vec::new();
        for resource in &self.resources {
            if resource.variant == crate::models::models::ResourceVariant::Project {
                return Err(ArunaError::InvalidParameter {
                    name: "variant".to_string(),
                    error: "Wrong request, use CreateProjectRequest to create projects".to_string(),
                });
            }
            resource_ids.push(Ulid::new());
        }

        let request_tx = CreateResourceBatchRequestTx {
            req: self,
            resource_ids,
            requester: requester
                .ok_or_else(|| ArunaError::Unauthorized)
                .inspect_err(logerr!())?,
            created_at: Utc::now().timestamp_millis(),
        };

        let response = controller.transaction(Ulid::new().0, &request_tx).await?;

        Ok(bincode::deserialize(&response).inspect_err(logerr!())?)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CreateResourceBatchRequestTx {
    req: CreateResourceBatchRequest,
    requester: Requester,
    resource_ids: Vec<Ulid>,
    created_at: i64,
}

impl CreateResourceBatchRequestTx {
    fn parse_resources(
        &self,
    ) -> Result<
        (
            HashMap<Ulid, Vec<Resource>, RandomState>, // Only for checking existing parents
            Vec<(Ulid, Resource)>,                     // Includes all resources with their parents
        ),
        ArunaError,
    > {
        let time = DateTime::from_timestamp_millis(self.created_at).ok_or_else(|| {
            ArunaError::ConversionError {
                from: "i64".to_string(),
                to: "Chrono::DateTime".to_string(),
            }
        })?;

        //let mut resources = Vec::new();
        let mut existing: HashMap<Ulid, Vec<Resource>, RandomState> = HashMap::default();
        let mut new: HashMap<u32, Vec<Resource>, RandomState> = HashMap::default();
        let mut all = Vec::new();

        for (idx, id) in self.resource_ids.iter().enumerate() {
            let res = self.req.resources.get(idx).cloned().ok_or_else(|| {
                ArunaError::DeserializeError(
                    "No entry for id found in CreateResourceBatchRequest".to_string(),
                )
            })?;

            let resource = Resource {
                id: *id,
                name: res.name.clone(),
                description: res.description,
                title: res.title,
                revision: 0,
                variant: res.variant,
                labels: res.labels,
                identifiers: res.identifiers,
                content_len: 0,
                count: 0,
                visibility: res.visibility,
                created_at: time,
                last_modified: time,
                authors: res.authors,
                license_tag: res.license_tag,
                locked: false,
                location: vec![], // TODO: Locations and DataProxies
                hashes: vec![],
            };
            match res.parent {
                Parent::ID(parent_id) => {
                    let entry = existing.entry(parent_id).or_default();
                    if entry.iter().find(|r| &r.name == &res.name).is_some() {
                        return Err(ArunaError::ConflictParameter {
                            name: "name".to_string(),
                            error: "Name is not unique in parent resource".to_string(),
                        });
                    } else {
                        entry.push(resource.clone());
                        all.push((parent_id, resource));
                    }
                }
                Parent::Idx(parent_idx) => {
                    let entry = new.entry(parent_idx).or_default();
                    if entry.iter().find(|r| r.name == res.name).is_some() {
                        return Err(ArunaError::ConflictParameter {
                            name: "name".to_string(),
                            error: "Name is not unique in parent resource".to_string(),
                        });
                    } else {
                        let parent_id =
                            self.resource_ids.get(parent_idx as usize).ok_or_else(|| {
                                ArunaError::InvalidParameter {
                                    name: "parent".to_string(),
                                    error: "Parent not found in idx".to_string(),
                                }
                            })?;
                        entry.push(resource.clone());
                        all.push((*parent_id, resource));
                    }
                }
            }
        }

        let mut to_check: Vec<u32> = new.keys().cloned().collect();
        to_check.sort();
        let mut checked: Vec<u32> = Vec::new();
        for new_parent in &to_check {
            let entry = self
                .req
                .resources
                .get(*new_parent as usize)
                .ok_or_else(|| ArunaError::InvalidParameter {
                    name: "parent".to_string(),
                    error: "Parent not found in resources".to_string(),
                })?;
            match entry.parent {
                Parent::ID(_) => {
                    checked.push(*new_parent);
                }
                Parent::Idx(idx) => {
                    if checked.contains(&idx) {
                        checked.push(*new_parent);
                    } else {
                        return Err(ArunaError::InvalidParameter {
                            name: "parent".to_string(),
                            error: "Parents not linked".to_string(),
                        });
                    }
                }
            }
        }
        if to_check.len() != checked.len() {
            dbg!(&to_check);
            return Err(ArunaError::InvalidParameter {
                name: "parent".to_string(),
                error: "New parent does not connect to existing resource".to_string(),
            });
        }
        Ok((existing, all))
    }
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for CreateResourceBatchRequestTx {
    async fn execute(
        &self,
        associated_event_id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, crate::error::ArunaError> {
        info!("Executing CreateResourceRequestTx");

        controller.authorize(&self.requester, &self.req).await?;

        //let transaction = self.clone();
        let (parents_to_check, resources) = self.parse_resources()?;

        let store = controller.get_store();
        Ok(tokio::task::spawn_blocking(move || {
            // Create resource

            let mut wtxn = store.write_txn()?;

            // Check naming conflicts and if existing parents exist
            for (parent_id, subresources) in parents_to_check.into_iter() {
                let Some(parent_idx) = store.get_idx_from_ulid(&parent_id, wtxn.get_txn()) else {
                    return Err(ArunaError::NotFound(parent_id.to_string()));
                };

                // Make sure that the parent is a folder or project
                let raw_parent_node = store
                    .get_raw_node(wtxn.get_txn(), parent_idx)
                    .expect("Idx exist but no node in documents -> corrupted database");

                let variant: NodeVariant = serde_json::from_slice::<u8>(
                    raw_parent_node
                        .get(1)
                        .expect("Missing variant -> corrupted database"),
                )
                .inspect_err(logerr!())?
                .try_into()
                .inspect_err(logerr!())?;

                if !matches!(
                    variant,
                    NodeVariant::ResourceProject | NodeVariant::ResourceFolder
                ) {
                    return Err(ArunaError::InvalidParameter {
                        name: "parent_id".to_string(),
                        error: "Wrong parent, must be folder or project".to_string(),
                    });
                }

                for resource in &subresources {
                    // Check for unique name 1. Get all resources with the same name
                    // 0 Project, 1 Folder, 2 Object -> < 3
                    let universe = store.filtered_universe(
                        Some(&format!("name='{}' AND variant < 3", resource.name)),
                        wtxn.get_txn(),
                    )?;

                    // 2. Check if any of the resources in the universe have the same parent as a parent
                    for idx in universe {
                        // We need to use this because we have a lock on the store
                        if get_parent(wtxn.get_ro_graph(), idx) == Some(parent_idx) {
                            return Err(ArunaError::ConflictParameter {
                                name: "name".to_string(),
                                error: "Resource with this name already exists in this hierarchy"
                                    .to_string(),
                            });
                        }
                    }
                }
            }

            let resource_idx =
                store.create_nodes_batch(&mut wtxn, resources.iter().map(|(_, r)| r).collect())?;

            let mut affected = vec![];
            for (parent_id, resource) in &resources {
                let Some(parent_idx) = store.get_idx_from_ulid(&parent_id, wtxn.get_txn()) else {
                    return Err(ArunaError::NotFound(parent_id.to_string()));
                };
                let (_, idx) = resource_idx
                    .iter()
                    .find(|(id, _)| id == &resource.id)
                    .ok_or_else(|| ArunaError::DeserializeError("Idx not found".to_string()))?;

                // Add relation parent --HAS_PART--> resource
                store.create_relation(&mut wtxn, parent_idx, *idx, relation_types::HAS_PART)?;
                affected.push(*idx);

                // Add resource to related universes
                match resource.visibility {
                    crate::models::models::VisibilityClass::Public
                    | crate::models::models::VisibilityClass::PublicMetadata => {
                        store.add_public_resources_universe(&mut wtxn, &[*idx])?;
                    }
                    crate::models::models::VisibilityClass::Private => {
                        let groups = get_related_user_or_groups(wtxn.get_ro_graph(), parent_idx)?;
                        for group_idx in groups {
                            store.add_read_permission_universe(&mut wtxn, group_idx, &[*idx])?;
                        }
                    }
                };
            }

            // Affected nodes: Group, Project
            wtxn.commit(associated_event_id, &affected, &[])?;
            Ok::<_, ArunaError>(bincode::serialize(&CreateResourceBatchResponse {
                resources: resources.into_iter().map(|(_, r)| r).collect(),
            })?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}

impl Request for GetResourcesRequest {
    type Response = GetResourcesResponse;
    fn get_context(&self) -> Context {
        Context::PermissionBatch(
            self.ids
                .iter()
                .map(|id| BatchPermission {
                    min_permission: crate::models::models::Permission::Read,
                    source: *id,
                })
                .collect(),
        )
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        info!("Executing GetResourceRequest");

        let public = if let Some(requester) = requester {
            controller.authorize(&requester, &self).await?;
            false
        } else {
            true
        };

        let store = controller.get_store();
        let response = tokio::task::spawn_blocking(move || {
            let rtxn = store.read_txn()?;
            let mut resources = Vec::new();

            for id in &self.ids {
                let idx = store
                    .get_idx_from_ulid(&id, &rtxn)
                    .ok_or_else(|| ArunaError::NotFound(id.to_string()))?;

                let resource = store
                    .get_node::<Resource>(&rtxn, idx)
                    .ok_or_else(|| ArunaError::NotFound(id.to_string()))?;

                if public {
                    if !matches!(
                        resource.visibility,
                        crate::models::models::VisibilityClass::Public
                    ) {
                        return Err(ArunaError::Unauthorized);
                    }
                }
                resources.push(resource);
            }

            Ok::<_, ArunaError>(GetResourcesResponse { resources })
        })
        .await
        .map_err(|e| ArunaError::ServerError(e.to_string()))??;

        Ok(response)
    }
}

impl Request for ResourceUpdateRequests {
    type Response = ResourceUpdateResponses;
    fn get_context(&self) -> Context {
        Context::Permission {
            min_permission: crate::models::models::Permission::Write,
            source: self.get_id(),
        }
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        let request_tx = UpdateResourceTx {
            req: self,
            requester: requester.ok_or_else(|| ArunaError::Unauthorized)?,
            updated_at: Utc::now().timestamp_millis(),
        };
        let response = controller.transaction(Ulid::new().0, &request_tx).await?;

        Ok(bincode::deserialize(&response)?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateResourceTx {
    req: ResourceUpdateRequests,
    requester: Requester,
    updated_at: i64,
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for UpdateResourceTx {
    async fn execute(
        &self,
        associated_event_id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, crate::error::ArunaError> {
        controller.authorize(&self.requester, &self.req).await?;

        let time = DateTime::from_timestamp_millis(self.updated_at).ok_or_else(|| {
            ArunaError::ConversionError {
                from: "i64".to_string(),
                to: "Chrono::DateTime".to_string(),
            }
        })?;

        let resource_id = self.req.get_id();
        let request = self.req.clone();

        let store = controller.get_store();

        Ok(tokio::task::spawn_blocking(move || {
            let mut wtxn = store.write_txn()?;

            // Get resource idx
            let Some(resource_idx) = store.get_idx_from_ulid(&resource_id, wtxn.get_txn()) else {
                return Err(ArunaError::NotFound(resource_id.to_string()));
            };

            let Some(old_resource): Option<Resource> =
                store.get_node(&wtxn.get_txn(), resource_idx)
            else {
                return Err(ArunaError::NotFound(format!(
                    "Resource with id {resource_id} not found"
                )));
            };

            let map =
                parse_update_fields(&store, &mut wtxn, old_resource, resource_id, time, request.clone())?;

            store.update_node_field(&mut wtxn, resource_id, map)?;

            // Affected nodes: Group, Realm, Project
            let resource = store.get_node(wtxn.get_txn(), resource_idx).unwrap();
            println!("{resource:?}");
            let response = match request {
                ResourceUpdateRequests::Name(_) => {
                    ResourceUpdateResponses::Name(UpdateResourceNameResponse { resource })
                }
                ResourceUpdateRequests::Title(_) => {
                    ResourceUpdateResponses::Title(UpdateResourceTitleResponse { resource })
                }
                ResourceUpdateRequests::Description(_) => {
                    ResourceUpdateResponses::Description(UpdateResourceDescriptionResponse{ resource })
                }
                ResourceUpdateRequests::Visibility(_) => {
                    ResourceUpdateResponses::Visibility(UpdateResourceVisibilityResponse { resource })
                }
                ResourceUpdateRequests::License(_) => {
                    ResourceUpdateResponses::License(UpdateResourceLicenseResponse { resource })
                }
                ResourceUpdateRequests::Labels(_) => {
                    ResourceUpdateResponses::Labels(UpdateResourceLabelsResponse { resource })
                }
                ResourceUpdateRequests::Identifiers(_) => {
                    ResourceUpdateResponses::Identifiers(UpdateResourceIdentifiersResponse { resource })
                }
                ResourceUpdateRequests::Authors(_) => {
                    ResourceUpdateResponses::Authors(UpdateResourceAuthorsResponse { resource })
                }
            };

            wtxn.commit(associated_event_id, &[resource_idx], &[])?;
            // Create admin group, add user to admin group
            Ok::<_, ArunaError>(bincode::serialize(&response)?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}

fn parse_update_fields(
    store: &Store,
    wtxn: &mut WriteTxn,
    old_resource: Resource,
    resource_id: Ulid,
    time: DateTime<Utc>,
    request: ResourceUpdateRequests,
) -> Result<serde_json::Map<String, Value>, ArunaError> {
    let mut map = serde_json::Map::new();
    map.insert("id".to_string(), serde_json::to_value(resource_id)?);
    map.insert("last_modified".to_string(), serde_json::to_value(time)?);
    match request {
        ResourceUpdateRequests::Name(request) => {
            if !request.name.is_empty() && request.name != old_resource.name {
                // Check that name is unique
                if !store
                    .filtered_universe(
                        Some(&format!("name='{}' AND variant=0", request.name)),
                        wtxn.get_txn(),
                    )?
                    .is_empty()
                {
                    return Err(ArunaError::ConflictParameter {
                        name: "name".to_string(),
                        error: "Resource with this name already exists".to_string(),
                    });
                }
            }
            map.insert("name".to_string(), request.name.into());
        }
        ResourceUpdateRequests::Title(request) => {
            map.insert("tag".to_string(), request.title.into());
        }
        ResourceUpdateRequests::Description(request) => {
            map.insert("description".to_string(), request.description.into());
        }
        ResourceUpdateRequests::Visibility(request) => {
            if old_resource.visibility > request.visibility {
                return Err(ArunaError::ConflictParameter {
                    name: "visibility".to_string(),
                    error: "Cannot restrict visibiliyt".to_string(),
                });
            }
            let value = match request.visibility {
                crate::models::models::VisibilityClass::Public => "Public",
                crate::models::models::VisibilityClass::PublicMetadata => "PublicMetadata",
                crate::models::models::VisibilityClass::Private => "Private",
            };
            map.insert("visibility".to_string(), value.into());
        }
        ResourceUpdateRequests::License(request) => {
            map.insert("license_tag".to_string(), request.license_tag.into());
        }
        ResourceUpdateRequests::Labels(request) => {
            let mut labels = old_resource.labels;
            if !request.labels_to_remove.is_empty() {
                labels = labels
                    .into_iter()
                    .filter(|kv| !request.labels_to_remove.contains(kv))
                    .collect();
            }
            if !request.labels_to_add.is_empty() {
                labels.extend(request.labels_to_add);
            }
            map.insert(
                "labels".to_string(),
                serde_json::Value::Array(
                    labels
                        .iter()
                        .map(|kv| {
                            serde_json::to_value(kv)
                                .map_err(|e| ArunaError::DeserializeError(e.to_string()))
                        })
                        .collect::<Result<Vec<Value>, ArunaError>>()?,
                ),
            );
        }
        ResourceUpdateRequests::Identifiers(request) => {
            let mut ids = old_resource.identifiers;
            if !request.ids_to_remove.is_empty() {
                ids = ids
                    .into_iter()
                    .filter(|id| !request.ids_to_remove.contains(id))
                    .collect();
            }
            if !request.ids_to_add.is_empty() {
                ids.extend(request.ids_to_add);
            }
            map.insert(
                "identifiers".to_string(),
                serde_json::Value::Array(
                    ids.iter()
                        .map(|id| {
                            serde_json::to_value(id)
                                .map_err(|e| ArunaError::DeserializeError(e.to_string()))
                        })
                        .collect::<Result<Vec<Value>, ArunaError>>()?,
                ),
            );
        }
        ResourceUpdateRequests::Authors(request) => {
            let mut authors = old_resource.authors;
            if !request.authors_to_remove.is_empty() {
                authors = authors
                    .into_iter()
                    .filter(|a| !request.authors_to_remove.contains(a))
                    .collect();
            }
            if !request.authors_to_add.is_empty() {
                authors.extend(request.authors_to_add);
            }
            map.insert(
                "authors".to_string(),
                serde_json::Value::Array(
                    authors
                        .iter()
                        .map(|a| {
                            serde_json::to_value(a)
                                .map_err(|e| ArunaError::DeserializeError(e.to_string()))
                        })
                        .collect::<Result<Vec<Value>, ArunaError>>()?,
                ),
            );
        }
    };
    Ok(map)
}
