use super::{
    controller::Controller,
    request::{Request, Requester, SerializedResponse},
};
use crate::{
    constants::relation_types::{self, DEFAULT},
    context::{BatchPermission, Context},
    error::ArunaError,
    logerr,
    models::{
        models::{Component, DataLocation, NodeVariant, Resource, ResourceVariant, SyncingStatus},
        requests::{
            CreateProjectRequest, CreateProjectResponse, CreateRelationRequest,
            CreateRelationResponse, CreateResourceBatchRequest, CreateResourceBatchResponse,
            CreateResourceRequest, CreateResourceResponse, Direction, GetRelationInfosRequest,
            GetRelationInfosResponse, GetRelationsRequest, GetRelationsResponse,
            GetResourcesRequest, GetResourcesResponse, Parent, RegisterDataRequest,
            RegisterDataResponse, UpdateResourceRequest, UpdateResourceResponse,
        },
    },
    storage::graph::{get_parent, get_related_user_or_groups, get_relations, has_relation},
    transactions::request::WriteRequest,
};
use ahash::RandomState;
use chrono::{DateTime, Utc};
use petgraph::Direction::Outgoing;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, error, info};
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

        let _requester = self.requester.clone();

        let store = controller.get_store();
        Ok(tokio::task::spawn_blocking(move || {
            // Create project

            let mut wtxn = store.write_txn()?;

            // Get group idx
            let group_idx = store.get_idx_from_ulid_validate(
                &group_id,
                "group_id",
                &[NodeVariant::Group],
                wtxn.get_ro_txn(),
                wtxn.get_ro_graph(),
            )?;
            // Get the realm
            let realm_idx = store.get_idx_from_ulid_validate(
                &realm_id,
                "realm_id",
                &[NodeVariant::Realm],
                wtxn.get_ro_txn(),
                wtxn.get_ro_graph(),
            )?;

            // Validate that the data endpoint is part of the realm
            let endpoint_idx = if let Some(data_endpoint) = endpoint {
                // Get the endpoint
                let data_endpoint_idx = store.get_idx_from_ulid_validate(
                    &data_endpoint,
                    "data_endpoint",
                    &[NodeVariant::Component],
                    wtxn.get_ro_txn(),
                    wtxn.get_ro_graph(),
                )?;

                if !has_relation(
                    wtxn.get_ro_graph(),
                    realm_idx,
                    data_endpoint_idx,
                    &[relation_types::REALM_USES_COMPONENT],
                ) {
                    error!("Realm does not use this data endpoint");
                    return Err(ArunaError::InvalidParameter {
                        name: "data_endpoint".to_string(),
                        error: "Realm does not use this data endpoint".to_string(),
                    });
                }
                Some(data_endpoint_idx)
            } else {
                debug!("No data endpoint provided");
                get_relations(wtxn.get_ro_graph(), realm_idx, Some(&[DEFAULT]), Outgoing)
                    .iter()
                    .find_map(|r| {
                        debug!("Checking relation: {:?}", r);
                        let node_type = wtxn.get_ro_graph().node_weight(r.target.into());
                        debug!("Node type: {:?}", node_type);
                        if node_type == Some(&NodeVariant::Component) {
                            Some(r.target)
                        } else {
                            None
                        }
                    })
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

            debug!("Endpoint found: {:?}", endpoint_idx);

            if let Some(endpoint_idx) = endpoint_idx {
                debug!("Endpoint found: {}", endpoint_idx);
                if let Some(component) = store.get_node::<Component>(wtxn.get_txn(), endpoint_idx) {
                    project.location = vec![DataLocation {
                        endpoint_id: component.id,
                        status: SyncingStatus::Finished,
                    }];
                } else {
                    return Err(ArunaError::NotFound(endpoint_idx.to_string()));
                }
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
            let additional_affected = if let Some(endpoint_idx) = endpoint_idx {
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

        let mut resource = Resource {
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
            let parent_node = store
                .get_node::<Resource>(wtxn.get_txn(), parent_idx)
                .expect("Idx exist but no node in documents -> corrupted database");

            if !matches!(
                parent_node.variant,
                ResourceVariant::Project | ResourceVariant::Folder
            ) {
                return Err(ArunaError::InvalidParameter {
                    name: "parent_id".to_string(),
                    error: "Wrong parent, must be folder or project".to_string(),
                });
            }

            let parent_endpoints = parent_node.location.clone();
            let affected = parent_endpoints
                .iter()
                .filter_map(|l| store.get_idx_from_ulid(&l.endpoint_id, wtxn.get_txn()))
                .collect::<Vec<_>>();
            resource.location = parent_endpoints;

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
            wtxn.commit(associated_event_id, &[parent_idx, resource_idx], &affected)?;
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
        //let (parents_to_check, resources) = self.parse_resources()?;
        let request = self.req.clone();
        let ids = self.resource_ids.clone();
        let time = DateTime::from_timestamp_millis(self.created_at).ok_or_else(|| {
            ArunaError::ConversionError {
                from: "i64".to_string(),
                to: "Chrono::DateTime".to_string(),
            }
        })?;

        let store = controller.get_store();
        Ok(tokio::task::spawn_blocking(move || {
            // Create resource

            let mut wtxn = store.write_txn()?;

            let mut to_create: Vec<(Resource, Ulid)> = vec![];
            let mut existing_names = HashMap::<u32, String, RandomState>::default();
            let mut affected_endpoints = vec![];

            for (index, batch_resource) in request.resources.iter().enumerate() {
                let mut resource = Resource {
                    id: ids
                        .get(index)
                        .ok_or_else(|| {
                            error!("Id not found: {}", index);
                            ArunaError::DeserializeError("Id not found".to_string())
                        })?
                        .clone(),
                    name: batch_resource.name.clone(),
                    description: batch_resource.description.clone(),
                    title: batch_resource.title.clone(),
                    revision: 0,
                    variant: batch_resource.variant.clone(),
                    labels: batch_resource.labels.clone(),
                    identifiers: batch_resource.identifiers.clone(),
                    content_len: 0,
                    count: 0,
                    visibility: batch_resource.visibility.clone(),
                    created_at: time,
                    last_modified: time,
                    authors: batch_resource.authors.clone(),
                    license_tag: batch_resource.license_tag.clone(),
                    locked: false,
                    location: vec![], // TODO: Locations and DataProxies
                    hashes: vec![],
                };
                let parent_id = match batch_resource.parent {
                    Parent::ID(ulid) => {
                        let parent_existing_idx = store
                            .get_idx_from_ulid(&ulid, wtxn.get_txn())
                            .ok_or_else(|| {
                                error!("Parent not found: {}", ulid);
                                ArunaError::NotFound(ulid.to_string())
                            })?;

                        let parent_node = store
                            .get_node::<Resource>(wtxn.get_txn(), parent_existing_idx)
                            .expect("Idx exist but no node in documents -> corrupted database");

                        if !matches!(
                            parent_node.variant,
                            ResourceVariant::Project | ResourceVariant::Folder
                        ) {
                            return Err(ArunaError::InvalidParameter {
                                name: "parent_id".to_string(),
                                error: "Wrong parent, must be folder or project".to_string(),
                            });
                        }

                        let parent_endpoints = parent_node.location.clone();
                        affected_endpoints.extend(parent_endpoints.iter().filter_map(|l| {
                            store.get_idx_from_ulid(&l.endpoint_id, wtxn.get_txn())
                        }));
                        resource.location = parent_endpoints;

                        // Check for unique name 1. Get all resources with the same name
                        // 0 Project, 1 Folder, 2 Object -> < 3
                        let universe = store.filtered_universe(
                            Some(&format!("name='{}' AND variant < 3", resource.name)),
                            wtxn.get_txn(),
                        )?;

                        // 2. Check if any of the resources in the universe have the same parent as a parent
                        for idx in universe {
                            // We need to use this because we have a lock on the store
                            if get_parent(wtxn.get_ro_graph(), idx) == Some(parent_existing_idx) {
                                return Err(ArunaError::ConflictParameter {
                                    name: "name".to_string(),
                                    error:
                                        "Resource with this name already exists in this hierarchy"
                                            .to_string(),
                                });
                            }
                        }
                        ulid
                    }
                    Parent::Idx(index) => {
                        let parent: &Resource = &to_create
                            .get(index as usize)
                            .ok_or_else(|| {
                                error!("Parent not found: {}", index);
                                ArunaError::NotFound(index.to_string())
                            })?
                            .0;

                        if !matches!(
                            parent.variant,
                            ResourceVariant::Project | ResourceVariant::Folder
                        ) {
                            return Err(ArunaError::InvalidParameter {
                                name: "parent_id".to_string(),
                                error: "Wrong parent, must be folder or project".to_string(),
                            });
                        }

                        let parent_endpoints = parent.location.clone();
                        resource.location = parent_endpoints;

                        if existing_names
                            .insert(index, resource.name.clone())
                            .is_some()
                        {
                            return Err(ArunaError::ConflictParameter {
                                name: "name".to_string(),
                                error: "Resource with this name already exists in this hierarchy"
                                    .to_string(),
                            });
                        }
                        parent.id
                    }
                };

                to_create.push((resource, parent_id));
            }

            let resource_idx =
                store.create_nodes_batch(&mut wtxn, to_create.iter().map(|e| &e.0).collect())?;

            let mut affected = vec![];
            for (resource, parent_id) in &to_create {
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
                resources: to_create.into_iter().map(|(r, _)| r).collect(),
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

impl Request for UpdateResourceRequest {
    type Response = UpdateResourceResponse;
    fn get_context(&self) -> Context {
        Context::Permission {
            min_permission: crate::models::models::Permission::Write,
            source: self.id,
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

        return Err(ArunaError::NotFound("Request unimplemented".to_string()));
        let response = controller.transaction(Ulid::new().0, &request_tx).await?;

        Ok(bincode::deserialize(&response)?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateResourceTx {
    req: UpdateResourceRequest,
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

        let resource_id = self.req.id;
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
            let mut map = serde_json::Map::new();
            map.insert("name".to_string(), request.name.into());

            store.update_node_field(&mut wtxn, resource_id, map)?;

            // Affected nodes: Group, Realm, Project

            let resource = store.get_node(wtxn.get_txn(), resource_idx).unwrap();

            wtxn.commit(associated_event_id, &[resource_idx], &[])?;
            // Create admin group, add user to admin group
            Ok::<_, ArunaError>(bincode::serialize(&UpdateResourceResponse { resource })?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}

impl Request for RegisterDataRequest {
    type Response = RegisterDataResponse;
    fn get_context(&self) -> Context {
        Context::Permission {
            min_permission: crate::models::models::Permission::Write,
            source: self.object_id,
        }
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        let request_tx = RegisterDataRequestTx {
            req: self,
            requester: requester.ok_or_else(|| ArunaError::Unauthorized)?,
            updated_at: Utc::now().timestamp_millis(),
        };

        let response = controller.transaction(Ulid::new().0, &request_tx).await?;

        Ok(bincode::deserialize(&response)?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterDataRequestTx {
    req: RegisterDataRequest,
    requester: Requester,
    updated_at: i64,
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for RegisterDataRequestTx {
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

        let object_id = self.req.object_id;
        let request = self.req.clone();

        let store = controller.get_store();

        Ok(tokio::task::spawn_blocking(move || {
            let mut wtxn = store.write_txn()?;

            // Get resource idx
            let Some(resource_idx) = store.get_idx_from_ulid(&object_id, wtxn.get_txn()) else {
                return Err(ArunaError::NotFound(object_id.to_string()));
            };

            let Some(resource): Option<Resource> = store.get_node(&wtxn.get_txn(), resource_idx)
            else {
                return Err(ArunaError::NotFound(format!(
                    "Resource with id {object_id} not found"
                )));
            };

            if resource.variant != ResourceVariant::Object {
                return Err(ArunaError::InvalidParameter {
                    name: "object_id".to_string(),
                    error: "Resource is not an object".to_string(),
                });
            }

            // Affected nodes: Group, Realm, Project
            wtxn.commit(associated_event_id, &[resource_idx], &[])?;
            // Create admin group, add user to admin group
            Ok::<_, ArunaError>(bincode::serialize(&RegisterDataResponse {})?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}
