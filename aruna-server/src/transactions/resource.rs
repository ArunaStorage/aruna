use super::{
    controller::Controller,
    request::{Request, Requester, SerializedResponse},
};
use crate::{
    constants::relation_types,
    context::Context,
    error::ArunaError,
    logerr,
    models::{
        models::{NodeVariant, Resource},
        requests::{
            CreateProjectRequest, CreateProjectResponse, CreateResourceBatchRequest,
            CreateResourceBatchResponse, CreateResourceRequest, CreateResourceResponse,
        },
    },
    storage::graph::get_parents,
    transactions::request::WriteRequest,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::info;
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
            created_at: Utc::now().timestamp(),
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

        let project = Resource {
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

        let store = controller.get_store();
        Ok(tokio::task::spawn_blocking(move || {
            // Create project

            let store = store.write().expect("Failed to lock store");
            let mut wtxn = store.write_txn()?;

            // Get group idx
            let Some(group_idx) = store.get_idx_from_ulid(&group_id, wtxn.get_txn()) else {
                return Err(ArunaError::NotFound(group_id.to_string()));
            };
            // Get the realm
            let Some(realm_idx) = store.get_idx_from_ulid(&realm_id, wtxn.get_txn()) else {
                return Err(ArunaError::NotFound(group_id.to_string()));
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

            // Affected nodes: Group, Realm, Project
            store.register_event(
                &mut wtxn,
                associated_event_id,
                &[realm_idx, group_idx, project_idx],
            )?;

            wtxn.commit()?;
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
            created_at: Utc::now().timestamp(),
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

            let store = store.write().expect("Failed to lock store");
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
                if get_parents(wtxn.get_ro_graph(), idx).contains(&parent_idx) {
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

            // Affected nodes: Group, Project
            store.register_event(&mut wtxn, associated_event_id, &[parent_idx, resource_idx])?;

            wtxn.commit()?;
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
            created_at: Utc::now().timestamp(),
        };

        let response = controller.transaction(Ulid::new().0, &request_tx).await?;

        Ok(bincode::deserialize(&response).inspect_err(logerr!())?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
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

        let time = DateTime::from_timestamp_millis(self.created_at).ok_or_else(|| {
            ArunaError::ConversionError {
                from: "i64".to_string(),
                to: "Chrono::DateTime".to_string(),
            }
        })?;

        let mut resources = Vec::new();

        for (idx, id) in self.resource_ids.iter().enumerate() {
            let res = self.req.resources.get(idx).cloned().ok_or_else(|| {
                ArunaError::DeserializeError(
                    "No entry for id found in CreateResourceBatchRequest".to_string(),
                )
            })?;
            resources.push(Resource {
                id: *id,
                name: res.name,
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
            });
        }

        let parent_id = self.req.parent_id;

        let store = controller.get_store();
        Ok(tokio::task::spawn_blocking(move || {
            // Create resource

            let store = store.write().expect("Failed to lock store");
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

            for resource in &resources {
                // Check for unique name 1. Get all resources with the same name
                // 0 Project, 1 Folder, 2 Object -> < 3
                let universe = store.filtered_universe(
                    Some(&format!("name='{}' AND variant < 3", resource.name)),
                    wtxn.get_txn(),
                )?;

                // 2. Check if any of the resources in the universe have the same parent as a parent
                for idx in universe {
                    // We need to use this because we have a lock on the store
                    if get_parents(wtxn.get_ro_graph(), idx).contains(&parent_idx) {
                        return Err(ArunaError::ConflictParameter {
                            name: "name".to_string(),
                            error: "Resource with this name already exists in this hierarchy"
                                .to_string(),
                        });
                    }
                }
            }

            let resource_idx = store.create_nodes_batch(&mut wtxn, resources.iter().collect())?;

            let mut affected = vec![parent_idx];
            for (_id, idx) in &resource_idx {

                // Add relation parent --HAS_PART--> resource
                store.create_relation(
                    &mut wtxn,
                    parent_idx,
                    *idx,
                    relation_types::HAS_PART,
                )?;
                affected.push(*idx);
            }

            // Affected nodes: Group, Project
            store.register_event(&mut wtxn, associated_event_id, &affected)?;

            wtxn.commit()?;
            Ok::<_, ArunaError>(bincode::serialize(&CreateResourceBatchResponse {
                resources,
            })?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}

//
// use super::auth::Auth;
// use super::controller::{Get, Transaction};
// use super::transaction::{ArunaTransaction, Fields, Metadata, Requests, TransactionOk};
// use super::utils::{get_created_at_field, get_resource_field};
// use crate::error::ArunaError;
// use crate::models::{self, HAS_PART, OWNS_PROJECT};
// use crate::requests::controller::Controller;
// use ulid::Ulid;

// Trait auth
// Trait <Get>
// Trait <Transaction>

// pub trait ReadResourceHandler: Auth + Get {
//     async fn get_resource(
//         &self,
//         token: Option<String>,
//         request: models::GetResourceRequest,
//     ) -> Result<models::GetResourceResponse, ArunaError> {
//         let _ = self.authorize_token(token, &request).await?;

//         let id = request.id;
//         let Some(models::NodeVariantValue::Resource(resource)) = self.get(id).await? else {
//             tracing::error!("Resource not found: {}", id);
//             return Err(ArunaError::NotFound(id.to_string()));
//         };

//         Ok(models::GetResourceResponse {
//             resource,
//             relations: vec![], // TODO: Add get_relations to Get trait
//         })
//     }
// }

// pub trait WriteResourceRequestHandler: Transaction + Auth + Get {
//     async fn create_resource(
//         &self,
//         token: Option<String>,
//         request: models::CreateResourceRequest,
//     ) -> Result<models::CreateResourceResponse, ArunaError> {
//         let transaction_id = u128::from_be_bytes(Ulid::new().to_bytes());
//         let resource_id = Ulid::new();
//         let created_at = chrono::Utc::now().timestamp();

//         // TODO: Auth

//         let requester = self
//             .authorize_token(token, &request)
//             .await?
//             .ok_or_else(|| {
//                 tracing::error!("Requester not found");
//                 ArunaError::Unauthorized
//             })?;

//         let TransactionOk::CreateResourceResponse(response) = self
//             .transaction(
//                 transaction_id,
//                 ArunaTransaction {
//                     request: Requests::CreateResourceRequest(request),
//                     metadata: Metadata { requester },
//                     generated_fields: Some(vec![
//                         Fields::ResourceId(resource_id),
//                         Fields::CreatedAt(created_at),
//                     ]),
//                 },
//             )
//             .await?
//         else {
//             tracing::error!("Unexpected response: Not CreateResourceResponse");
//             return Err(ArunaError::TransactionFailure(
//                 "Unexpected response: Not CreateResourceResponse".to_string(),
//             ));
//         };
//         Ok(response)
//     }

//     async fn create_project(
//         &self,
//         token: Option<String>,
//         request: models::CreateProjectRequest,
//     ) -> Result<models::CreateProjectResponse, ArunaError> {
//         let transaction_id = u128::from_be_bytes(Ulid::new().to_bytes());
//         let resource_id = Ulid::new();
//         let created_at = chrono::Utc::now().timestamp();

//         // TODO: Auth

//         let requester = self
//             .authorize_token(token, &request)
//             .await?
//             .ok_or_else(|| {
//                 tracing::error!("Requester not found");
//                 ArunaError::Unauthorized
//             })?;

//         let TransactionOk::CreateProjectResponse(response) = self
//             .transaction(
//                 transaction_id,
//                 ArunaTransaction {
//                     request: Requests::CreateProjectRequest(request),
//                     metadata: Metadata { requester },
//                     generated_fields: Some(vec![
//                         Fields::ResourceId(resource_id),
//                         Fields::CreatedAt(created_at),
//                     ]),
//                 },
//             )
//             .await?
//         else {
//             tracing::error!("Unexpected response: Not CreateProjectResponse");
//             return Err(ArunaError::TransactionFailure(
//                 "Unexpected response: Not CreateProjectResponse".to_string(),
//             ));
//         };
//         Ok(response)
//     }
// }

// impl ReadResourceHandler for Controller {}

// impl WriteResourceRequestHandler for Controller {}

// pub trait WriteResourceExecuteHandler: Auth + Get {
//     async fn create_resource(
//         &self,
//         request: models::CreateResourceRequest,
//         metadata: Metadata,
//         fields: Option<Vec<Fields>>,
//     ) -> Result<TransactionOk, ArunaError>;
//     async fn create_project(
//         &self,
//         request: models::CreateProjectRequest,
//         metadata: Metadata,
//         fields: Option<Vec<Fields>>,
//     ) -> Result<TransactionOk, ArunaError>;
// }

// impl WriteResourceExecuteHandler for Controller {
//     async fn create_resource(
//         &self,
//         request: models::CreateResourceRequest,
//         metadata: Metadata,
//         fields: Option<Vec<Fields>>,
//     ) -> Result<TransactionOk, ArunaError> {
//         self.authorize(&metadata.requester, &request).await?;

//         let resource_id = get_resource_field(&fields)?;
//         let created_at = get_created_at_field(&fields)?;

//         let status = match request.variant {
//             models::ResourceVariant::Folder => models::ResourceStatus::StatusAvailable,
//             models::ResourceVariant::Object => models::ResourceStatus::StatusInitializing,
//             _ => {
//                 tracing::error!("Unexpected resource type");
//                 return Err(ArunaError::TransactionFailure(
//                     "Unexpected resource type".to_string(),
//                 ));
//             }
//         };
//         let resource = models::Resource {
//             id: resource_id,
//             name: request.name,
//             title: request.title,
//             description: request.description,
//             revision: 0,
//             variant: request.variant,
//             labels: request.labels,
//             hook_status: Vec::new(),
//             identifiers: request.identifiers,
//             content_len: 0,
//             count: 0,
//             visibility: request.visibility,
//             created_at,
//             last_modified: created_at,
//             authors: request.authors,
//             status,
//             locked: false,
//             license_tag: request.license_tag,
//             endpoint_status: Vec::new(),
//             hashes: Vec::new(),
//         };

//         let mut lock = self.store.write().await;
//         let env = lock.view_store.get_env();
//         lock.view_store
//             .add_node(models::NodeVariantValue::Resource(resource.clone()))?;
//         // TODO: Create Admin group and set user as admin for this group
//         lock.graph
//             .add_node(models::NodeVariantId::Resource(resource_id));
//         lock.graph
//             .add_relation(
//                 models::NodeVariantId::Resource(request.parent_id),
//                 models::NodeVariantId::Resource(resource_id),
//                 HAS_PART,
//                 env,
//             )
//             .await?;

//         Ok(TransactionOk::CreateResourceResponse(
//             models::CreateResourceResponse { resource },
//         ))
//     }

//     async fn create_project(
//         &self,
//         request: models::CreateProjectRequest,
//         metadata: Metadata,
//         fields: Option<Vec<Fields>>,
//     ) -> Result<TransactionOk, ArunaError> {
//         self.authorize(&metadata.requester, &request).await?;

//         let resource_id = get_resource_field(&fields)?;
//         let created_at = get_created_at_field(&fields)?;

//         let resource = models::Resource {
//             id: resource_id,
//             name: request.name,
//             title: request.title,
//             description: request.description,
//             revision: 0,
//             variant: models::ResourceVariant::Project,
//             labels: request.labels,
//             hook_status: Vec::new(),
//             identifiers: request.identifiers,
//             content_len: 0,
//             count: 0,
//             visibility: request.visibility,
//             created_at,
//             last_modified: created_at,
//             authors: request.authors,
//             status: models::ResourceStatus::StatusAvailable,
//             locked: false,
//             license_tag: request.license_tag,
//             endpoint_status: Vec::new(),
//             hashes: Vec::new(),
//         };

//         let mut lock = self.store.write().await;
//         let env = lock.view_store.get_env();
//         lock.view_store
//             .add_node(models::NodeVariantValue::Resource(resource.clone()))?;
//         // TODO: Create Admin group and set user as admin for this group
//         lock.graph
//             .add_node(models::NodeVariantId::Resource(resource_id));
//         lock.graph
//             .add_relation(
//                 models::NodeVariantId::Group(request.group_id),
//                 models::NodeVariantId::Resource(resource_id),
//                 OWNS_PROJECT,
//                 env,
//             )
//             .await?;

//         Ok(TransactionOk::CreateProjectResponse(
//             models::CreateProjectResponse { resource },
//         ))
//     }
// }
