use super::replication_request_types::ReplicationVariant;
use crate::{
    database::{
        crud::CrudDb,
        dsls::object_dsl::{EndpointInfo, Object},
        enums::{ObjectType, ReplicationStatus, SyncObject},
    },
    middlelayer::db_handler::DatabaseHandler,
    utils::database_utils::sort_objects,
};
use anyhow::{anyhow, Result};
use aruna_rust_api::api::{
    notification::services::v2::EventVariant,
    storage::{
        models::v2::ReplicationStatus as APIReplicationStatus,
        services::v2::{
            partial_replicate_data_request::DataVariant, UpdateReplicationStatusRequest,
        },
    },
};
use diesel_ulid::DieselUlid;
use itertools::Itertools;
use std::str::FromStr;

impl DatabaseHandler {
    pub async fn replicate(&self, request: ReplicationVariant) -> Result<APIReplicationStatus> {
        let mut client = self.database.get_client().await?;
        let (resource_id, proxy_id, endpoint_status_objects, endpoint_status_hierarchy) =
            match request {
                ReplicationVariant::Full(request) => {
                    let project_id = DieselUlid::from_str(&request.project_id)?;
                    if let Some(project) = Object::get(project_id, &client).await? {
                        if project.object_type != ObjectType::PROJECT {
                            return Err(anyhow!("Resource is not a project"));
                        }
                    } else {
                        return Err(anyhow!("Resource not found"));
                    };
                    let proxy_id = DieselUlid::from_str(&request.endpoint_id)?;
                    let endpoint_status_objects = EndpointInfo {
                        replication: crate::database::enums::ReplicationType::FullSync(project_id),
                        status: Some(ReplicationStatus::Waiting),
                    };
                    let endpoint_status_hierarchy = EndpointInfo {
                        replication: crate::database::enums::ReplicationType::FullSync(project_id),
                        status: None,
                    };

                    (
                        project_id,
                        proxy_id,
                        endpoint_status_objects,
                        endpoint_status_hierarchy,
                    )
                }
                ReplicationVariant::Partial(request) => {
                    let (request_type, resource_id) = match request.data_variant {
                        Some(res) => match res {
                            DataVariant::CollectionId(id) => (
                                ObjectType::COLLECTION,
                                diesel_ulid::DieselUlid::from_str(&id)?,
                            ),
                            DataVariant::DatasetId(id) => {
                                (ObjectType::DATASET, diesel_ulid::DieselUlid::from_str(&id)?)
                            }
                            DataVariant::ObjectId(id) => {
                                (ObjectType::OBJECT, diesel_ulid::DieselUlid::from_str(&id)?)
                            }
                        },
                        None => {
                            return Err(anyhow!("Invalid resource id"));
                        }
                    };
                    let sync_object =
                        if let Some(object) = Object::get(resource_id, &client).await? {
                            if object.object_type != request_type {
                                return Err(anyhow!("Wrong ObjectType"));
                            } else {
                                match object.object_type {
                                    ObjectType::PROJECT => {
                                        return Err(anyhow!("Projects can only be full sync"));
                                    }
                                    ObjectType::COLLECTION => SyncObject::CollectionId(resource_id),
                                    ObjectType::DATASET => SyncObject::DatasetId(resource_id),
                                    ObjectType::OBJECT => SyncObject::ObjectId(resource_id),
                                }
                            }
                        } else {
                            return Err(anyhow!("Resource not found"));
                        };
                    let proxy_id = DieselUlid::from_str(&request.endpoint_id)?;
                    let endpoint_status_objects = EndpointInfo {
                        replication: crate::database::enums::ReplicationType::PartialSync(
                            sync_object,
                        ),
                        status: Some(ReplicationStatus::Waiting),
                    };
                    let endpoint_status_hierarchy = EndpointInfo {
                        replication: crate::database::enums::ReplicationType::PartialSync(
                            sync_object,
                        ),
                        status: None,
                    };

                    (
                        resource_id,
                        proxy_id,
                        endpoint_status_objects,
                        endpoint_status_hierarchy,
                    )
                }
            };
        // Get all sub resources for project
        let res = Object::get(resource_id, &client)
            .await?
            .ok_or_else(|| anyhow!("ReplicationResource not found"))?;
        let mut sub_res: Vec<Object> =
            Object::fetch_recursive_objects(&resource_id, &client).await?;
        sub_res.push(res);

        // Collect all objects
        let objects: Vec<DieselUlid> = sub_res
            .iter()
            .filter_map(|o| match o.object_type {
                ObjectType::OBJECT => Some(o.id),
                _ => None,
            })
            .collect();

        // Collect all none-objects
        let hierarchy_resources: Vec<DieselUlid> = sub_res
            .iter()
            .filter_map(|r| match r.object_type {
                ObjectType::OBJECT => None,
                _ => Some(r.id),
            })
            .collect();

        // TODO:
        // - Find out which resources are outside of fullsync
        let mut all_affected = Vec::new();
        for object in &sub_res {
            let mut hierarchy = Object::fetch_object_hierarchies_by_id(&object.id, &client).await?;
            all_affected.append(&mut hierarchy);
        }

        let mut partial_synced_hierarchy = Vec::new();
        let mut partial_synced_objects = Vec::new();
        for affected_hierarchy in &all_affected {
            let mut flattened_hierarchy = vec![affected_hierarchy.project_id.clone()];
            if let Some(collection) = &affected_hierarchy.collection_id {
                flattened_hierarchy.push(collection.to_string());
            }
            if let Some(dataset) = &affected_hierarchy.dataset_id {
                flattened_hierarchy.push(dataset.to_string());
            }
            if let Some(object) = &affected_hierarchy.object_id {
                if !objects.iter().map(|id| id.to_string()).contains(object) {
                    partial_synced_objects.push(DieselUlid::from_str(object)?);
                }
            }
            for id in flattened_hierarchy {
                if !hierarchy_resources
                    .iter()
                    .map(|id| id.to_string())
                    .contains(&id)
                {
                    partial_synced_hierarchy.push(DieselUlid::from_str(&id)?);
                }
            }
        }
        dbg!(&objects);
        dbg!(&hierarchy_resources);
        dbg!(&partial_synced_hierarchy);
        dbg!(&partial_synced_objects);

        // Create transaction for status & endpoint updates
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();

        // Update objects with Status and EndpointInfo
        if !objects.is_empty() {
            Object::update_endpoints(
                proxy_id,
                endpoint_status_objects.clone(),
                objects,
                transaction_client,
            )
            .await?;
        } else {
            return Err(anyhow!("No objects found for syncing"));
        }
        // Update non-objects only with EndpointInfo
        if !hierarchy_resources.is_empty() {
            Object::update_endpoints(
                proxy_id,
                endpoint_status_hierarchy.clone(),
                hierarchy_resources,
                transaction_client,
            )
            .await?;
        }
        // Update not-explicit synced with PartialSyncInfo
        if !partial_synced_hierarchy.is_empty() {
            let ep_status_hierarchy = match endpoint_status_hierarchy.replication {
                crate::database::enums::ReplicationType::FullSync(id) => EndpointInfo {
                    replication: crate::database::enums::ReplicationType::PartialSync(
                        SyncObject::ProjectId(id),
                    ),
                    status: None,
                },
                crate::database::enums::ReplicationType::PartialSync(_) => {
                    endpoint_status_hierarchy
                }
            };
            Object::update_endpoints(
                proxy_id,
                ep_status_hierarchy,
                partial_synced_hierarchy.clone(),
                transaction_client,
            )
            .await?;
        }

        if !partial_synced_objects.is_empty() {
            let ep_status_objects = match endpoint_status_objects.replication {
                crate::database::enums::ReplicationType::FullSync(id) => EndpointInfo {
                    replication: crate::database::enums::ReplicationType::PartialSync(
                        SyncObject::ProjectId(id),
                    ),
                    status: Some(ReplicationStatus::Waiting),
                },
                crate::database::enums::ReplicationType::PartialSync(_) => endpoint_status_objects,
            };
            Object::update_endpoints(
                proxy_id,
                ep_status_objects,
                partial_synced_objects.clone(),
                transaction_client,
            )
            .await?;
        }
        transaction.commit().await?;

        // Try to emit object updated notification(s)
        let mut all_updated: Vec<DieselUlid> = sub_res.iter().map(|r| r.id).collect();
        all_updated.push(resource_id);
        all_updated.append(&mut partial_synced_objects);
        all_updated.append(&mut partial_synced_hierarchy);
        let mut all = Object::get_objects_with_relations(&all_updated, &client).await?;
        sort_objects(&mut all);
        dbg!(&all);
        for owr in all {
            self.cache.upsert_object(&owr.object.id, owr.clone());
            if let Err(err) = self
                .natsio_handler
                .register_resource_event(
                    &owr,
                    owr.object.fetch_object_hierarchies(&client).await?,
                    EventVariant::Updated,
                    Some(&DieselUlid::generate()), // block_id for deduplication
                )
                .await
            {
                log::error!("{}", err);
                return Ok(APIReplicationStatus::Error);
            };
        }
        Ok(APIReplicationStatus::Waiting)
    }
    pub async fn update_replication_status(
        &self,
        request: UpdateReplicationStatusRequest,
    ) -> Result<()> {
        let client = self.database.get_client().await?;
        let object_id = DieselUlid::from_str(&request.object_id)?;
        let object = Object::get(object_id, &client)
            .await?
            .ok_or_else(|| anyhow!("Object not found"))?;
        let endpoint_id = DieselUlid::from_str(&request.endpoint_id)?; // TODO: Remove from API and
                                                                       // query from token
        let mut endpoint_info = object
            .endpoints
            .0
            .get_mut(&endpoint_id)
            .ok_or_else(|| anyhow!("Endpoint not found in object"))?;
        let status = match request.status() {
            APIReplicationStatus::Unspecified => {
                return Err(anyhow!("Unspecified replication status"))
            }
            APIReplicationStatus::Waiting => ReplicationStatus::Waiting,
            APIReplicationStatus::Running => ReplicationStatus::Running,
            APIReplicationStatus::Finished => ReplicationStatus::Finished,
            APIReplicationStatus::Error => ReplicationStatus::Finished,
        };
        endpoint_info.status = Some(status);
        Object::update_endpoints(endpoint_id, endpoint_info.clone(), vec![object_id], &client)
            .await?;
        // Update cache
        let updated = Object::get_object_with_relations(&object_id, &client).await?;
        self.cache.upsert_object(&object_id, updated);
        Ok(())
    }
}
