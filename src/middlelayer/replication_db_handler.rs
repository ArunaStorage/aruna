use super::replication_request_types::ReplicationVariant;
use crate::{
    database::{
        crud::CrudDb,
        dsls::object_dsl::{EndpointInfo, Object},
        enums::{ObjectMapping, ObjectType, ReplicationStatus},
    },
    middlelayer::db_handler::DatabaseHandler,
};
use anyhow::{anyhow, Result};
use aruna_rust_api::api::{
    notification::services::v2::EventVariant,
    storage::{
        models::v2::ReplicationStatus as APIReplicationStatus,
        services::v2::partial_replicate_data_request::DataVariant,
    },
};
use diesel_ulid::DieselUlid;
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
                    let mapping = if let Some(object) = Object::get(resource_id, &client).await? {
                        if object.object_type != request_type {
                            return Err(anyhow!("Wrong ObjectType"));
                        } else {
                            match object.object_type {
                                ObjectType::PROJECT => {
                                    return Err(anyhow!("Projects can only be full sync"));
                                }
                                ObjectType::COLLECTION => ObjectMapping::COLLECTION(resource_id),
                                ObjectType::DATASET => ObjectMapping::DATASET(resource_id),
                                ObjectType::OBJECT => ObjectMapping::OBJECT(resource_id),
                            }
                        }
                    } else {
                        return Err(anyhow!("Resource not found"));
                    };
                    let proxy_id = DieselUlid::from_str(&request.endpoint_id)?;
                    let endpoint_status_objects = EndpointInfo {
                        replication: crate::database::enums::ReplicationType::PartialSync(mapping),
                        status: Some(ReplicationStatus::Waiting),
                    };
                    let endpoint_status_hierarchy = EndpointInfo {
                        replication: crate::database::enums::ReplicationType::PartialSync(mapping),
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
        let sub_res: Vec<Object> = Object::fetch_recursive_objects(&resource_id, &client).await?;
        // Create transaction for status & endpoint updates
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
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
        // Update objects with Status and EndpointInfo
        if !objects.is_empty() {
            Object::update_endpoints(
                proxy_id,
                endpoint_status_objects,
                objects,
                &transaction_client,
            )
            .await?;
        } else {
            return Err(anyhow!("No objects found for syncing"));
        }
        // Update non-objects only with EndpointInfo
        if !hierarchy_resources.is_empty() {
            Object::update_endpoints(
                proxy_id,
                endpoint_status_hierarchy,
                hierarchy_resources,
                &transaction_client,
            )
            .await?;
        }
        transaction.commit().await?;

        // Try to emit object updated notification(s)
        let mut all_updated: Vec<DieselUlid> = sub_res.iter().map(|r| r.id).collect();
        all_updated.push(resource_id);
        let all = Object::get_objects_with_relations(&all_updated, &client).await?;
        for owr in all {
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
}
