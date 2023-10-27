use crate::auth::permission_handler::PermissionHandler;
use crate::database::crud::CrudDb;
use crate::database::dsls::internal_relation_dsl::{
    InternalRelation, INTERNAL_RELATION_VARIANT_BELONGS_TO,
};
use crate::database::dsls::object_dsl::{Object, ObjectWithRelations};
use crate::database::dsls::user_dsl::User;
use crate::database::enums::{DbPermissionLevel, ObjectMapping, ObjectType};
use crate::middlelayer::create_request_types::CreateRequest;
use crate::middlelayer::db_handler::DatabaseHandler;
use ahash::RandomState;
use anyhow::{anyhow, Result};
use aruna_rust_api::api::notification::services::v2::EventVariant;
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use itertools::Itertools;
use postgres_types::Json;
use std::sync::Arc;

impl DatabaseHandler {
    pub async fn create_resource(
        &self,
        authorizer: Arc<PermissionHandler>,
        request: CreateRequest,
        user_id: DieselUlid,
        is_dataproxy: bool,
    ) -> Result<(ObjectWithRelations, Option<User>)> {
        // Init transaction
        let mut client = self.database.get_client().await?;

        // query endpoints
        let endpoint_ids = request.get_endpoint(self.cache.clone(), &client).await?;

        // check if resource with same name on same hierarchy exists
        match request.get_type() {
            ObjectType::PROJECT => {
                let name = request.get_name()?;
                let object = Object::check_existing_projects(name, &client).await?;
                if let Some(object) = object {
                    return if is_dataproxy {
                        // return existing project
                        Ok((object, None))
                    } else {
                        // return err
                        Err(anyhow!("Project exists!"))
                    };
                };
            }
            ObjectType::DATASET | ObjectType::COLLECTION => {
                let parent_id = request
                    .get_parent()
                    .ok_or_else(|| anyhow!("No parent found"))?
                    .get_id()?;
                let parent = Object::get_object_with_relations(&parent_id, &client).await?;
                let name = request.get_name()?;
                if parent
                    .outbound_belongs_to
                    .0
                    .iter()
                    .filter_map(|rel| match rel.target_type {
                        ObjectType::OBJECT => {
                            // Check if object splits by '/'
                            match rel.target_name.split('/').next().map(|s| s.to_string()) {
                                // return first path
                                Some(split) => Some(split),
                                // return full name
                                None => Some(rel.target_name.to_string()),
                            }
                        }
                        // return name of other resources
                        _ => Some(rel.target_name.to_string()),
                    })
                    // Check if names contain request name
                    .contains(&name)
                {
                    return Err(anyhow!(
                        "Name is invalid: Contains path of object".to_string()
                    ));
                }
            }
            ObjectType::OBJECT => {
                let parent_id = request
                    .get_parent()
                    .ok_or_else(|| anyhow!("No parent found"))?
                    .get_id()?;
                let parent = Object::get_object_with_relations(&parent_id, &client).await?;
                let name = request.get_name()?;
                let query = match name.split('/').next() {
                    Some(name) => name.to_string(),
                    None => name,
                };
                if parent
                    .outbound_belongs_to
                    .0
                    .iter()
                    .filter_map(|rel| match rel.target_type {
                        ObjectType::OBJECT => {
                            // Check if object splits by '/'
                            match rel.target_name.split('/').next().map(|s| s.to_string()) {
                                // return first path
                                Some(split) => Some(split),
                                // return full name
                                None => Some(rel.target_name.to_string()),
                            }
                        }
                        // return name of other resources
                        _ => Some(rel.target_name.to_string()),
                    })
                    .contains(&query)
                {
                    return Err(anyhow!(
                        "Name is invalid: Contains substring that matches same hierarchy object"
                    ));
                }
            }
        }
        // Transaction setup
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        let mut user = None;

        // Create object in database
        let mut object = request
            .into_new_db_object(user_id, DieselUlid::default(), transaction_client)
            .await?;
        object.endpoints = Json(endpoint_ids);
        object.create(transaction_client).await?;

        // Create internal relation for parent and add user permissions for resource
        let (parent, internal_relation): (
            Option<ObjectWithRelations>,
            DashMap<DieselUlid, InternalRelation, RandomState>,
        ) = match request.get_type() {
            ObjectType::PROJECT => {
                user = Some(
                    self.add_permission_to_user(
                        user_id,
                        object.id,
                        &object.name,
                        ObjectMapping::PROJECT(DbPermissionLevel::ADMIN),
                        false,
                    )
                    .await?,
                );

                (None, DashMap::default())
            }
            _ => {
                let parent = request
                    .get_parent()
                    .ok_or_else(|| anyhow!("No parent provided"))?;

                let mut ir = InternalRelation {
                    id: DieselUlid::generate(),
                    origin_pid: parent.get_id()?,
                    origin_type: parent.get_type(),
                    target_pid: object.id,
                    target_type: object.object_type,
                    relation_name: INTERNAL_RELATION_VARIANT_BELONGS_TO.to_string(),
                    target_name: object.name.to_string(),
                };
                let result = ir.create(transaction_client).await;

                // When dataproxy creates object and object exists, return Ok(existing_object)
                if result.is_err() && is_dataproxy {
                    transaction.rollback().await?;
                    // Get parent ...
                    if let Some(parent) = self.cache.get_object(&parent.get_id()?) {
                        // ... iterate over outbound relations ...
                        for (id, irel) in parent.outbound_belongs_to.0 {
                            // ... if object name exists in outbound relations ...
                            if irel.target_name == object.name {
                                // ... return existing object
                                return Ok((
                                    self.cache
                                        .get_object(&id)
                                        .ok_or_else(|| anyhow!("Cache not synced"))?
                                        .clone(),
                                    None,
                                ));
                            }
                        }
                    }
                    return Err(anyhow!(
                        "Either cache not synced or other database error while creating object"
                    ));
                } else {
                    result?
                }

                let parent =
                    Object::get_object_with_relations(&parent.get_id()?, transaction_client)
                        .await?;

                (
                    Some(parent.clone()),
                    DashMap::from_iter([(parent.object.id, ir)]),
                )
            }
        };

        // Create specified relations
        let internal_relations = request.get_internal_relations(object.id, self.cache.clone())?;
        InternalRelation::batch_create(&internal_relations, transaction_client).await?;
        // Collect affected objects
        let mut affected: Vec<DieselUlid> = Vec::new();
        for (source, destination) in internal_relations
            .iter()
            .map(|ir| (ir.origin_pid, ir.target_pid))
        {
            if source == object.id && destination == object.id {
                // Are relations from self to self even possible?
                continue;
            } else if source == object.id {
                affected.push(destination);
            } else if destination == object.id {
                affected.push(source);
            } else {
                affected.push(source);
                affected.push(destination);
            }
        }
        transaction.commit().await?;

        // TODO:
        // - Update cache with affected objects
        let affected_owrs = Object::get_objects_with_relations(&affected, &client).await?;
        for affected_owr in affected_owrs {
            self.cache
                .upsert_object(&affected_owr.object.id, affected_owr.clone())
        }
        // - Trigger hooks for all

        // Create DTO which combines the object and its internal relations
        let owr = ObjectWithRelations {
            object: object.clone(),
            inbound: Json(DashMap::default()),
            inbound_belongs_to: Json(internal_relation.clone()),
            outbound: Json(DashMap::default()),
            outbound_belongs_to: Json(DashMap::default()),
        };

        // Update cache
        self.cache.add_object(owr.clone());
        if let Some(parent_plus) = parent {
            self.cache
                .upsert_object(&parent_plus.object.id, parent_plus.clone());

            // If created resource has parent emit notification for updated parent
            let parent_hierachies = parent_plus.object.fetch_object_hierarchies(&client).await?;

            // Try to emit object created notification(s)
            if let Err(err) = self
                .natsio_handler
                .register_resource_event(
                    &parent_plus,
                    parent_hierachies,
                    EventVariant::Updated,
                    Some(&DieselUlid::generate()), // block_id for deduplication
                )
                .await
            {
                // Log error and return
                log::error!("{}", err);
                //transaction.rollback().await?;
                return Err(anyhow::anyhow!("Notification emission failed: {err}"));
            }
        };

        // Trigger hooks
        if object.object_type != ObjectType::PROJECT {
            let db_handler = DatabaseHandler {
                database: self.database.clone(),
                natsio_handler: self.natsio_handler.clone(),
                cache: self.cache.clone(),
            };
            tokio::spawn(async move {
                let mut affected = affected;
                affected.push(object.id);
                for affected_id in affected {
                    let hook_trigger = db_handler
                        .trigger_on_creation(authorizer.clone(), affected_id, user_id)
                        .await;
                    if hook_trigger.is_err() {
                        log::error!("{:?}", hook_trigger)
                    }
                }
            });
        } else {
            let db_handler = DatabaseHandler {
                database: self.database.clone(),
                natsio_handler: self.natsio_handler.clone(),
                cache: self.cache.clone(),
            };
            tokio::spawn(async move {
                for affected_id in affected {
                    let hook_trigger = db_handler
                        .trigger_on_creation(authorizer.clone(), affected_id, user_id)
                        .await;
                    if hook_trigger.is_err() {
                        log::error!("{:?}", hook_trigger)
                    }
                }
            });
        };
        // Fetch all object paths for the notification subjects
        let object_hierarchies = object.fetch_object_hierarchies(&client).await?;
        // Try to emit object created notification(s)
        if let Err(err) = self
            .natsio_handler
            .register_resource_event(
                &owr,
                object_hierarchies,
                EventVariant::Created,
                Some(&DieselUlid::generate()), // block_id for deduplication
            )
            .await
        {
            // Log error, rollback transaction and return
            log::error!("{}", err);
            //transaction.rollback().await?;
            Err(anyhow::anyhow!("Notification emission failed: {err}"))
        } else {
            // Commit transaction and return
            //transaction.commit().await?;
            Ok((owr, user))
        }
    }
}
