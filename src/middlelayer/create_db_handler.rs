use super::create_request_types::CreateRequest;
use super::db_handler::DatabaseHandler;
use crate::auth::permission_handler::PermissionHandler;
use crate::database::crud::CrudDb;
use crate::database::dsls::internal_relation_dsl::{
    InternalRelation, INTERNAL_RELATION_VARIANT_BELONGS_TO,
};
use crate::database::dsls::object_dsl::{Object, ObjectWithRelations};
use crate::database::dsls::user_dsl::User;
use crate::database::enums::{DbPermissionLevel, ObjectMapping, ObjectType};
use ahash::RandomState;
use anyhow::{anyhow, Result};
use aruna_rust_api::api::notification::services::v2::EventVariant;
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
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

        // check if project exists:
        if request.get_type() == ObjectType::PROJECT {
            let object = Object::check_existing_projects(request.get_name(), &client).await?;
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
        // Transaction setup
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        let mut user = None;

        // Create object in database
        let mut object = request.into_new_db_object(user_id, DieselUlid::default())?;
        object.endpoints = Json(endpoint_ids);
        object.create(transaction_client).await?;

        // Create internal relation in database && add user permissions for resource
        let (parent, internal_relation): (
            Option<ObjectWithRelations>,
            DashMap<DieselUlid, InternalRelation, RandomState>,
        ) = match request.get_type() {
            ObjectType::PROJECT => {
                user = Some(
                    self.add_permission_to_user(
                        user_id,
                        object.id,
                        ObjectMapping::PROJECT(DbPermissionLevel::ADMIN),
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
                if result.is_err() && is_dataproxy {
                    dbg!(&result);
                    dbg!(&is_dataproxy);
                    transaction.rollback().await?;
                    if let Some(parent) = self.cache.get_object(&parent.get_id()?) {
                        dbg!(&parent);
                        dbg!(&self.cache.object_cache);
                        for (id, irel) in parent.outbound_belongs_to.0 {
                            dbg!(&id);
                            dbg!(&irel);
                            if irel.target_name == object.name {
                                dbg!("RETURN");
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
        transaction.commit().await?;
        let owr = ObjectWithRelations {
            object: object.clone(),
            inbound: Json(DashMap::default()),
            inbound_belongs_to: Json(internal_relation.clone()),
            outbound: Json(DashMap::default()),
            outbound_belongs_to: Json(DashMap::default()),
        };
        self.cache.add_object(owr.clone());
        if let Some(p) = parent {
            self.cache.update_object(&p.object.id, p.clone());
        };
        // Trigger hooks
        let object_with_rel = if object.object_type != ObjectType::PROJECT {
            dbg!("Reached trigger");
            match self
                .trigger_on_creation(authorizer.clone(), object.id, user_id)
                .await?
            {
                Some(owr) => owr,
                None => owr,
            }
        } else {
            // Create DTO which combines the object and its internal relations
            owr
        };
        // Fetch all object paths for the notification subjects
        let object_hierarchies = object.fetch_object_hierarchies(&client).await?;

        // Try to emit object created notification(s)
        if let Err(err) = self
            .natsio_handler
            .register_resource_event(&object_with_rel, object_hierarchies, EventVariant::Created)
            .await
        {
            // Log error, rollback transaction and return
            log::error!("{}", err);
            //transaction.rollback().await?;
            Err(anyhow::anyhow!("Notification emission failed"))
        } else {
            // Commit transaction and return
            //transaction.commit().await?;
            Ok((object_with_rel, user))
        }
    }
}
