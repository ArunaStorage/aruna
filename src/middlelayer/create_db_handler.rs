use super::create_request_types::CreateRequest;
use super::db_handler::DatabaseHandler;
use crate::caching::cache::Cache;
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
        request: CreateRequest,
        user_id: DieselUlid,
        is_dataproxy: bool,
        cache: Arc<Cache>,
    ) -> Result<(ObjectWithRelations, Option<User>)> {
        // Init transaction
        let mut client = self.database.get_client().await?;

        // query endpoints
        let endpoint_ids = request.get_endpoint(cache.clone())?;

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
        let result = object.create(transaction_client).await;

        // return Object if ProxyCtx else return Err
        if result.is_err() && is_dataproxy {
            let owr = cache.get_object(&object.id);
            return match owr {
                Some(owr) => Ok((owr, None)),
                None => Err(anyhow!(
                    "Either cache not synced or other database error while creating object"
                )),
            };
        } else {
            result?
        }

        // Create internal relation in database && add user permissions for resource
        let internal_relation: DashMap<DieselUlid, InternalRelation, RandomState> =
            match request.get_type() {
                ObjectType::PROJECT => {
                    user = Some(
                        self.add_permission_to_user(
                            user_id,
                            object.id,
                            ObjectMapping::PROJECT(DbPermissionLevel::ADMIN),
                        )
                        .await?,
                    );

                    DashMap::default()
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
                    ir.create(transaction_client).await?;
                    DashMap::from_iter([(parent.get_id()?, ir)])
                }
            };
        transaction.commit().await?;

        // Fetch all object paths for the notification subjects
        let object_hierarchies = object.fetch_object_hierarchies(&client).await?;

        // Create DTO which combines the object and its internal relations
        let object_with_rel = ObjectWithRelations {
            object,
            inbound: Json(DashMap::default()),
            inbound_belongs_to: Json(internal_relation),
            outbound: Json(DashMap::default()),
            outbound_belongs_to: Json(DashMap::default()),
        };

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
