use super::create_request_types::CreateRequest;
use super::db_handler::DatabaseHandler;
use crate::database::crud::CrudDb;
use crate::database::dsls::internal_relation_dsl::{
    InternalRelation, INTERNAL_RELATION_VARIANT_BELONGS_TO,
};
use crate::database::dsls::object_dsl::{Hierarchy, Object, ObjectWithRelations};
use crate::database::dsls::user_dsl::User;
use crate::database::enums::{DbPermissionLevel, ObjectMapping, ObjectType};
use ahash::{HashMap, RandomState};
use anyhow::{anyhow, Result};
use aruna_rust_api::api::notification::services::v2::EventVariant;
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use itertools::Itertools;
use postgres_types::Json;

impl DatabaseHandler {
    pub async fn create_resource(
        &self,
        request: CreateRequest,
        user_id: DieselUlid,
        is_dataproxy: bool,
    ) -> Result<ObjectWithRelations> {
        // Init transaction
        let mut client = self.database.get_client().await?;
        let mut object = request.into_new_db_object(user_id)?;

        // let parent = if let Some(parent) = request.get_parent() {
        //     let check_existing =
        //         Object::get_object_with_relations(&parent.get_id()?, &client).await?;
        //     let (existing_ulid, exists) =

        //     if true
        //     {
        //         if is_dataproxy {
        //             let existing = Object::get_object_with_relations()
        //         } else {
        //             return Err(anyhow!("Object exists"));
        //         }
        //     }
        // };

        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();

        // Create object in database
        let mut object = request.into_new_db_object(user_id)?;
        object.create(transaction_client).await?;
        let create_result = object.create(transaction_client).await;

        if is_dataproxy && create_result.is_err() {
            // TODO: return conflicting object
            let owr = ObjectWithRelations {
                object,
                inbound: Json(DashMap::default()),
                // TODO: get
                inbound_belongs_to: Json(DashMap::default()),
                outbound: Json(DashMap::default()),
                outbound_belongs_to: Json(DashMap::default()),
            };
            return Err(anyhow!("Conflicting value")); // Placeholder
        }

        // Create internal relation in database
        let internal_relation: DashMap<DieselUlid, InternalRelation, RandomState> =
            match request.get_type() {
                ObjectType::PROJECT => {
                    self.add_permission_to_user(
                        user_id,
                        object.id,
                        ObjectMapping::PROJECT(DbPermissionLevel::ADMIN),
                    )
                    .await?;
                    DashMap::default()
                }
                _ => {
                    let parent = request
                        .get_parent()
                        .ok_or_else(|| anyhow!("No parent provided"))?;
                    let parent_with_relations =
                        Object::get_object_with_relations(&parent.get_id()?, transaction_client)
                            .await?;
                    let mut ir = InternalRelation {
                        id: DieselUlid::generate(),
                        origin_pid: parent.get_id()?,
                        origin_type: parent.get_type(),
                        target_pid: object.id,
                        target_type: object.object_type,
                        relation_name: INTERNAL_RELATION_VARIANT_BELONGS_TO.to_string(),
                    };
                    ir.create(transaction_client).await?;
                    DashMap::from_iter([(parent.get_id()?, ir)])
                }
            };
        transaction.commit().await?;

        // Fetch all object paths for the notification subjects
        let object_hierarchies = if let ObjectType::PROJECT = object.object_type {
            vec![Hierarchy {
                project_id: object.id.to_string(),
                collection_id: None,
                dataset_id: None,
                object_id: None,
            }]
        } else {
            object.fetch_object_hierarchies(&client).await?
        };

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
            Ok(object_with_rel)
        }
    }
    async fn exists(parent: ObjectWithRelations, name: String) -> (Option<DieselUlid>, bool) {
        todo!()
    }
}
