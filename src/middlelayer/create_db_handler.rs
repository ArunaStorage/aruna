use super::create_request_types::CreateRequest;
use super::db_handler::DatabaseHandler;
use crate::database::crud::CrudDb;
use crate::database::dsls::internal_relation_dsl::{
    InternalRelation, INTERNAL_RELATION_VARIANT_BELONGS_TO,
};
use crate::database::dsls::object_dsl::{Hierarchy, ObjectWithRelations};
use crate::database::enums::ObjectType;
use ahash::RandomState;
use anyhow::{anyhow, Result};
use aruna_rust_api::api::notification::services::v2::EventVariant;
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use postgres_types::Json;

impl DatabaseHandler {
    pub async fn create_resource(
        &self,
        request: CreateRequest,
        user_id: DieselUlid,
    ) -> Result<ObjectWithRelations> {
        // Init transaction
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();

        // Create object in database
        let mut object = request.into_new_db_object(user_id)?;
        object.create(transaction_client).await?;

        // Create internal relation in database
        let internal_relation: DashMap<DieselUlid, InternalRelation, RandomState> =
            match request.get_type() {
                ObjectType::PROJECT => DashMap::default(),
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
}
