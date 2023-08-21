use std::str::FromStr;

use crate::database::dsls::internal_relation_dsl::InternalRelation;
use crate::database::dsls::object_dsl::Object;
use crate::database::dsls::object_dsl::ObjectWithRelations;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::relations_request_types::{
    ModifyRelations, RelationsToAdd, RelationsToModify, RelationsToRemove,
};
use ahash::HashSet;
use anyhow::Result;
use aruna_rust_api::api::notification::services::v2::EventVariant;
use diesel_ulid::DieselUlid;

impl DatabaseHandler {
    pub async fn modify_relations(
        &self,
        resource: Object,
        labels_to_add: RelationsToAdd,
        labels_to_remove: RelationsToRemove,
    ) -> Result<ObjectWithRelations> {
        // Collect all affected ids before transaction
        let mut affected_objects: HashSet<diesel_ulid::DieselUlid> = HashSet::default();
        affected_objects.insert(resource.id);

        for ext in &labels_to_add.external {
            affected_objects
                .insert(DieselUlid::from_str(&ext.identifier).map_err(|e| anyhow::anyhow!(e))?);
        }
        for ext in &labels_to_remove.external {
            affected_objects
                .insert(DieselUlid::from_str(&ext.identifier).map_err(|e| anyhow::anyhow!(e))?);
        }
        labels_to_add.internal.iter().for_each(|internal| {
            affected_objects.insert(internal.id);
        });
        labels_to_remove.internal.iter().for_each(|internal| {
            affected_objects.insert(internal.id);
        });

        // Transaction
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        if !labels_to_add.external.is_empty() {
            Object::add_external_relations(
                &resource.id,
                transaction_client,
                labels_to_add.external,
            )
            .await?;
        }
        if !labels_to_add.internal.is_empty() {
            InternalRelation::batch_create(&labels_to_add.internal, transaction_client).await?;
        }
        if !labels_to_remove.external.is_empty() {
            Object::remove_external_relation(
                &resource.id,
                transaction_client,
                labels_to_remove.external,
            )
            .await?;
        }
        if !labels_to_remove.internal.is_empty() {
            InternalRelation::batch_delete(
                // This does not work because the conversion cannot guess diesel ulids
                &labels_to_remove.internal.iter().map(|r| r.id).collect(),
                transaction_client,
            )
            .await?;
        }
        transaction.commit().await?;

        // Try to emit object updated notification(s)
        let affected_ids = Vec::from_iter(affected_objects);

        let objects_plus = Object::get_objects_with_relations(&affected_ids, &client).await?;

        for object_plus in &objects_plus {
            let hierarchies = object_plus.object.fetch_object_hierarchies(&client).await?;

            if let Err(err) = self
                .natsio_handler
                .register_resource_event(object_plus, hierarchies, EventVariant::Updated)
                .await
            {
                // Log error, rollback transaction and return
                log::error!("{}", err);
                //transaction.rollback().await?;
                return Err(anyhow::anyhow!("Notification emission failed"));
            }
        }

        let object = Object::get_object_with_relations(&resource.id, &client).await?;
        Ok(object)
    }

    pub async fn get_resource(
        &self,
        request: ModifyRelations,
    ) -> Result<(Object, RelationsToModify)> {
        let client = self.database.get_client().await?;
        let id = request.get_id()?;
        let resource = Object::get_object_with_relations(&id, &client).await?;
        Ok((resource.object.clone(), request.get_labels(resource)?))
    }
}
