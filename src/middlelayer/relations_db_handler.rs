use crate::database::crud::CrudDb;
use crate::database::dsls::object_dsl::Object;
use crate::database::dsls::object_dsl::ObjectWithRelations;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::relations_request_types::{
    LabelsToAdd, LabelsToRemove, ModifyLabels, ModifyRelations,
};
use anyhow::{anyhow, Result};

impl DatabaseHandler {
    pub async fn modify_relations(
        &self,
        resource: Object,
        labels_to_add: LabelsToAdd,
        labels_to_remove: LabelsToRemove,
    ) -> Result<ObjectWithRelations> {
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        Object::add_external_relations(&resource.id, &transaction_client, labels_to_add.external)
            .await?;
        for internal_to_add in labels_to_add.internal {
            internal_to_add.create(&transaction_client).await?;
        }
        for external_to_remove in labels_to_remove.external {
            resource
                .remove_external_relation(&transaction_client, external_to_remove)
                .await?;
        }
        for internal_to_remove in labels_to_remove.internal {
            internal_to_remove.delete(&transaction_client).await?;
        }
        let object = Object::get_object_with_relations(&resource.id, &transaction_client).await?;
        transaction.commit().await?;
        Ok(object)
    }
    pub async fn get_resource(&self, request: ModifyRelations) -> Result<(Object, ModifyLabels)> {
        let client = self.database.get_client().await?;
        let id = request.get_id()?;
        let resource = Object::get(id, &client)
            .await?
            .ok_or_else(|| anyhow!("Resource not found"))?;
        Ok((resource.clone(), request.get_labels(resource)?))
    }
}
