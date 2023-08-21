use crate::database::dsls::internal_relation_dsl::INTERNAL_RELATION_VARIANT_VERSION;
use crate::database::dsls::object_dsl::{Hierarchy, ObjectWithRelations};
use crate::database::enums::ObjectType;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::{database::dsls::object_dsl::Object, middlelayer::delete_request_types::DeleteRequest};
use anyhow::Result;
use aruna_rust_api::api::notification::services::v2::EventVariant;
use diesel_ulid::DieselUlid;

impl DatabaseHandler {
    pub async fn delete_resource(
        &self,
        request: DeleteRequest,
    ) -> Result<Vec<ObjectWithRelations>> {
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        let id = request.get_id()?;
        let resources = match request {
            DeleteRequest::Object(req) => {
                if req.with_revisions {
                    let object = Object::get_object_with_relations(&id, transaction_client).await?;
                    let mut objects: Vec<DieselUlid> = object
                        .inbound
                        .0
                        .iter()
                        .filter_map(|o| match o.relation_name.as_str() {
                            INTERNAL_RELATION_VARIANT_VERSION => Some(o.origin_pid),
                            _ => None,
                        })
                        .collect();
                    objects.push(id);
                    Object::set_deleted(&objects, transaction_client).await?;
                    objects
                } else {
                    Object::set_deleted(&vec![id], transaction_client).await?;
                    vec![id]
                }
            }
            _ => {
                Object::set_deleted(&vec![id], transaction_client).await?;
                vec![id]
            }
        };

        transaction.commit().await?;

        // Fetch hierarchies and object relations for notifications
        let objects_plus = Object::get_objects_with_relations(&resources, &client).await?;

        for object_plus in &objects_plus {
            let hierarchies = if object_plus.object.object_type == ObjectType::PROJECT {
                vec![Hierarchy {
                    project_id: object_plus.object.id.to_string(),
                    collection_id: None,
                    dataset_id: None,
                    object_id: None,
                }]
            } else {
                object_plus.object.fetch_object_hierarchies(&client).await?
            };

            if let Err(err) = self
                .natsio_handler
                .register_resource_event(object_plus, hierarchies, EventVariant::Deleted)
                .await
            {
                // Log error, rollback transaction and return
                log::error!("{}", err);
                //transaction.rollback().await?;
                return Err(anyhow::anyhow!("Notification emission failed"));
            }
        }

        //transaction.commit().await?;
        Ok(objects_plus)
    }
}
