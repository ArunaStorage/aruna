use crate::database::dsls::internal_relation_dsl::INTERNAL_RELATION_VARIANT_VERSION;
use crate::database::dsls::object_dsl::ObjectWithRelations;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::{database::dsls::object_dsl::Object, middlelayer::delete_request_types::DeleteRequest};
use anyhow::Result;
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
        let result = Object::get_objects_with_relations(&resources, transaction_client).await?;
        transaction.commit().await?;
        Ok(result)
    }
}
