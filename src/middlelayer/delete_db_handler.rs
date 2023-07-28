use crate::database::crud::CrudDb;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::{database::dsls::object_dsl::Object, middlelayer::delete_request_types::DeleteRequest};
use anyhow::{anyhow, Result};
use aruna_rust_api::api::storage::models::v2::generic_resource;
use diesel_ulid::DieselUlid;

impl DatabaseHandler {
    pub async fn delete_resource(
        &self,
        request: DeleteRequest,
    ) -> Result<
        Vec<(
            generic_resource::Resource,
            DieselUlid,
            aruna_cache::structs::Resource,
        )>,
    > {
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        let id = request.get_id()?;
        let resources = match request {
            DeleteRequest::Object(req) => {
                if req.with_revisions {
                    Object::set_deleted_shared(&id, transaction_client).await?;
                    Object::get_all_revisions(&id, transaction_client).await?
                } else {
                    let object = Object::get(id, transaction_client)
                        .await?
                        .ok_or(anyhow!("Resource not found."))?;
                    object.delete(transaction_client).await?;
                    vec![object]
                }
            }
            _ => {
                let resource = Object::get(id, transaction_client)
                    .await?
                    .ok_or(anyhow!("Resource not found."))?;
                resource.delete(transaction_client).await?;
                vec![resource]
            }
        };
        let mut result = Vec::new();
        for resource in resources {
            let object =
                Object::get_object_with_relations(&resource.id, transaction_client).await?;
            result.push((
                object.try_into()?,
                resource.get_shared(),
                resource.get_cache_resource(),
            ));
        }
        transaction.commit().await?;
        Ok(result)
    }
}
