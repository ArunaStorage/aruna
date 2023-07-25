use crate::database::crud::CrudDb;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::{database::dsls::object_dsl::Object, middlelayer::delete_request_types::DeleteRequest};
use anyhow::{anyhow, Result};

impl DatabaseHandler {
    pub async fn delete_resource(&self, request: DeleteRequest) -> Result<()> {
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        let id = request.get_id()?;
        match request {
            DeleteRequest::Object(req) => {
                if req.with_revisions {
                    let all = Object::get_all_revisions(&id, &transaction_client).await?;
                    for o in all {
                        o.delete(&transaction_client).await?;
                    }
                } else {
                    let object = Object::get(id, &transaction_client)
                        .await?
                        .ok_or(anyhow!("Resource not found."))?;
                    object.delete(&transaction_client).await?;
                }
            }
            _ => {
                let resource = Object::get(id, &transaction_client)
                    .await?
                    .ok_or(anyhow!("Resource not found."))?;
                resource.delete(&transaction_client).await?;
            }
        }
        transaction.commit().await?;
        Ok(())
    }
}
