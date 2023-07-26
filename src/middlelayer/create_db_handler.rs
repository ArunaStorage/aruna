use super::create_request_types::CreateRequest;
use super::db_handler::DatabaseHandler;
use crate::database::crud::CrudDb;
use crate::database::dsls::internal_relation_dsl::{
    InternalRelation, INTERNAL_RELATION_VARIANT_BELONGS_TO,
};
use crate::database::dsls::object_dsl::{Inbound, ObjectWithRelations, Outbound};
use crate::database::enums::ObjectType;
use anyhow::{anyhow, Result};
use diesel_ulid::DieselUlid;
use postgres_types::Json;

impl DatabaseHandler {
    pub async fn create_resource(
        &self,
        request: CreateRequest,
        user_id: DieselUlid,
    ) -> Result<ObjectWithRelations> {
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        let object = request.into_new_db_object(user_id)?;
        object.create(transaction_client).await?;

        let internal_relation: Option<InternalRelation> = match request.get_type() {
            ObjectType::PROJECT => None,
            _ => {
                let parent = request
                    .get_parent()
                    .ok_or_else(|| anyhow!("No parent provided"))?;
                let ir = InternalRelation {
                    id: DieselUlid::generate(),
                    origin_pid: parent.get_id()?,
                    origin_type: parent.get_type(),
                    is_persistent: false,
                    target_pid: object.id.clone(),
                    target_type: object.object_type.clone(),
                    relation_name: INTERNAL_RELATION_VARIANT_BELONGS_TO.to_string(),
                };
                ir.create(transaction_client).await?;
                Some(ir)
            }
        };

        Ok(ObjectWithRelations {
            object: object,
            inbound: Json(Inbound(
                internal_relation.map(|x| vec![x]).unwrap_or_default(),
            )),
            outbound: Json(Outbound(vec![])),
        })
    }
}
