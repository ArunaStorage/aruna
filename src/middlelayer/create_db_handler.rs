use super::create_request_types::CreateRequest;
use super::db_handler::DatabaseHandler;
use crate::database::crud::CrudDb;
use crate::database::dsls::internal_relation_dsl::{
    InternalRelation, INTERNAL_RELATION_VARIANT_BELONGS_TO,
};
use crate::database::enums::ObjectType;
use crate::utils::conversions::from_db_object;
use anyhow::{anyhow, Result};
use aruna_rust_api::api::storage::models::v2::generic_resource;
use diesel_ulid::DieselUlid;

impl DatabaseHandler {
    pub async fn create_resource(
        &self,
        request: CreateRequest,
        user_id: DieselUlid,
    ) -> Result<(generic_resource::Resource, DieselUlid)> {
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
                    target_pid: object.id,
                    target_type: ObjectType::OBJECT,
                    relation_name: INTERNAL_RELATION_VARIANT_BELONGS_TO.to_string(),
                };
                ir.create(transaction_client).await?;
                Some(ir)
            }
        };

        let cache_res = object.get_cache_resource();
        let shared_id = object.get_shared();

        Ok((
            from_db_object(internal_relation, object)?,
            shared_id,
            cache_res,
        ))
    }
}
