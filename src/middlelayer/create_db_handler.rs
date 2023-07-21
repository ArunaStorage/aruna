use super::create_request_handler::CreateRequest;
use crate::database::connection::Database;
use crate::database::crud::CrudDb;
use crate::database::dsls::internal_relation_dsl::{
    InternalRelation, INTERNAL_RELATION_VARIANT_BELONGS_TO,
};
use crate::database::dsls::object_dsl::{ExternalRelations, Hashes, KeyValues, Object};
use crate::database::enums::ObjectType;
use crate::utils::conversions::from_db_object;
use anyhow::{anyhow, Result};
use aruna_rust_api::api::storage::models::v2::generic_resource;
use diesel_ulid::DieselUlid;
use postgres_types::Json;
use std::sync::Arc;

pub struct DatabaseHandler {
    pub database: Arc<Database>,
}

impl DatabaseHandler {
    pub async fn create_resource(
        &self,
        request: CreateRequest,
        user_id: DieselUlid,
    ) -> Result<generic_resource::Resource> {
        // Conversions
        let id = DieselUlid::generate();
        let shared_id = DieselUlid::generate();
        let key_values: KeyValues = request.get_key_values().try_into()?;
        let external_relations: ExternalRelations = request.get_external_relations().try_into()?;
        let data_class = request.get_data_class().try_into()?;
        let hashes: Hashes = match request.get_hashes() {
            Some(h) => h.try_into()?,
            None => Hashes(Vec::new()),
        };
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();

        let object = Object {
            id,
            shared_id,
            revision_number: 0,
            name: request.get_name(),
            description: request.get_description(),
            created_at: None,
            content_len: 0,
            created_by: user_id,
            count: 1,
            key_values: Json(key_values.clone()),
            object_status: crate::database::enums::ObjectStatus::INITIALIZING,
            data_class,
            object_type: request.get_type(),
            external_relations: Json(external_relations.clone()),
            hashes: Json(hashes.clone()),
        };
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
                    target_pid: id,
                    target_type: ObjectType::OBJECT,
                    type_name: INTERNAL_RELATION_VARIANT_BELONGS_TO.to_string(),
                };
                ir.create(transaction_client).await?;
                Some(ir)
            }
        };
        Ok(from_db_object(internal_relation, object)?)
    }
}
