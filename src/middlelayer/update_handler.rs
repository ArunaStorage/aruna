use crate::database::connection::Database;
use crate::database::crud::CrudDb;
use crate::database::dsls::internal_relation_dsl::{
    InternalRelation, INTERNAL_RELATION_VARIANT_BELONGS_TO,
};
use crate::database::dsls::object_dsl::{ExternalRelations, Hashes, KeyValues, Object};
use crate::database::enums::ObjectType;
use crate::utils::conversions::from_db_object;
use anyhow::{anyhow, Result};
use aruna_rust_api::api::storage::models::v2::{
    Collection, Dataset, ExternalRelation, Hash, KeyValue, Object as GRPCObject, Project,
};
use diesel_ulid::DieselUlid;
use postgres_types::Json;
use std::sync::Arc;

pub struct UpdateHandler {
    pub database: Arc<Database>,
    pub resource_type: ObjectType,
}

pub enum GRPCResource {
    Project(Project),
    Collection(Collection),
    Dataset(Dataset),
    Object(GRPCObject),
}

impl UpdateHandler {
    pub async fn create_resource(
        &self,
        name: String,
        description: String,
        key_values: Vec<KeyValue>,
        external_relations: Vec<ExternalRelation>,
        user_id: DieselUlid,
        data_class: i32,
        parent: Option<DieselUlid>,
        parent_variant: Option<i32>,
        hashes: Option<Vec<Hash>>,
    ) -> Result<GRPCResource> {
        // Conversions
        let id = DieselUlid::generate();
        let shared_id = DieselUlid::generate();
        let key_values: KeyValues = key_values.try_into()?;
        let external_relations: ExternalRelations = external_relations.try_into()?;
        let data_class = data_class.try_into()?;
        let hashes: Hashes = match hashes {
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
            name,
            description,
            created_at: None,
            content_len: 0,
            created_by: user_id,
            count: 1,
            key_values: Json(key_values.clone()),
            object_status: crate::database::enums::ObjectStatus::INITIALIZING,
            data_class,
            object_type: self.resource_type.clone(),
            external_relations: Json(external_relations.clone()),
            hashes: Json(hashes.clone()),
        };
        object.create(transaction_client).await?;

        let internal_relation: Option<InternalRelation> = match self.resource_type {
            ObjectType::PROJECT => None,
            _ => {
                let ir = InternalRelation {
                    id: DieselUlid::generate(),
                    origin_pid: parent.ok_or(anyhow!("No parent provided"))?,
                    origin_type: parent_variant
                        .ok_or(anyhow!("No parent provided"))?
                        .try_into()?,
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
