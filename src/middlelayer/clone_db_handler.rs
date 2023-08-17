use crate::database::crud::CrudDb;
use crate::database::dsls::internal_relation_dsl::{
    InternalRelation, INTERNAL_RELATION_VARIANT_BELONGS_TO,
};
use crate::database::dsls::object_dsl::Object;
use crate::database::dsls::object_dsl::ObjectWithRelations;
use crate::database::enums::{ObjectMapping, ObjectType};
use crate::middlelayer::db_handler::DatabaseHandler;
use anyhow::{anyhow, Result};
use deadpool_postgres::GenericClient;
use diesel_ulid::DieselUlid;

impl DatabaseHandler {
    pub async fn clone_object(
        &self,
        user_id: &DieselUlid,
        object_id: &DieselUlid,
        parent: ObjectMapping<DieselUlid>,
    ) -> Result<ObjectWithRelations> {
        let client = self.database.get_client().await?;
        let object = Object::get_object_with_relations(object_id, &client).await?;
        let new_id = DieselUlid::generate();
        let mut clone = object.object;
        clone.id = new_id;
        clone.created_by = *user_id;
        let (origin_pid, origin_type) = match parent {
            ObjectMapping::PROJECT(id) => (id, ObjectType::PROJECT),
            ObjectMapping::COLLECTION(id) => (id, ObjectType::COLLECTION),
            ObjectMapping::DATASET(id) => (id, ObjectType::DATASET),
            _ => return Err(anyhow!("Invalid parent")),
        };
        let mut relation = InternalRelation {
            id: DieselUlid::generate(),
            origin_pid,
            origin_type,
            relation_name: INTERNAL_RELATION_VARIANT_BELONGS_TO.to_string(),
            target_pid: new_id,
            target_type: ObjectType::OBJECT,
        };
        let mut t_client = self.database.get_client().await?;
        let transaction = t_client.transaction().await?;
        let transaction_client = transaction.client();
        clone.create(transaction_client).await?;
        relation.create(transaction_client).await?;
        transaction.commit().await?;
        Object::get_object_with_relations(&new_id, &client).await
    }
}
