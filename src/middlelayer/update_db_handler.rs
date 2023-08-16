use super::update_request_types::UpdateObject;
use crate::database::crud::CrudDb;
use crate::database::dsls::object_dsl::{KeyValueVariant, Object, ObjectWithRelations};
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::update_request_types::{
    DataClassUpdate, DescriptionUpdate, KeyValueUpdate, NameUpdate,
};
use anyhow::{anyhow, Result};
use aruna_rust_api::api::storage::services::v2::UpdateObjectRequest;
use diesel_ulid::DieselUlid;
use postgres_types::Json;

impl DatabaseHandler {
    pub async fn update_dataclass(&self, request: DataClassUpdate) -> Result<ObjectWithRelations> {
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        let dataclass = request.get_dataclass()?;
        let id = request.get_id()?;
        let old_class: i32 = Object::get(id, transaction_client)
            .await?
            .ok_or(anyhow!("Resource not found."))?
            .data_class
            .into();
        if old_class < dataclass.clone().into() {
            return Err(anyhow!("Dataclasses can only be relaxed."));
        }
        Object::update_dataclass(id, dataclass, transaction_client).await?;
        transaction.commit().await?;
        let object_with_relations = Object::get_object_with_relations(&id, &client).await?;

        Ok(object_with_relations)
    }
    pub async fn update_name(&self, request: NameUpdate) -> Result<ObjectWithRelations> {
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        let name = request.get_name();
        let id = request.get_id()?;
        Object::update_name(id, name, transaction_client).await?;
        transaction.commit().await?;
        let object_with_relations = Object::get_object_with_relations(&id, &client).await?;
        Ok(object_with_relations)
    }
    pub async fn update_description(
        &self,
        request: DescriptionUpdate,
    ) -> Result<ObjectWithRelations> {
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        let description = request.get_description();
        let id = request.get_id()?;
        Object::update_description(id, description, transaction_client).await?;
        transaction.commit().await?;
        let object_with_relations = Object::get_object_with_relations(&id, &client).await?;
        Ok(object_with_relations)
    }
    pub async fn update_keyvals(&self, request: KeyValueUpdate) -> Result<ObjectWithRelations> {
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        let id = request.get_id()?;
        let (add_key_values, rm_key_values) = request.get_keyvals()?;
        if !add_key_values.0.is_empty() {
            for kv in add_key_values.0 {
                Object::add_key_value(&id, transaction_client, kv).await?;
            }
        } else if !rm_key_values.0.is_empty() {
            let object = Object::get(id, transaction_client)
                .await?
                .ok_or(anyhow!("Dataset does not exist."))?;
            for kv in rm_key_values.0 {
                if !(kv.variant == KeyValueVariant::STATIC_LABEL) {
                    object.remove_key_value(transaction_client, kv).await?;
                } else {
                    return Err(anyhow!("Cannot remove static labels."));
                }
            }
        } else {
            return Err(anyhow!(
                "Both add_key_values and remove_key_values are empty.",
            ));
        }
        transaction.commit().await?;
        let object_with_relations = Object::get_object_with_relations(&id, &client).await?;
        Ok(object_with_relations)
    }
    pub async fn update_grpc_object(
        &self,
        request: UpdateObjectRequest,
        user_id: DieselUlid,
    ) -> Result<(
        ObjectWithRelations,
        bool, // Creates revision
    )> {
        let mut client = self.database.get_client().await?;
        let req = UpdateObject(request.clone());
        let id = req.get_id()?;
        let old = Object::get(id, &client)
            .await?
            .ok_or(anyhow!("Object not found."))?;
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        let (id, flag) = if request.name.is_some()
            || !request.remove_key_values.is_empty()
            || !request.hashes.is_empty()
        {
            let id = DieselUlid::generate();
            // Create new object
            let create_object = Object {
                id,
                content_len: old.content_len,
                count: 1,
                revision_number: old.revision_number + 1,
                external_relations: old.clone().external_relations,
                created_at: None,
                created_by: user_id,
                data_class: req.get_dataclass(old.clone())?,
                description: req.get_description(old.clone()),
                name: req.get_name(old.clone()),
                key_values: Json(req.get_all_kvs(old.clone())?),
                hashes: Json(req.get_hashes(old.clone())?),
                object_type: crate::database::enums::ObjectType::OBJECT,
                object_status: crate::database::enums::ObjectStatus::AVAILABLE,
                dynamic: false,
                endpoints: Json(req.get_endpoints(old)?),
            };
            create_object.create(transaction_client).await?;
            if let Some(p) = request.parent {
                let relation = UpdateObject::add_parent_relation(id, p)?;
                relation.create(transaction_client).await?;
            }
            transaction.commit().await?;
            (id, true)
        } else {
            // Update in place
            let update_object = Object {
                id: old.id,
                content_len: old.content_len,
                count: 1,
                revision_number: old.revision_number,
                external_relations: old.clone().external_relations,
                created_at: None,
                created_by: old.created_by,
                data_class: req.get_dataclass(old.clone())?,
                description: req.get_description(old.clone()),
                name: old.clone().name,
                key_values: Json(req.get_add_keyvals(old.clone())?),
                hashes: old.clone().hashes,
                object_type: crate::database::enums::ObjectType::OBJECT,
                object_status: crate::database::enums::ObjectStatus::AVAILABLE,
                dynamic: false,
                endpoints: Json(req.get_endpoints(old)?),
            };
            update_object.update(transaction_client).await?;
            if let Some(p) = request.parent {
                let relation = UpdateObject::add_parent_relation(id, p)?;
                relation.create(transaction_client).await?;
            }
            transaction.commit().await?;
            (id, false)
        };
        let object_with_relations = Object::get_object_with_relations(&id, &client).await?;
        Ok((object_with_relations, flag))
    }
}
