use super::update_request_types::UpdateObject;
use crate::auth::permission_handler::PermissionHandler;
use crate::database::crud::CrudDb;
use crate::database::dsls::internal_relation_dsl::{
    InternalRelation, INTERNAL_RELATION_VARIANT_VERSION,
};
use crate::database::dsls::object_dsl::{KeyValue, KeyValueVariant, Object, ObjectWithRelations};
use crate::database::enums::ObjectStatus;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::update_request_types::{
    DataClassUpdate, DescriptionUpdate, KeyValueUpdate, NameUpdate,
};
use anyhow::{anyhow, Result};
use aruna_rust_api::api::notification::services::v2::EventVariant;
use aruna_rust_api::api::storage::services::v2::{FinishObjectStagingRequest, UpdateObjectRequest};
use diesel_ulid::DieselUlid;
use postgres_types::Json;
use std::str::FromStr;
use std::sync::Arc;

impl DatabaseHandler {
    pub async fn update_dataclass(&self, request: DataClassUpdate) -> Result<ObjectWithRelations> {
        // Extract parameter from request
        let dataclass = request.get_dataclass()?;
        let id = request.get_id()?;

        // Init transaction
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();

        // Update object in database
        let old_object = Object::get(id, transaction_client)
            .await?
            .ok_or(anyhow!("Resource not found."))?;

        if old_object.data_class < dataclass {
            return Err(anyhow!("Dataclasses can only be relaxed."));
        }

        Object::update_dataclass(id, dataclass, transaction_client).await?;
        transaction.commit().await?;

        // Fetch hierarchies and object relations for notifications
        let object_plus = Object::get_object_with_relations(&id, &client).await?; // Why not just modify the original object?
        let hierarchies = object_plus.object.fetch_object_hierarchies(&client).await?;

        // Try to emit object updated notification(s)
        if let Err(err) = self
            .natsio_handler
            .register_resource_event(&object_plus, hierarchies, EventVariant::Updated)
            .await
        {
            // Log error, rollback transaction and return
            log::error!("{}", err);
            //transaction.rollback().await?;
            Err(anyhow::anyhow!("Notification emission failed"))
        } else {
            //transaction.commit().await?;
            Ok(object_plus)
        }
    }

    pub async fn update_name(&self, request: NameUpdate) -> Result<ObjectWithRelations> {
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        let name = request.get_name();
        let id = request.get_id()?;
        Object::update_name(id, name, transaction_client).await?;
        transaction.commit().await?;

        // Fetch hierarchies and object relations for notifications
        let object_plus = Object::get_object_with_relations(&id, &client).await?;

        let hierarchies = object_plus.object.fetch_object_hierarchies(&client).await?;

        // Try to emit object updated notification(s)
        if let Err(err) = self
            .natsio_handler
            .register_resource_event(&object_plus, hierarchies, EventVariant::Updated)
            .await
        {
            // Log error, rollback transaction and return
            log::error!("{}", err);
            //transaction.rollback().await?;
            Err(anyhow::anyhow!("Notification emission failed"))
        } else {
            //transaction.commit().await?;
            Ok(object_plus)
        }
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

        // Fetch hierarchies and object relations for notifications
        let object_plus = Object::get_object_with_relations(&id, &client).await?;

        let hierarchies = object_plus.object.fetch_object_hierarchies(&client).await?;

        // Try to emit object updated notification(s)
        if let Err(err) = self
            .natsio_handler
            .register_resource_event(&object_plus, hierarchies, EventVariant::Updated)
            .await
        {
            // Log error, rollback transaction and return
            log::error!("{}", err);
            //transaction.rollback().await?;
            Err(anyhow::anyhow!("Notification emission failed"))
        } else {
            //transaction.commit().await?;
            Ok(object_plus)
        }
    }

    pub async fn update_keyvals(
        &self,
        authorizer: Arc<PermissionHandler>,
        request: KeyValueUpdate,
        user_id: DieselUlid,
    ) -> Result<ObjectWithRelations> {
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        let id = request.get_id()?;
        let (add_key_values, rm_key_values) = request.get_keyvals()?;
        let mut hooks = Vec::new();

        if add_key_values.0.is_empty() && rm_key_values.0.is_empty() {
            return Err(anyhow!(
                "Both add_key_values and remove_key_values are empty.",
            ));
        }

        if !add_key_values.0.is_empty() {
            for kv in add_key_values.0 {
                if kv.variant == KeyValueVariant::HOOK {
                    hooks.push(kv.clone());
                }
                if kv.variant == KeyValueVariant::HOOK_STATUS {
                    return Err(anyhow!(
                        "Can't create hook status outside of hook callbacks"
                    ));
                }
                Object::add_key_value(&id, transaction_client, kv).await?;
            }
        }

        if !rm_key_values.0.is_empty() {
            let object = Object::get(id, transaction_client)
                .await?
                .ok_or(anyhow!("Dataset does not exist."))?;
            for kv in rm_key_values.0 {
                if kv.variant == KeyValueVariant::STATIC_LABEL {
                    return Err(anyhow!("Cannot remove static labels."));
                } else if kv.variant == KeyValueVariant::HOOK_STATUS {
                    return Err(anyhow!(
                        "Cannot remove hook_status outside of hook_callback"
                    ));
                } else {
                    object.remove_key_value(transaction_client, kv).await?;
                }
            }
        }
        transaction.commit().await?;

        // Trigger hook
        let object_plus = if hooks.is_empty() {
            Object::get_object_with_relations(&id, &client).await?
        } else {
            match self
                .trigger_on_append_hook(authorizer, user_id, id, hooks)
                .await?
            {
                Some(owr) => owr,
                None => Object::get_object_with_relations(&id, &client).await?,
            }
        };

        // Fetch hierarchies and object relations for notifications
        let hierarchies = object_plus.object.fetch_object_hierarchies(&client).await?;

        // Try to emit object updated notification(s)
        if let Err(err) = self
            .natsio_handler
            .register_resource_event(&object_plus, hierarchies, EventVariant::Updated)
            .await
        {
            // Log error, rollback transaction and return
            log::error!("{}", err);
            //transaction.rollback().await?;
            Err(anyhow::anyhow!("Notification emission failed"))
        } else {
            //transaction.commit().await?;
            Ok(object_plus)
        }
    }

    pub async fn update_grpc_object(
        &self,
        authorizer: Arc<PermissionHandler>,
        request: UpdateObjectRequest,
        user_id: DieselUlid,
    ) -> Result<(
        ObjectWithRelations,
        bool, // Creates revision
    )> {
        let mut client = self.database.get_client().await?;
        let req = UpdateObject(request.clone());
        let id = req.get_id()?;
        let owr = Object::get_object_with_relations(&id, &client).await?;
        let old = owr.object.clone();
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        let (id, is_new, affected) = if request.name.is_some()
            || !request.remove_key_values.is_empty()
            || !request.hashes.is_empty()
        {
            let id = DieselUlid::generate();
            // Create new object
            let mut create_object = Object {
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
                object_status: old.object_status.clone(),
                dynamic: false,
                endpoints: Json(req.get_endpoints(old.clone())?),
            };
            create_object.create(transaction_client).await?;

            // Clone all relations of old object with new object id
            let relations = UpdateObject::get_all_relations(owr.clone(), create_object.clone());
            let (mut new, (delete, mut affected)): (
                Vec<InternalRelation>,
                (Vec<DieselUlid>, Vec<DieselUlid>),
            ) = relations.into_iter().unzip();
            // Add version relation for old -> new
            let version = InternalRelation {
                id: DieselUlid::generate(),
                origin_pid: old.id,
                origin_type: old.object_type,
                relation_name: INTERNAL_RELATION_VARIANT_VERSION.to_string(),
                target_pid: create_object.id,
                target_type: create_object.object_type,
                target_name: create_object.name.clone(),
            };
            new.push(version);
            // Create all relations for new_object
            InternalRelation::batch_create(&new, transaction_client).await?;
            // Delete all relations for old object
            InternalRelation::batch_delete(&delete, transaction_client).await?;
            // Add parent if updated
            if let Some(p) = request.parent.clone() {
                let mut relation = UpdateObject::add_parent_relation(
                    id,
                    p.clone(),
                    create_object.name.to_string(),
                )?;
                relation.create(transaction_client).await?;
                let changed = match p {
                    aruna_rust_api::api::storage::services::v2::update_object_request::Parent::ProjectId(id) => DieselUlid::from_str(&id)?,
                    aruna_rust_api::api::storage::services::v2::update_object_request::Parent::CollectionId(id) => DieselUlid::from_str(&id)?,
                    aruna_rust_api::api::storage::services::v2::update_object_request::Parent::DatasetId(id) => DieselUlid::from_str(&id)?,
                };
                affected.push(changed);
            }
            // Return all affected ids for cache sync
            affected.push(old.id);

            (id, true, affected)
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
                object_status: old.object_status.clone(),
                dynamic: false,
                endpoints: Json(req.get_endpoints(old.clone())?),
            };
            update_object.update(transaction_client).await?;
            // Create & return all affected ids for cache sync
            let affected = if let Some(p) = request.parent.clone() {
                let mut relation = UpdateObject::add_parent_relation(
                    id,
                    p.clone(),
                    update_object.name.to_string(),
                )?;
                relation.create(transaction_client).await?;
                let p_id = match p {
                    aruna_rust_api::api::storage::services::v2::update_object_request::Parent::ProjectId(id) => DieselUlid::from_str(&id)?,
                    aruna_rust_api::api::storage::services::v2::update_object_request::Parent::CollectionId(id) => DieselUlid::from_str(&id)?,
                    aruna_rust_api::api::storage::services::v2::update_object_request::Parent::DatasetId(id) => DieselUlid::from_str(&id)?,
                };
                vec![p_id, update_object.id]
            } else {
                vec![update_object.id]
            };

            (id, false, affected)
        };
        transaction.commit().await?;

        // Cache sync of newly created object
        if is_new {
            let owr = Object::get_object_with_relations(&id, &client).await?;
            self.cache.add_object(owr);
        }

        // Update all affected objects in cache
        if !affected.is_empty() {
            let affected = Object::get_objects_with_relations(&affected, &client).await?;
            for o in affected {
                dbg!(&o);
                self.cache.update_object(&o.object.id.clone(), o);
            }
        }

        // Trigger hooks for the 4 combinations:
        // 1. update in place & new key_vals
        // 2. New object & new key_vals
        // 3. New object & no added key_vals -> Only old key_vals
        // 4. update in place & no key_vals
        let object_plus = if !req.0.add_key_values.is_empty() && !is_new {
            dbg!("TRIGGER");
            let kvs: Result<Vec<KeyValue>> = req
                .0
                .add_key_values
                .iter()
                .map(|kv| kv.try_into())
                .collect();
            match self
                .trigger_on_append_hook(authorizer, user_id, old.id, kvs?)
                .await?
            {
                Some(owr) => owr,
                None => Object::get_object_with_relations(&id, &client).await?,
            }
        } else if !req.0.add_key_values.is_empty() && is_new {
            self.trigger_on_creation(authorizer.clone(), id, user_id)
                .await?;
            let kvs: Result<Vec<KeyValue>> = req
                .0
                .add_key_values
                .iter()
                .map(|kv| kv.try_into())
                .collect();
            match self
                .trigger_on_append_hook(authorizer, user_id, id, kvs?)
                .await?
            {
                Some(owr) => owr,
                None => Object::get_object_with_relations(&id, &client).await?,
            }
        } else if is_new {
            let kvs = owr.object.key_values.0 .0.clone();
            if !kvs.is_empty() {
                self.trigger_on_append_hook(authorizer.clone(), user_id, id, kvs)
                    .await?;
            }
            match self.trigger_on_creation(authorizer, id, user_id).await? {
                Some(owr) => owr,
                None => Object::get_object_with_relations(&id, &client).await?,
            }
        } else {
            Object::get_object_with_relations(&id, &client).await?
        };
        // Update cache again with triggered hooks.
        // Two cache updates are needed because hooks
        // must have an updated cache for tree traversal
        self.cache
            .update_object(&object_plus.object.id, object_plus.clone());

        let hierarchies = object_plus.object.fetch_object_hierarchies(&client).await?;

        // Try to emit object updated notification(s)
        if let Err(err) = self
            .natsio_handler
            .register_resource_event(&object_plus, hierarchies, EventVariant::Updated)
            .await
        {
            // Log error, rollback transaction and return
            log::error!("{}", err);
            //transaction.rollback().await?;
            Err(anyhow::anyhow!("Notification emission failed"))
        } else {
            //transaction.commit().await?;
            Ok((object_plus, is_new))
        }
    }
    pub async fn finish_object(
        &self,
        request: FinishObjectStagingRequest,
    ) -> Result<ObjectWithRelations> {
        let client = self.database.get_client().await?;
        let id = DieselUlid::from_str(&request.object_id)?;
        let hashes = if request.hashes.is_empty() {
            None
        } else {
            Some(request.hashes.try_into()?)
        };
        let content_len = request.content_len;
        Object::finish_object_staging(&id, &client, hashes, content_len, ObjectStatus::AVAILABLE)
            .await?;

        // TODO: Database request finish_object_staging() should return ObjectWithRelations
        Object::get_object_with_relations(&id, &client).await
    }
}
