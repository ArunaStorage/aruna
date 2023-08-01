use crate::database::dsls::internal_relation_dsl::{
    InternalRelation, INTERNAL_RELATION_VARIANT_BELONGS_TO,
};
use crate::database::dsls::object_dsl::{Hashes, KeyValue as DBKeyValue, KeyValues, Object};
use crate::database::enums::{DataClass, ObjectType};
use ahash::RandomState;
use anyhow::{anyhow, Result};
use aruna_rust_api::api::storage::services::v2::update_object_request::Parent as UpdateParent;
use aruna_rust_api::api::storage::services::v2::{
    UpdateCollectionDataClassRequest, UpdateCollectionDescriptionRequest,
    UpdateCollectionKeyValuesRequest, UpdateCollectionNameRequest, UpdateDatasetDataClassRequest,
    UpdateDatasetDescriptionRequest, UpdateDatasetKeyValuesRequest, UpdateDatasetNameRequest,
    UpdateObjectRequest, UpdateProjectDataClassRequest, UpdateProjectDescriptionRequest,
    UpdateProjectKeyValuesRequest, UpdateProjectNameRequest,
};
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use std::str::FromStr;

pub struct UpdateObject(pub UpdateObjectRequest);

pub enum DataClassUpdate {
    Project(UpdateProjectDataClassRequest),
    Collection(UpdateCollectionDataClassRequest),
    Dataset(UpdateDatasetDataClassRequest),
}
pub enum DescriptionUpdate {
    Project(UpdateProjectDescriptionRequest),
    Collection(UpdateCollectionDescriptionRequest),
    Dataset(UpdateDatasetDescriptionRequest),
}
pub enum KeyValueUpdate {
    Project(UpdateProjectKeyValuesRequest),
    Collection(UpdateCollectionKeyValuesRequest),
    Dataset(UpdateDatasetKeyValuesRequest),
}

pub enum NameUpdate {
    Project(UpdateProjectNameRequest),
    Collection(UpdateCollectionNameRequest),
    Dataset(UpdateDatasetNameRequest),
}

impl DataClassUpdate {
    pub fn get_dataclass(&self) -> Result<DataClass> {
        let class = match self {
            DataClassUpdate::Project(req) => req.data_class.try_into()?,
            DataClassUpdate::Collection(req) => req.data_class.try_into()?,
            DataClassUpdate::Dataset(req) => req.data_class.try_into()?,
        };
        Ok(class)
    }
    pub fn get_id(&self) -> Result<DieselUlid> {
        let id = match self {
            DataClassUpdate::Project(req) => DieselUlid::from_str(&req.project_id)?,
            DataClassUpdate::Collection(req) => DieselUlid::from_str(&req.collection_id)?,
            DataClassUpdate::Dataset(req) => DieselUlid::from_str(&req.dataset_id)?,
        };
        Ok(id)
    }
}

impl NameUpdate {
    pub fn get_name(&self) -> String {
        match self {
            NameUpdate::Project(req) => req.name.to_string(),
            NameUpdate::Collection(req) => req.name.to_string(),
            NameUpdate::Dataset(req) => req.name.to_string(),
        }
    }
    pub fn get_id(&self) -> Result<DieselUlid> {
        let id = match self {
            NameUpdate::Project(req) => DieselUlid::from_str(&req.project_id)?,
            NameUpdate::Collection(req) => DieselUlid::from_str(&req.collection_id)?,
            NameUpdate::Dataset(req) => DieselUlid::from_str(&req.dataset_id)?,
        };
        Ok(id)
    }
}

impl DescriptionUpdate {
    pub fn get_description(&self) -> String {
        match self {
            DescriptionUpdate::Project(req) => req.description.to_string(),
            DescriptionUpdate::Collection(req) => req.description.to_string(),
            DescriptionUpdate::Dataset(req) => req.description.to_string(),
        }
    }
    pub fn get_id(&self) -> Result<DieselUlid> {
        let id = match self {
            DescriptionUpdate::Project(req) => DieselUlid::from_str(&req.project_id)?,
            DescriptionUpdate::Collection(req) => DieselUlid::from_str(&req.collection_id)?,
            DescriptionUpdate::Dataset(req) => DieselUlid::from_str(&req.dataset_id)?,
        };
        Ok(id)
    }
}

impl KeyValueUpdate {
    pub fn get_keyvals(&self) -> Result<(KeyValues, KeyValues)> {
        let (add, rm) = match self {
            KeyValueUpdate::Project(req) => (&req.add_key_values, &req.remove_key_values),
            KeyValueUpdate::Collection(req) => (&req.add_key_values, &req.remove_key_values),
            KeyValueUpdate::Dataset(req) => (&req.add_key_values, &req.remove_key_values),
        };
        Ok((add.try_into()?, rm.try_into()?))
    }
    pub fn get_id(&self) -> Result<DieselUlid> {
        let id = match self {
            KeyValueUpdate::Project(req) => DieselUlid::from_str(&req.project_id)?,
            KeyValueUpdate::Collection(req) => DieselUlid::from_str(&req.collection_id)?,
            KeyValueUpdate::Dataset(req) => DieselUlid::from_str(&req.dataset_id)?,
        };
        Ok(id)
    }
}

impl UpdateObject {
    pub fn get_id(&self) -> Result<DieselUlid> {
        Ok(DieselUlid::from_str(&self.0.object_id)?)
    }
    pub fn get_description(&self, old: Object) -> String {
        match self.0.description.clone() {
            Some(d) => d,
            None => old.description,
        }
    }
    pub fn get_add_keyvals(&self, old: Object) -> Result<KeyValues> {
        Ok(match self.0.add_key_values.is_empty() {
            false => {
                let kv = &self.0.add_key_values;
                kv.try_into()?
            }
            true => old.key_values.0,
        })
    }
    pub fn get_hashes(&self, old: Object) -> Result<Hashes> {
        Ok(match self.0.hashes.is_empty() {
            false => self.0.hashes.clone().try_into()?,
            true => old.hashes.0,
        })
    }
    pub fn get_all_kvs(&self, old: Object) -> Result<KeyValues> {
        let rm_kv = &self.0.remove_key_values;
        let add_kv = &self.0.add_key_values;
        let remove_kv: KeyValues = rm_kv.try_into()?;
        let mut add_kv: KeyValues = add_kv.try_into()?;
        let mut key_values: Vec<DBKeyValue> = old
            .key_values
            .0
             .0
            .into_iter()
            .filter(|l| !remove_kv.0.contains(l))
            .collect();
        key_values.append(&mut add_kv.0);
        Ok(KeyValues(key_values))
    }
    pub fn get_name(&self, old: Object) -> String {
        match self.0.name.clone() {
            Some(n) => n,
            None => old.name,
        }
    }
    pub fn get_dataclass(&self, old: Object) -> Result<DataClass> {
        let new = self.0.data_class;
        let old: i32 = old.data_class.into();
        if old > new {
            return Err(anyhow!("Dataclass can only be relaxed."));
        }
        new.try_into()
    }
    pub fn get_endpoints(&self, old: Object) -> Result<DashMap<DieselUlid, bool, RandomState>> {
        // TODO -> Currently not implemented in APICall
        Ok(old.endpoints.0)
    }
    pub fn add_parent_relation(
        object_id: DieselUlid,
        parent: UpdateParent,
    ) -> Result<InternalRelation> {
        let (parent_id, parent_type) = match parent {
            UpdateParent::ProjectId(p) => (DieselUlid::from_str(&p)?, ObjectType::PROJECT),
            UpdateParent::CollectionId(p) => (DieselUlid::from_str(&p)?, ObjectType::COLLECTION),
            UpdateParent::DatasetId(p) => (DieselUlid::from_str(&p)?, ObjectType::DATASET),
        };
        Ok(InternalRelation {
            id: DieselUlid::generate(),
            origin_pid: parent_id,
            origin_type: parent_type,
            relation_name: INTERNAL_RELATION_VARIANT_BELONGS_TO.to_string(),
            target_pid: object_id,
            target_type: ObjectType::OBJECT,
        })
    }
}
