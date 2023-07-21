use std::str::FromStr;

use crate::database::dsls::object_dsl::{ExternalRelations, Hashes, KeyValues, Object};
use crate::database::enums::{ObjectStatus, ObjectType};
use anyhow::Result;
use aruna_rust_api::api::storage::models::v2::Hash;
use aruna_rust_api::api::storage::{
    models::v2::{ExternalRelation, KeyValue},
    services::v2::{
        CreateCollectionRequest, CreateDatasetRequest, CreateObjectRequest, CreateProjectRequest,
    },
};
use diesel_ulid::DieselUlid;
use postgres_types::Json;

pub enum CreateRequest {
    Project(CreateProjectRequest),
    Collection(CreateCollectionRequest),
    Dataset(CreateDatasetRequest),
    Object(CreateObjectRequest),
}

pub enum Parent {
    Project(String),
    Collection(String),
    Dataset(String),
}

impl Parent {
    pub fn get_id(&self) -> Result<DieselUlid> {
        match self {
            Parent::Project(id) | Parent::Collection(id) | Parent::Dataset(id) => {
                Ok(DieselUlid::from_str(id.as_str())?)
            }
        }
    }

    pub fn get_type(&self) -> ObjectType {
        match self {
            Parent::Project(_) => ObjectType::PROJECT,
            Parent::Collection(_) => ObjectType::COLLECTION,
            Parent::Dataset(_) => ObjectType::DATASET,
        }
    }
}

impl CreateRequest {
    pub fn get_name(&self) -> String {
        match self {
            CreateRequest::Project(request) => request.name,
            CreateRequest::Collection(request) => request.name,
            CreateRequest::Dataset(request) => request.name,
            CreateRequest::Object(request) => request.name,
        }
    }

    pub fn get_description(&self) -> String {
        match self {
            CreateRequest::Project(request) => request.description,
            CreateRequest::Collection(request) => request.description,
            CreateRequest::Dataset(request) => request.description,
            CreateRequest::Object(request) => request.description,
        }
    }

    pub fn get_key_values(&self) -> &Vec<KeyValue> {
        match self {
            CreateRequest::Project(request) => &request.key_values,
            CreateRequest::Collection(request) => &request.key_values,
            CreateRequest::Dataset(request) => &request.key_values,
            CreateRequest::Object(request) => &request.key_values,
        }
    }

    pub fn get_external_relations(&self) -> &Vec<ExternalRelation> {
        match self {
            CreateRequest::Project(request) => &request.external_relations,
            CreateRequest::Collection(request) => &request.external_relations,
            CreateRequest::Dataset(request) => &request.external_relations,
            CreateRequest::Object(request) => &request.external_relations,
        }
    }

    pub fn get_data_class(&self) -> i32 {
        match self {
            CreateRequest::Project(request) => request.data_class,
            CreateRequest::Collection(request) => request.data_class,
            CreateRequest::Dataset(request) => request.data_class,
            CreateRequest::Object(request) => request.data_class,
        }
    }

    pub fn get_hashes(&self) -> Option<Vec<Hash>> {
        match self {
            CreateRequest::Object(request) => Some(request.hashes),
            _ => None,
        }
    }

    pub fn get_type(&self) -> ObjectType {
        match self {
            CreateRequest::Project(_) => ObjectType::PROJECT,
            CreateRequest::Collection(_) => ObjectType::COLLECTION,
            CreateRequest::Dataset(_) => ObjectType::DATASET,
            CreateRequest::Object(_) => ObjectType::OBJECT,
        }
    }

    pub fn get_status(&self) -> ObjectStatus {
        match self {
            CreateRequest::Project(_)
            | CreateRequest::Collection(_)
            | CreateRequest::Dataset(_) => ObjectStatus::AVAILABLE,
            CreateRequest::Object(_) => ObjectStatus::INITIALIZING,
        }
    }

    pub fn get_parent(&self) -> Option<Parent> {
        match self {
            CreateRequest::Project(request) => None,
            CreateRequest::Collection(request) => Some(request.parent?.into()),
            CreateRequest::Dataset(request) => Some(request.parent?.into()),
            CreateRequest::Object(request) => Some(request.parent?.into()),
        }
    }

    pub fn into_new_db_object(&self, user_id: DieselUlid) -> Result<Object> {
        // Conversions
        let id = DieselUlid::generate();
        let shared_id = DieselUlid::generate();
        let key_values: KeyValues = self.get_key_values().try_into()?;
        let external_relations: ExternalRelations = self.get_external_relations().try_into()?;
        let data_class = self.get_data_class().try_into()?;
        let hashes: Hashes = match self.get_hashes() {
            Some(h) => h.try_into()?,
            None => Hashes(Vec::new()),
        };

        Ok(Object {
            id,
            shared_id,
            revision_number: 0,
            name: self.get_name(),
            description: self.get_description(),
            created_at: None,
            content_len: 0,
            created_by: user_id,
            count: 1,
            key_values: Json(key_values),
            object_status: self.get_status(),
            data_class,
            object_type: self.get_type(),
            external_relations: Json(external_relations),
            hashes: Json(hashes),
        })
    }
}
