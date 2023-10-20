use crate::auth::structs::Context;
use crate::caching::cache::Cache;
use crate::database::crud::CrudDb;
use crate::database::dsls::endpoint_dsl::Endpoint;
use crate::database::dsls::object_dsl::{ExternalRelations, Hashes, KeyValues, Object};
use crate::database::enums::{DbPermissionLevel, ObjectStatus, ObjectType};
use ahash::RandomState;
use anyhow::{anyhow, Result};
use aruna_rust_api::api::storage::models::v2::relation::Relation;
use aruna_rust_api::api::storage::models::v2::Hash;
use aruna_rust_api::api::storage::{
    models::v2::{ExternalRelation, KeyValue},
    services::v2::{
        CreateCollectionRequest, CreateDatasetRequest, CreateObjectRequest, CreateProjectRequest,
    },
};
use dashmap::DashMap;
use deadpool_postgres::Client;
use diesel_ulid::DieselUlid;
use postgres_types::Json;
use std::str::FromStr;
use std::sync::Arc;

pub enum CreateRequest {
    Project(CreateProjectRequest, String),
    Collection(CreateCollectionRequest),
    Dataset(CreateDatasetRequest),
    Object(CreateObjectRequest),
}

#[derive(Clone)]
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

    pub fn get_context(&self) -> Result<Context> {
        Ok(Context::res_ctx(
            self.get_id()?,
            DbPermissionLevel::APPEND,
            true,
        ))
    }
}

impl CreateRequest {
    pub fn get_name(&self) -> String {
        match self {
            CreateRequest::Project(request, _) => request.name.to_string(),
            CreateRequest::Collection(request) => request.name.to_string(),
            CreateRequest::Dataset(request) => request.name.to_string(),
            CreateRequest::Object(request) => request.name.to_string(),
        }
    }

    pub fn get_description(&self) -> String {
        match self {
            CreateRequest::Project(request, _) => request.description.to_string(),
            CreateRequest::Collection(request) => request.description.to_string(),
            CreateRequest::Dataset(request) => request.description.to_string(),
            CreateRequest::Object(request) => request.description.to_string(),
        }
    }

    pub fn get_key_values(&self) -> &Vec<KeyValue> {
        match self {
            CreateRequest::Project(request, _) => &request.key_values,
            CreateRequest::Collection(request) => &request.key_values,
            CreateRequest::Dataset(request) => &request.key_values,
            CreateRequest::Object(request) => &request.key_values,
        }
    }

    pub fn get_external_relations(&self) -> Vec<ExternalRelation> {
        match self {
            CreateRequest::Project(request, _) => request
                .relations
                .iter()
                .filter_map(|relation| match &relation.relation {
                    Some(Relation::External(rel)) => Some(rel.clone()),
                    _ => None,
                })
                .collect(),
            CreateRequest::Collection(request) => request
                .relations
                .iter()
                .filter_map(|relation| match &relation.relation {
                    Some(Relation::External(rel)) => Some(rel.clone()),
                    _ => None,
                })
                .collect(),
            CreateRequest::Dataset(request) => request
                .relations
                .iter()
                .filter_map(|relation| match &relation.relation {
                    Some(Relation::External(rel)) => Some(rel.clone()),
                    _ => None,
                })
                .collect(),

            CreateRequest::Object(request) => request
                .relations
                .iter()
                .filter_map(|relation| match &relation.relation {
                    Some(Relation::External(rel)) => Some(rel.clone()),
                    _ => None,
                })
                .collect(),
        }
    }

    pub fn get_data_class(&self) -> i32 {
        match self {
            CreateRequest::Project(request, _) => request.data_class,
            CreateRequest::Collection(request) => request.data_class,
            CreateRequest::Dataset(request) => request.data_class,
            CreateRequest::Object(request) => request.data_class,
        }
    }

    pub fn get_hashes(&self) -> Option<Vec<Hash>> {
        match self {
            CreateRequest::Object(request) => Some(request.hashes.clone()),
            _ => None,
        }
    }

    pub fn get_type(&self) -> ObjectType {
        match self {
            CreateRequest::Project(..) => ObjectType::PROJECT,
            CreateRequest::Collection(_) => ObjectType::COLLECTION,
            CreateRequest::Dataset(_) => ObjectType::DATASET,
            CreateRequest::Object(_) => ObjectType::OBJECT,
        }
    }

    pub fn is_dynamic(&self) -> bool {
        !matches!(self, CreateRequest::Object(_))
    }

    pub fn get_status(&self) -> ObjectStatus {
        match self {
            CreateRequest::Project(..)
            | CreateRequest::Collection(_)
            | CreateRequest::Dataset(_) => ObjectStatus::AVAILABLE,
            CreateRequest::Object(_) => ObjectStatus::INITIALIZING,
        }
    }

    pub fn get_parent(&self) -> Option<Parent> {
        match self {
            CreateRequest::Project(..) => None,
            CreateRequest::Collection(request) => Some(request.parent.clone()?.into()),
            CreateRequest::Dataset(request) => Some(request.parent.clone()?.into()),
            CreateRequest::Object(request) => Some(request.parent.clone()?.into()),
        }
    }

    pub async fn get_endpoint(
        &self,
        cache: Arc<Cache>,
        db_client: &Client,
    ) -> Result<DashMap<DieselUlid, bool, RandomState>> {
        match self {
            CreateRequest::Project(req, default_endpoint) => {
                if req.preferred_endpoint.is_empty() {
                    Ok(DashMap::from_iter([(
                        DieselUlid::from_str(default_endpoint)?,
                        true, // is true, because at least one full sync endpoint is needed for projects
                    )]))
                } else {
                    // TODO: Check if endpoint exists

                    let endpoint_id = DieselUlid::from_str(&req.preferred_endpoint)?;
                    match Endpoint::get(endpoint_id, db_client).await? {
                        Some(_) => Ok(DashMap::from_iter([(
                            endpoint_id,
                            true, // is true, because at least one full sync endpoint is needed for projects
                        )])),
                        None => Err(anyhow!("Endpoint does not exist")),
                    }
                }
            }
            _ => {
                let parent = self
                    .get_parent()
                    .ok_or_else(|| anyhow!("No parent found"))?;
                let parent = cache
                    .get_object(&parent.get_id()?)
                    .ok_or_else(|| anyhow!("Parent not found"))?;
                Ok(parent
                    .object
                    .endpoints
                    .0
                    .into_iter()
                    .filter(|(_, full_sync)| *full_sync)
                    .collect())
            }
        }
    }

    pub fn into_new_db_object(
        &self,
        user_id: DieselUlid,
        endpoint_id: DieselUlid,
    ) -> Result<Object> {
        // Conversions
        let id = DieselUlid::generate();
        let key_values: KeyValues = self.get_key_values().try_into()?;
        let external_relations: ExternalRelations = (&self.get_external_relations()).try_into()?;
        let data_class = self.get_data_class().try_into()?;
        let hashes: Hashes = match self.get_hashes() {
            Some(h) => h.try_into()?,
            None => Hashes(Vec::new()),
        };

        Ok(Object {
            id,
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
            dynamic: self.is_dynamic(),
            endpoints: Json(DashMap::from_iter([(endpoint_id, true)])),
        })
    }
}
