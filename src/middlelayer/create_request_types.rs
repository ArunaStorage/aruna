use crate::auth::structs::Context;
use crate::caching::cache::Cache;
use crate::database::crud::CrudDb;
use crate::database::dsls::endpoint_dsl::Endpoint;
use crate::database::dsls::internal_relation_dsl::InternalRelation;
use crate::database::dsls::license_dsl::{License, ALL_RIGHTS_RESERVED};
use crate::database::dsls::object_dsl::{ExternalRelations, Hashes, KeyValues, Object};
use crate::database::enums::{DbPermissionLevel, ObjectStatus, ObjectType};
use crate::utils::conversions::ContextContainer;
use ahash::RandomState;
use anyhow::{anyhow, Result};
use aruna_rust_api::api::storage::models::v2::relation::Relation as RelationEnum;
use aruna_rust_api::api::storage::models::v2::Hash;
use aruna_rust_api::api::storage::models::v2::Relation;
use aruna_rust_api::api::storage::{
    models::v2::{ExternalRelation, KeyValue},
    services::v2::{
        CreateCollectionRequest, CreateDatasetRequest, CreateObjectRequest, CreateProjectRequest,
    },
};
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use postgres_types::Json;
use regex::Regex;
use std::str::FromStr;
use std::sync::Arc;
use tokio_postgres::Client;

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

pub static PROJECT_SCHEMA: Regex = Regex::new(r"^[a-z0-9\-]+$");
pub static S3_KEY_SCHEMA: Regex = Regex::new(r"^[a-z0-9\-\!\_\.\*\_\'\(\)]+$");

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
    pub fn get_name(&self) -> Result<String> {
        match self {
            CreateRequest::Project(request, _) => {
                let name = request.name.to_string();
                if !PROJECT_SCHEMA.is_match(&name) {
                    Err(anyhow!("Invalid project name"))
                } else {
                    Ok(name)
                }
            }
            CreateRequest::Collection(request) => {
                let name = request.name.to_string();
                if !S3_KEY_SCHEMA.is_match(&name) {
                    Err(anyhow!("Invalid collection name"))
                } else {
                    Ok(name)
                }
            }
            CreateRequest::Dataset(request) => {
                let name = request.name.to_string();
                if !S3_KEY_SCHEMA.is_match(&name) {
                    Err(anyhow!("Invalid dataset name"))
                } else {
                    Ok(name)
                }
            }
            CreateRequest::Object(request) => {
                let name = request.name.to_string();
                if !S3_KEY_SCHEMA.is_match(&name) {
                    Err(anyhow!("Invalid object name"))
                } else {
                    Ok(name)
                }
            }
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

    pub fn get_relation_contexts(&self) -> Result<Vec<Context>, tonic::Status> {
        let container: ContextContainer = match self {
            CreateRequest::Project(req, _) => req.relations.clone().try_into()?,
            CreateRequest::Collection(req) => req.relations.clone().try_into()?,
            CreateRequest::Dataset(req) => req.relations.clone().try_into()?,
            CreateRequest::Object(req) => req.relations.clone().try_into()?,
        };
        Ok(container.0)
    }

    pub fn get_internal_relations(
        &self,
        id: DieselUlid,
        cache: Arc<Cache>,
    ) -> Result<Vec<InternalRelation>> {
        match self {
            CreateRequest::Project(req, _) => req
                .relations
                .iter()
                .filter_map(|rel| match &rel.relation {
                    Some(RelationEnum::Internal(internal)) => Some(internal),
                    _ => None,
                })
                .map(|ir| InternalRelation::from_api(&ir, id, cache.clone()))
                .collect::<Result<Vec<InternalRelation>>>(),
            CreateRequest::Collection(req) => req
                .relations
                .iter()
                .filter_map(|rel| match &rel.relation {
                    Some(RelationEnum::Internal(internal)) => Some(internal),
                    _ => None,
                })
                .map(|ir| InternalRelation::from_api(&ir, id, cache.clone()))
                .collect::<Result<Vec<InternalRelation>>>(),
            CreateRequest::Dataset(req) => req
                .relations
                .iter()
                .filter_map(|rel| match &rel.relation {
                    Some(RelationEnum::Internal(internal)) => Some(internal),
                    _ => None,
                })
                .map(|ir| InternalRelation::from_api(&ir, id, cache.clone()))
                .collect::<Result<Vec<InternalRelation>>>(),
            CreateRequest::Object(req) => req
                .relations
                .iter()
                .filter_map(|rel| match &rel.relation {
                    Some(RelationEnum::Internal(internal)) => Some(internal),
                    _ => None,
                })
                .map(|ir| InternalRelation::from_api(&ir, id, cache.clone()))
                .collect::<Result<Vec<InternalRelation>>>(),
        }
    }
    pub fn get_external_relations(&self) -> Vec<ExternalRelation> {
        match self {
            CreateRequest::Project(request, _) => request
                .relations
                .iter()
                .filter_map(|relation| match &relation.relation {
                    Some(RelationEnum::External(rel)) => Some(rel.clone()),
                    _ => None,
                })
                .collect(),
            CreateRequest::Collection(request) => request
                .relations
                .iter()
                .filter_map(|relation| match &relation.relation {
                    Some(RelationEnum::External(rel)) => Some(rel.clone()),
                    _ => None,
                })
                .collect(),
            CreateRequest::Dataset(request) => request
                .relations
                .iter()
                .filter_map(|relation| match &relation.relation {
                    Some(RelationEnum::External(rel)) => Some(rel.clone()),
                    _ => None,
                })
                .collect(),

            CreateRequest::Object(request) => request
                .relations
                .iter()
                .filter_map(|relation| match &relation.relation {
                    Some(RelationEnum::External(rel)) => Some(rel.clone()),
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
                    // Checks if endpoints exists
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

    pub async fn into_new_db_object(
        &self,
        user_id: DieselUlid,
        endpoint_id: DieselUlid,
        client: &Client,
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
        let (metadata_license, data_license) = self.get_licenses(client).await?;
        let name = self.get_name()?;

        Ok(Object {
            id,
            revision_number: 0,
            name,
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
            metadata_license,
            data_license,
        })
    }

    pub async fn get_licenses(&self, client: &Client) -> Result<(String, String)> {
        // Either retrieve license from request or parent
        match &self {
            // Projects must specify licenses
            CreateRequest::Project(req, _) => {
                let data_tag = if &req.default_data_license_tag.is_empty() {
                    ALL_RIGHTS_RESERVED
                } else {
                    &req.default_data_license_tag
                };
                let meta_tag = if &req.metadata_license_tag.is_empty() {
                    ALL_RIGHTS_RESERVED
                } else {
                    &req.metadata_license_tag
                };
                if License::get(data_tag.clone(), client).await?.is_some()
                    && License::get(meta_tag.clone(), client).await?.is_some()
                {
                    Ok((meta_tag.to_string(), data_tag.to_string()))
                } else {
                    Err(anyhow!("Invalid license: License not found"))
                }
            }
            CreateRequest::Collection(req) => {
                let parent = self
                    .get_parent()
                    .ok_or_else(|| anyhow!("No parent specified"))?
                    .get_id()?;
                let data_tag = req.default_data_license_tag.clone();
                let meta_tag = req.metadata_license_tag.clone();
                CreateRequest::check_license(data_tag, meta_tag, parent, client).await
            }
            CreateRequest::Dataset(req) => {
                let parent = self
                    .get_parent()
                    .ok_or_else(|| anyhow!("No parent specified"))?
                    .get_id()?;
                let data_tag = req.default_data_license_tag.clone();
                let meta_tag = req.metadata_license_tag.clone();
                CreateRequest::check_license(data_tag, meta_tag, parent, client).await
            }
            CreateRequest::Object(req) => {
                let parent = self
                    .get_parent()
                    .ok_or_else(|| anyhow!("No parent specified"))?
                    .get_id()?;
                let data_tag = req.data_license_tag.clone();
                let meta_tag = req.metadata_license_tag.clone();
                CreateRequest::check_license(data_tag, meta_tag, parent, client).await
            }
        }
    }

    // Checks if licenses are specified
    // and if not tries to retrieve parent licenses
    async fn check_license(
        data: String,
        meta: String,
        parent: DieselUlid,
        client: &Client,
    ) -> Result<(String, String)> {
        match (meta.is_empty(), data.is_empty()) {
            // both not specified -> get parent licenses
            (true, true) => {
                let parent = Object::get(parent, client)
                    .await?
                    .ok_or_else(|| anyhow!("Parent not found"))?;
                Ok((parent.metadata_license, parent.data_license))
            }
            (true, false) => {
                let parent = Object::get(parent, client)
                    .await?
                    .ok_or_else(|| anyhow!("Parent not found"))?;
                if License::get(data.clone(), client).await?.is_some() {
                    Ok((parent.metadata_license, data.to_string()))
                } else {
                    Err(anyhow!("License invalid"))
                }
            }
            (false, true) => {
                let parent = Object::get(parent, client)
                    .await?
                    .ok_or_else(|| anyhow!("Parent not found"))?;
                if License::get(meta.clone(), client).await?.is_some() {
                    Ok((meta.to_string(), parent.data_license))
                } else {
                    Err(anyhow!("License invalid"))
                }
            }
            (false, false) => {
                if License::get(data.clone(), client).await?.is_some()
                    && License::get(meta.clone(), client).await?.is_some()
                {
                    Ok((meta.to_string(), data.to_string()))
                } else {
                    Err(anyhow!("Licenses invalid"))
                }
            }
        }
    }
}
