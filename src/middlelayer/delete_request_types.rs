use anyhow::Result;
use aruna_rust_api::api::storage::services::v2::{
    DeleteCollectionRequest, DeleteDatasetRequest, DeleteObjectRequest, DeleteProjectRequest,
};
use diesel_ulid::DieselUlid;
use std::str::FromStr;

pub enum DeleteRequest {
    Project(DeleteProjectRequest),
    Collection(DeleteCollectionRequest),
    Dataset(DeleteDatasetRequest),
    Object(DeleteObjectRequest),
}

impl DeleteRequest {
    pub fn get_id(&self) -> Result<DieselUlid> {
        let id = match &self {
            DeleteRequest::Project(req) => DieselUlid::from_str(&req.project_id)?,
            DeleteRequest::Collection(req) => DieselUlid::from_str(&req.collection_id)?,
            DeleteRequest::Dataset(req) => DieselUlid::from_str(&req.dataset_id)?,
            DeleteRequest::Object(req) => DieselUlid::from_str(&req.object_id)?,
        };
        Ok(id)
    }
}
