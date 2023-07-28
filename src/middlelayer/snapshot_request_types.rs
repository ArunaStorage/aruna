use crate::auth::structs::Context;
use crate::database::dsls::internal_relation_dsl::InternalRelation;
use crate::database::dsls::object_dsl::{ExternalRelation, Object, ObjectWithRelations};
use crate::database::enums::DbPermissionLevel;
use crate::database::enums::ObjectType;
use anyhow::Result;
use aruna_rust_api::api::storage::services::v2::{
    ArchiveProjectRequest, SnapshotCollectionRequest, SnapshotDatasetRequest,
};
use diesel_ulid::DieselUlid;
use std::str::FromStr;

pub enum SnapshotRequest {
    Project(ArchiveProjectRequest),
    Collection(SnapshotCollectionRequest),
    Dataset(SnapshotDatasetRequest),
}

pub struct SnapshotDataset {
    pub dataset: Object,
    pub relations: Vec<InternalRelation>,
}

pub struct SnapshotCollection {
    pub collection: Object,
    pub datasets: Vec<SnapshotDataset>,
    pub relations: Vec<InternalRelation>,
}

pub struct SnapshotProject {
    pub resource_ids: Vec<DieselUlid>,
    pub relation_ids: Vec<DieselUlid>,
}

impl SnapshotRequest {
    pub fn get_id(&self) -> Result<DieselUlid> {
        Ok(match self {
            SnapshotRequest::Project(req) => DieselUlid::from_str(&req.project_id)?,
            SnapshotRequest::Collection(req) => DieselUlid::from_str(&req.collection_id)?,
            SnapshotRequest::Dataset(req) => DieselUlid::from_str(&req.dataset_id)?,
        })
    }
}
