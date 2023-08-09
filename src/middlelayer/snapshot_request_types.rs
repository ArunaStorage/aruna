use crate::database::crud::CrudDb;
use crate::database::dsls::internal_relation_dsl::InternalRelation;
use crate::database::dsls::object_dsl::{Object, ObjectWithRelations};
use crate::database::enums::ObjectType;
use ahash::RandomState;
use anyhow::Result;
use aruna_rust_api::api::storage::services::v2::{
    ArchiveProjectRequest, SnapshotCollectionRequest, SnapshotDatasetRequest,
};
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use std::str::FromStr;
use tokio_postgres::Client;

pub enum SnapshotRequest {
    Project(ArchiveProjectRequest),
    Collection(SnapshotCollectionRequest),
    Dataset(SnapshotDatasetRequest),
}

pub enum SnapshotResponse {
    ArchiveProject(SnapshotProject),
    SnapshotCollection(SnapshotCollection),
    SnapshotDataset(SnapshotDataset),
}
#[derive(Clone)]
pub struct SnapshotDataset {
    pub dataset: Object,
    pub relations: Vec<InternalRelation>,
}

pub struct SnapshotCollection {
    pub collection: Object,
    pub datasets: Vec<Object>,
    pub relations: Vec<InternalRelation>,
}

pub struct SnapshotProject {
    pub resource_ids: Vec<DieselUlid>,
}

impl SnapshotResponse {
    pub async fn snapshot(&self, client: &Client) -> Result<Vec<ObjectWithRelations>> {
        let result = match self {
            SnapshotResponse::ArchiveProject(req) => {
                SnapshotResponse::archive_project(req, client).await?
            }
            SnapshotResponse::SnapshotCollection(req) => {
                SnapshotResponse::snapshot_collection(req, client).await?
            }
            SnapshotResponse::SnapshotDataset(req) => {
                SnapshotResponse::snapshot_dataset(req, client).await?
            }
        };
        Ok(result)
    }
    async fn archive_project(
        project: &SnapshotProject,
        client: &Client,
    ) -> Result<Vec<ObjectWithRelations>> {
        Object::archive(&project.resource_ids, client).await
    }
    async fn snapshot_dataset(
        dataset: &SnapshotDataset,
        client: &Client,
    ) -> Result<Vec<ObjectWithRelations>> {
        dataset.dataset.create(client).await?;
        if !dataset.relations.is_empty() {
            InternalRelation::batch_create(&dataset.relations, client).await?;
        }
        Ok(vec![
            Object::get_object_with_relations(&dataset.dataset.id, client).await?,
        ])
    }
    async fn snapshot_collection(
        collection: &SnapshotCollection,
        client: &Client,
    ) -> Result<Vec<ObjectWithRelations>> {
        collection.collection.create(client).await?;
        let mut updated: Vec<DieselUlid> = collection
            .datasets
            .clone()
            .into_iter()
            .map(|o| o.id)
            .collect();
        updated.push(collection.collection.id);
        Object::batch_create(&collection.datasets, client).await?;
        InternalRelation::batch_create(&collection.relations, client).await?;
        let results = Object::get_objects_with_relations(&updated, client).await?;
        Ok(results)
    }
}

impl SnapshotRequest {
    pub fn get_id(&self) -> Result<DieselUlid> {
        Ok(match self {
            SnapshotRequest::Project(req) => DieselUlid::from_str(&req.project_id)?,
            SnapshotRequest::Collection(req) => DieselUlid::from_str(&req.collection_id)?,
            SnapshotRequest::Dataset(req) => DieselUlid::from_str(&req.dataset_id)?,
        })
    }
    pub async fn get_archived_project(
        project: ObjectWithRelations,
        client: &Client,
    ) -> Result<SnapshotProject> {
        Ok(SnapshotProject {
            resource_ids: SnapshotRequest::get_all_resource_ids(project.clone(), client).await?,
        })
    }
    pub async fn get_cloned_dataset(dataset: ObjectWithRelations) -> Result<SnapshotDataset> {
        let new_id = DieselUlid::generate();
        let mut relations =
            SnapshotRequest::get_other_relations(dataset.outbound.0, new_id).await?;
        let mut belongs_to: Vec<InternalRelation> = dataset
            .outbound_belongs_to
            .0
            .into_iter()
            .map(|r| r.1.clone_relation(&new_id))
            .collect();
        relations.append(&mut belongs_to);
        Ok(SnapshotDataset {
            dataset: dataset.object.get_cloned_persistent(new_id),
            relations,
        })
    }

    pub async fn get_cloned_collection(
        collection: ObjectWithRelations,
        client: &Client,
    ) -> Result<SnapshotCollection> {
        let new_id = DieselUlid::generate();
        let mut dataset_ulids: Vec<DieselUlid> = Vec::new();
        let mut relations: Vec<InternalRelation> =
            SnapshotRequest::get_other_relations(collection.outbound.0, new_id).await?;
        for r in collection.outbound_belongs_to.0 {
            if r.1.target_type == ObjectType::OBJECT {
                relations.push(r.1);
            } else {
                dataset_ulids.push(r.1.target_pid);
            }
        }
        let datasets_with_relations =
            Object::get_objects_with_relations(&dataset_ulids, client).await?;
        let mut datasets = Vec::new();
        for d in datasets_with_relations {
            let mut outbound = d.outbound.0.into_iter().map(|r| r.1).collect();
            let mut outbound_belongs_to =
                d.outbound_belongs_to.0.into_iter().map(|r| r.1).collect();
            relations.append(&mut outbound);
            relations.append(&mut outbound_belongs_to);
            datasets.push(d.object);
        }
        Ok(SnapshotCollection {
            collection: collection.object.get_cloned_persistent(new_id),
            datasets,
            relations,
        })
    }
    async fn get_all_resource_ids(
        project: ObjectWithRelations,
        client: &Client,
    ) -> Result<Vec<DieselUlid>> {
        let mut collections: Vec<DieselUlid> = Vec::new();
        let mut datasets: Vec<DieselUlid> = Vec::new();
        for resource in project.outbound_belongs_to.0 {
            if resource.1.origin_type == ObjectType::COLLECTION {
                collections.push(resource.0);
            } else if resource.1.origin_type == ObjectType::DATASET {
                datasets.push(resource.0);
            } else {
                continue;
            };
        }
        let collections_with_relations =
            Object::get_objects_with_relations(&collections, client).await?;
        for collection in &collections_with_relations {
            for resource in &collection.outbound_belongs_to.0 {
                if resource.target_type == ObjectType::DATASET {
                    datasets.push(resource.target_pid);
                }
            }
        }
        let mut results: Vec<DieselUlid> = vec![project.object.id];
        results.append(&mut collections);
        results.append(&mut datasets);
        Ok(results)
    }
    async fn get_other_relations(
        relations: DashMap<DieselUlid, InternalRelation, RandomState>,
        new_id: DieselUlid,
    ) -> Result<Vec<InternalRelation>> {
        Ok(relations
            .into_iter()
            .map(|r| r.1.clone_relation(&new_id))
            .collect())
    }
}
