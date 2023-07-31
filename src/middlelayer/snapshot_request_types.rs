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
    pub datasets: Vec<SnapshotDataset>,
    pub relations: Vec<InternalRelation>,
}

pub struct SnapshotProject {
    pub resource_ids: Vec<DieselUlid>,
    pub relation_ids: Vec<DieselUlid>,
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
        let mut result: Vec<ObjectWithRelations> = Vec::new();
        for relation in &project.relation_ids {
            InternalRelation::archive(relation, client).await?;
        }
        for resource in &project.resource_ids {
            Object::archive(resource, client).await?;
            result.push(Object::get_object_with_relations(resource, client).await?);
        }
        Ok(result)
    }
    async fn snapshot_dataset(
        dataset: &SnapshotDataset,
        client: &Client,
    ) -> Result<Vec<ObjectWithRelations>> {
        dataset.dataset.create(client).await?;
        if !dataset.relations.is_empty() {
            for relation in dataset.relations.clone() {
                relation.create(client).await?;
            }
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
        for relation in collection.relations.clone() {
            relation.create(client).await?;
        }
        let mut results: Vec<ObjectWithRelations> =
            vec![Object::get_object_with_relations(&collection.collection.id, client).await?];
        for dataset in collection.datasets.clone() {
            let mut dataset = SnapshotResponse::snapshot_dataset(
                &SnapshotDataset {
                    dataset: dataset.dataset,
                    relations: Vec::new(),
                },
                client,
            )
            .await?;
            results.append(&mut dataset);
        }
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
            relation_ids: SnapshotRequest::get_all_relation_ids(project.object.id, client).await?,
        })
    }
    pub async fn get_cloned_dataset(
        dataset: ObjectWithRelations,
        client: &Client,
    ) -> Result<SnapshotDataset> {
        let new_id = DieselUlid::generate();
        let mut relations =
            SnapshotRequest::get_other_relations(dataset.outbound.0, new_id, client).await?;
        let mut belongs_to: Vec<InternalRelation> = dataset
            .outbound_belongs_to
            .0
            .into_iter()
            .map(|r| r.1.clone_persistent(new_id))
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
        let mut datasets: Vec<SnapshotDataset> = Vec::new();
        let mut relations: Vec<InternalRelation> =
            SnapshotRequest::get_other_relations(collection.outbound.0, new_id, client).await?;
        for r in collection.outbound_belongs_to.0 {
            if r.1.target_type == ObjectType::OBJECT {
                relations.push(r.1);
            } else {
                let dataset = Object::get_object_with_relations(&r.1.target_pid, client).await?;
                let dataset_snapshot = SnapshotRequest::get_cloned_dataset(dataset, client).await?;
                datasets.push(dataset_snapshot);
            }
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
        let mut objects: Vec<DieselUlid> = Vec::new();
        for resource in project.outbound_belongs_to.0 {
            if resource.1.origin_type == ObjectType::COLLECTION {
                collections.push(resource.0);
            } else if resource.1.origin_type == ObjectType::DATASET {
                datasets.push(resource.0);
            } else {
                objects.push(resource.0);
            };
        }
        for collection in &collections {
            let object = Object::get_object_with_relations(collection, client).await?;
            for resource in object.outbound_belongs_to.0 {
                if resource.1.origin_type == ObjectType::DATASET {
                    datasets.push(resource.0);
                } else {
                    objects.push(resource.0);
                }
            }
        }
        for dataset in &datasets {
            let object = Object::get_object_with_relations(dataset, client).await?;
            for resource in object.outbound_belongs_to.0 {
                objects.push(resource.0);
            }
        }
        let mut results: Vec<DieselUlid> = vec![project.object.id];
        results.append(&mut collections);
        results.append(&mut datasets);
        results.append(&mut objects);
        Ok(results)
    }
    async fn get_all_relation_ids(
        project_id: DieselUlid,
        client: &Client,
    ) -> Result<Vec<DieselUlid>> {
        let mut relations: Vec<DieselUlid> = Vec::new();
        let (mut ids, mut init_in) = InternalRelation::get_all_by_id(project_id, client).await?;
        ids.append(&mut init_in);
        for id in ids.iter().map(|i| i.id) {
            let (outs, ins) = InternalRelation::get_all_by_id(id, client).await?;
            let mut outs = outs.into_iter().map(|i| i.id).collect();
            let mut ins = ins.into_iter().map(|i| i.id).collect();
            relations.append(&mut outs);
            relations.append(&mut ins);
        }
        Ok(relations)
    }
    async fn get_other_relations(
        iter: DashMap<DieselUlid, InternalRelation, RandomState>,
        new_id: DieselUlid,
        client: &Client,
    ) -> Result<Vec<InternalRelation>> {
        let mut relations: Vec<InternalRelation> = Vec::new();
        for r in iter {
            if r.1.target_type != ObjectType::OBJECT {
                let to_check = Object::get(r.0, client).await?;
                match to_check {
                    Some(o) => {
                        if o.dynamic {
                            relations.push(r.1.clone_dynamic(new_id));
                        } else {
                            relations.push(r.1.clone_persistent(new_id));
                        }
                    }
                    None => log::debug!("No resource found for target in {:?}", r.1),
                }
            } else {
                relations.push(r.1.clone_persistent(new_id));
            }
        }
        Ok(relations)
    }
}
