use crate::database::crud::CrudDb;
use crate::database::dsls::internal_relation_dsl::InternalRelation;
use crate::database::dsls::object_dsl::{Object, ObjectWithRelations};
use crate::database::enums::ObjectType;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::snapshot_request_types::{
    SnapshotCollection, SnapshotDataset, SnapshotProject, SnapshotRequest,
};
use anyhow::{anyhow, Result};
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use log::debug;
use tokio_postgres::Client;

impl DatabaseHandler {
    pub async fn snapshot(&self, request: SnapshotRequest) -> Result<Vec<ObjectWithRelations>> {
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        let id = request.get_id()?;
        let resource = Object::get_object_with_relations(&id, transaction_client).await?;
        // 1. Clone resource with all InternalRelations

        // 1.2 Clone InternalRelations
        let relations = match request {
            SnapshotRequest::Project(req) => {
                todo!()
            }
            SnapshotRequest::Collection(_) => {
                let snapshot_collection =
                    DatabaseHandler::get_cloned_collection(resource, transaction_client).await?;
            }
            SnapshotRequest::Dataset(_) => {
                let snapshot_dataset =
                    DatabaseHandler::get_cloned_dataset(resource, transaction_client).await?;
            }
        };
        // 2. New InternalRelation with version_type to clone
        // 3. Persist all InternalRelations from snapshot resource
        todo!()
    }
    async fn get_cloned_dataset(
        dataset: ObjectWithRelations,
        client: &Client,
    ) -> Result<SnapshotDataset> {
        let new_id = DieselUlid::generate();
        let mut relations =
            DatabaseHandler::get_other_relations(dataset.outbound.0, new_id, client).await?;
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

    async fn get_cloned_collection(
        collection: ObjectWithRelations,
        client: &Client,
    ) -> Result<SnapshotCollection> {
        let new_id = DieselUlid::generate();
        let mut datasets: Vec<SnapshotDataset> = Vec::new();
        let mut relations: Vec<InternalRelation> =
            DatabaseHandler::get_other_relations(collection.outbound.0, new_id, client).await?;
        for r in collection.outbound_belongs_to.0 {
            if r.1.target_type == ObjectType::OBJECT {
                relations.push(r.1);
            } else {
                let dataset = Object::get_object_with_relations(&r.1.target_pid, client).await?;
                let dataset_snapshot = DatabaseHandler::get_cloned_dataset(dataset, client).await?;
                datasets.push(dataset_snapshot);
            }
        }
        Ok(SnapshotCollection {
            collection: collection.object.get_cloned_persistent(new_id),
            datasets,
            relations,
        })
    }
    async fn get_project_ids(
        project: ObjectWithRelations,
        client: &Client,
    ) -> Result<SnapshotProject> {
        todo!()
    }
    async fn get_other_relations(
        iter: DashMap<DieselUlid, InternalRelation>,
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
                    None => debug!("No resource found for target in {:?}", r.1),
                }
            } else {
                relations.push(r.1.clone_persistent(new_id));
            }
        }
        Ok(relations)
    }
}
