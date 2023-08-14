use crate::database::dsls::object_dsl::{Object, ObjectWithRelations};
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::snapshot_request_types::{SnapshotRequest, SnapshotResponse};
use anyhow::Result;
use diesel_ulid::DieselUlid;

impl DatabaseHandler {
    pub async fn snapshot(
        &self,
        request: SnapshotRequest,
    ) -> Result<(DieselUlid, Vec<ObjectWithRelations>)> {
        let client = self.database.get_client().await?;
        let id = request.get_id()?;
        let resource = Object::get_object_with_relations(&id, &client).await?;
        let (new_object_id, snapshot_resources) = match request {
            SnapshotRequest::Project(_) => (
                id,
                SnapshotResponse::ArchiveProject(
                    SnapshotRequest::get_archived_project(resource, &client).await?,
                ),
            ),
            SnapshotRequest::Collection(_) => {
                let request = SnapshotRequest::get_cloned_collection(resource, &client).await?;
                (
                    request.collection.id,
                    SnapshotResponse::SnapshotCollection(request),
                )
            }
            SnapshotRequest::Dataset(_) => {
                let request = SnapshotRequest::get_cloned_dataset(resource).await?;
                (
                    request.dataset.id,
                    SnapshotResponse::SnapshotDataset(request),
                )
            }
        };
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        let result = snapshot_resources.snapshot(transaction_client).await?;
        transaction.commit().await?;
        Ok((new_object_id, result))
    }
}
