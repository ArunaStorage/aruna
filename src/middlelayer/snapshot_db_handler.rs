use crate::database::dsls::object_dsl::{Object, ObjectWithRelations};
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::snapshot_request_types::{SnapshotRequest, SnapshotResponse};
use anyhow::Result;

impl DatabaseHandler {
    pub async fn snapshot(&self, request: SnapshotRequest) -> Result<Vec<ObjectWithRelations>> {
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        let id = request.get_id()?;
        let resource = Object::get_object_with_relations(&id, transaction_client).await?;
        let snapshot_resources = match request {
            SnapshotRequest::Project(_) => SnapshotResponse::ArchiveProject(
                SnapshotRequest::get_archived_project(resource, transaction_client).await?,
            ),
            SnapshotRequest::Collection(_) => SnapshotResponse::SnapshotCollection(
                SnapshotRequest::get_cloned_collection(resource, transaction_client).await?,
            ),
            SnapshotRequest::Dataset(_) => SnapshotResponse::SnapshotDataset(
                SnapshotRequest::get_cloned_dataset(resource, transaction_client).await?,
            ),
        };
        let result = snapshot_resources.snapshot(transaction_client).await?;
        transaction.commit().await?;
        Ok(result)
    }
}
