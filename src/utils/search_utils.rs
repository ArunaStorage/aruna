use crate::database::connection::Database;
use crate::database::crud::CrudDb;
use crate::database::dsls::object_dsl::Object;
use crate::database::enums::DataClass;
use crate::search::meilisearch_client::{MeilisearchClient, MeilisearchIndexes, ObjectDocument};
use itertools::Itertools;
use std::sync::Arc;

/// Updates the resource search index in a background thread.
pub async fn update_search_index(
    search_client: &Arc<MeilisearchClient>,
    index_updates: Vec<ObjectDocument>,
) {
    // Remove confidential/workspace objects
    let final_updates = index_updates
        .into_iter()
        .filter_map(|od| match od.data_class {
            DataClass::PUBLIC | DataClass::PRIVATE => Some(od),
            _ => None,
        })
        .collect::<Vec<_>>();

    // Update remaining objects in search index
    let client_clone = search_client.clone();
    tokio::spawn(async move {
        if let Err(err) = client_clone
            .add_or_update_stuff::<ObjectDocument>(
                final_updates.as_slice(),
                MeilisearchIndexes::OBJECT,
            )
            .await
        {
            log::warn!("Search index update failed: {}", err)
        }
    });
}

/// Fetches all Objects from the database and full syncs the search index in
/// chunks of 100.000 elements.
pub async fn full_sync_search_index(
    database_conn: Arc<Database>,
    search_client: Arc<MeilisearchClient>,
) -> anyhow::Result<()> {
    let client = database_conn.get_client().await?; // No transaction; only read
    let filtered_objects: Vec<ObjectDocument> = Object::all(&client)
        .await?
        .into_iter()
        .filter_map(|o| match o.data_class {
            DataClass::PUBLIC | DataClass::PRIVATE => Some(o),
            _ => None,
        })
        .map(|o| o.into())
        .collect_vec();

    // Update search index in chunks of 100.000 Objects
    for chunk in filtered_objects.chunks(100000) {
        search_client
            .add_or_update_stuff::<ObjectDocument>(chunk, MeilisearchIndexes::OBJECT)
            .await?;
    }

    Ok(())
}
