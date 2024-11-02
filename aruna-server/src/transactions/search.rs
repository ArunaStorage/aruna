use crate::{
    context::Context,
    error::ArunaError,
    models::requests::{SearchRequest, SearchResponse},
};

use super::{
    controller::Controller,
    request::{Request, Requester},
};

impl Request for SearchRequest {
    type Response = SearchResponse;

    fn get_context<'a>(&'a self) -> Context {
        Context::Public
    }

    async fn run_request(
        self,
        _requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, crate::error::ArunaError> {
        // TODO: Create universe filter for auth
        let store = controller.get_store();

        let query = self.query.clone();
        let filter = self.filter.clone();
        let offset = self.offset.unwrap_or(0);
        let limit = self.limit.unwrap_or(20);

        tokio::task::spawn_blocking(move || {
            let store_lock = store.read().expect("Poisoned lock on store");

            let rtxn = store_lock.read_txn()?;
            let (expected_hits, result) =
                store_lock.search(query, offset, limit, filter.as_deref(), &rtxn)?;

            Ok(SearchResponse {
                expected_hits,
                resources: result,
            })
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })?
    }
}
