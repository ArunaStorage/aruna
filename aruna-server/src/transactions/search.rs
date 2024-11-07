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
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, crate::error::ArunaError> {
        // TODO: Create universe filter for auth

        let store = controller.get_store();

        let query = self.query.clone();
        let filter = self.filter.clone();
        let offset = self.offset.unwrap_or(0);
        let limit = self.limit.unwrap_or(20);

        tokio::task::spawn_blocking(move || {
            let rtxn = store.read_txn()?;

            let universe = match requester {
                Some(requester) => {
                    let user_idx = store
                        .get_idx_from_ulid(&requester.get_id(), &rtxn)
                        .ok_or_else(|| ArunaError::NotFound("Requester not found".to_string()))?;
                    let groups = store.get_groups_for_user(user_idx);
                    let mut universe = store.get_read_permission_universe(&rtxn, &groups)?;
                    universe |= store.get_public_universe(&rtxn)?;
                    universe
                }
                None => store.get_public_universe(&rtxn)?,
            };
            let (expected_hits, result) =
                store.search(query, offset, limit, filter.as_deref(), &rtxn, universe)?;

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
