use tracing::error;
use ulid::Ulid;

use super::{
    controller::Controller,
    request::{Request, Requester, WriteRequest},
};
use crate::{
    context::Context,
    error::ArunaError,
    models::requests::{GetEventsRequest, GetEventsResponse},
};

impl Request for GetEventsRequest {
    type Response = GetEventsResponse;

    fn get_context<'a>(&'a self) -> Context {
        Context::SubscriberOwnerOf(self.subscriber_id)
    }

    async fn run_request(
        self,
        _requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        let store = controller.store.clone();
        let Some(node) = controller.node.blocking_read().clone() else {
            return Err(ArunaError::ServerError("Node not set".to_string()));
        };

        let response = tokio::task::spawn_blocking(move || {
            let mut wtxn = store.write_txn()?;

            let event_ids = store
                .get_events_subscriber(
                    &mut wtxn,
                    self.subscriber_id.0,
                    self.acknowledge_from.map(|x| x.0),
                )
                .map_err(|e| {
                    error!("Error getting events: {:?}", e);
                    ArunaError::ServerError("Error getting events".to_string())
                })?;

            let mut events = vec![];
            for event in event_ids.iter() {
                let event = node.get_event_by_id(*event).ok_or_else(|| {
                    error!("Event not found");
                    ArunaError::NotFound("Event not found".to_string())
                })?;

                let dyn_event: Box<dyn WriteRequest> = bincode::deserialize(&event.transaction)
                    .map_err(|e| {
                        error!("Error deserializing event: {:?}", e);
                        ArunaError::ServerError("Error deserializing event".to_string())
                    })?;

                let json_event = serde_json::to_value(dyn_event).map_err(|e| {
                    error!("Error serializing event: {:?}", e);
                    ArunaError::ServerError("Error serializing event".to_string())
                })?;
                events.push((Ulid::from(event.id), json_event))
            }

            Ok::<_, ArunaError>(GetEventsResponse { events })
        })
        .await
        .map_err(|e| {
            error!("Error getting events: {:?}", e);
            ArunaError::ServerError("Error getting events".to_string())
        })??;
        Ok(response)
    }
}
