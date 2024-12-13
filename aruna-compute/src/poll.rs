use std::collections::{HashMap, VecDeque};

use aruna_server::models::requests::GetEventsResponse;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use ulid::Ulid;

use crate::error::ComputeError;

pub struct State {
    pub client: reqwest::Client,
    pub token: String, // TODO: Remove this and think about realy authentication for a second
}

#[derive(Deserialize, Serialize, Debug)]
pub enum TxnEnum {
    RegisterData(Value),
    CreateProject(Value),
    CreateResource(Value),
    CreateResourceBatch(Value),
    UpdateResource(Value),
}

impl State {
    #[tracing::instrument(level = "trace", skip(token))]
    pub async fn new(token: String) -> State {
        State {
            client: reqwest::Client::new(),
            token,
        }
    }
    #[tracing::instrument(level = "trace", skip(self, queue))]
    pub async fn poll_notifications(
        &mut self,
        queue: &mut VecDeque<(Ulid, TxnEnum)>,
    ) -> Result<(), ComputeError> {
        let GetEventsResponse { events: event_map } = self
            .client
            .get("http://localhost:8080/api/v3/info/events")
            .bearer_auth(self.token.clone())
            .query(&[("subscriber_id", "01JER6JBQ39EDVR2M7A6Z9512B")])
            .send()
            .await?
            .json()
            .await?;
        println!("{:?}", event_map);
        let events: HashMap<Ulid, Value, ahash::RandomState> =
            serde_json::from_value(serde_json::to_value(&event_map).unwrap()).unwrap();
        for (id, event) in events.iter() {
            let variant = event.get("type").unwrap().to_string();
            let tx = match variant.as_str() {
                r#""CreateProjectRequestTx""# => TxnEnum::CreateProject(event.clone()),
                r#""CreateResourceRequestTx""# => TxnEnum::CreateResource(event.clone()),
                r#""CreateResourceBatchRequestTx""# => TxnEnum::CreateResource(event.clone()),
                r#""UpdateResourceTx""# => TxnEnum::UpdateResource(event.clone()),
                r#""RegisterDataRequestTx""# => TxnEnum::RegisterData(event.clone()),
                _ => continue,
            };
            queue.push_back((*id, tx));
        }
        Ok(())
    }
}
