use std::collections::HashMap;

use aruna_server::{models::requests::GetEventsResponse, transactions::user::{CreateTokenRequestTx, RegisterUserRequestTx}};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use ulid::Ulid;

use crate::error::ComputeError;

pub struct State {
    pub client: reqwest::Client,
    token: String, // TODO: Remove this and think about realy authentication for a second
    pub events: HashMap<Ulid, TxnEnum>,
}

#[derive(Deserialize, Serialize, Debug)]
pub enum TxnEnum {
    RegisterUser(RegisterUserRequestTx),
    CreateToken(CreateTokenRequestTx),
}

impl State {
    pub async fn new(token: String) -> State {
        State {
            client: reqwest::Client::new(),
            token,
            events: HashMap::default(),
        }
    }
    pub async fn poll_notifications(&mut self) -> Result<(), ComputeError> {
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
                r#""RegisterUserRequestTx""# => {
                    TxnEnum::RegisterUser(serde_json::from_value(event.clone()).unwrap())
                },
                r#""CreateTokenRequestTx""# => {
                    TxnEnum::CreateToken(serde_json::from_value(event.clone()).unwrap())
                },
                _ => continue
            };
            self.events.insert(*id, tx);
        }
        Ok(())
    }
}
