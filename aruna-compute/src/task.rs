use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};

use ahash::RandomState;
use aruna_server::models::{models::Component, requests::GetRealmComponentsResponse};
use tracing::error;
use ulid::Ulid;

use crate::{error::ComputeError, poll::State};

pub struct Config {
    pub components: HashMap<Ulid, Component, RandomState>,
}

#[tracing::instrument(level = "trace", skip(state))]
pub async fn start_server(
    mut state: State,
    frequency: u64,
    realm_id: Ulid,
) -> Result<(), ComputeError> {
    let mut task_queue = VecDeque::new();

    let server_config: GetRealmComponentsResponse = state
        .client
        .get(format!(
            "http://localhost:8080/api/v3/realms/{}/components",
            Ulid::new()
        ))
        .bearer_auth(state.token.clone())
        .query(&[("subscriber_id", "01JER6JBQ39EDVR2M7A6Z9512B")])
        .send()
        .await?
        .json()
        .await?;
    let mut config = Config {
        components: HashMap::default(),
    };
    for component in server_config.components {
        config.components.insert(component.id, component);
    }

    loop {
        state.poll_notifications(&mut task_queue).await?;
        while let Some((id, event)) = task_queue.pop_front() {
            match event {
                crate::poll::TxnEnum::RegisterData(value) => {
                    let endpoint = if let Some(component_id) = value.get("component_id") {
                        let id = Ulid::from_string(&serde_json::to_string(&component_id).unwrap())
                            .unwrap();
                        let Some(component) = config.components.get(&id) else {
                            error!("Component not found");
                            continue;
                        };
                        let Some(ep) = component
                            .endpoints
                            .iter()
                            .find(|ep| matches!(ep, aruna_server::models::models::Endpoint::S3(_))) else {
                            error!("S3 endpoint not found");
                            continue;
                        };
                        ep
                    } else {
                        error!("Could not find server_id");
                        continue;
                    };
                }
                crate::poll::TxnEnum::CreateProject(value) => todo!(),
                crate::poll::TxnEnum::CreateResource(value) => todo!(),
                crate::poll::TxnEnum::CreateResourceBatch(value) => todo!(),
                crate::poll::TxnEnum::UpdateResource(value) => todo!(),
            }
        }
        tokio::time::sleep(Duration::from_secs(frequency)).await;
    }
}
