use super::{
    controller::Controller,
    request::{Request, Requester, WriteRequest},
};
use crate::{
    constants::relation_types::{self, GROUP_PART_OF_REALM, REALM_USES_COMPONENT},
    context::Context,
    error::ArunaError,
    models::{
        models::Component,
        requests::{CreateComponentRequest, CreateComponentResponse},
    },
    transactions::request::SerializedResponse,
};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

impl Request for CreateComponentRequest {
    type Response = CreateComponentResponse;
    fn get_context(&self) -> Context {
        Context::UserOnly
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        let request_tx = CreateComponentRequestTx {
            id: Ulid::new(),
            req: self,
            requester: requester.ok_or_else(|| ArunaError::Unauthorized)?,
        };

        let response = controller.transaction(Ulid::new().0, &request_tx).await?;

        Ok(bincode::deserialize(&response)?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateComponentRequestTx {
    id: Ulid,
    req: CreateComponentRequest,
    requester: Requester,
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for CreateComponentRequestTx {
    async fn execute(
        &self,
        associated_event_id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, crate::error::ArunaError> {
        controller.authorize(&self.requester, &self.req).await?;
        let store = controller.get_store();
        let requester = self.requester.clone();
        let req = self.req.clone();
        let id = self.id;
        Ok(tokio::task::spawn_blocking(move || {
            // Create realm, add user to realm

            let mut wtxn = store.write_txn()?;

            let Some(requester_idx) = requester
                .get_id()
                .map(|id| store.get_idx_from_ulid(&id, wtxn.get_txn()))
                .flatten()
            else {
                return Err(ArunaError::Unauthorized);
            };

            let component = Component {
                id,
                name: req.name.clone(),
                description: req.description.clone(),
                component_type: req.component_type,
                endpoints: req.endpoints.clone(),
                public: req.public,
            };

            let idx = store.create_node(&mut wtxn, &component)?;

            // Add relation realm --USES COMPONENT--> component
            store.create_relation(&mut wtxn, idx, requester_idx, relation_types::OWNED_BY_USER)?;

            // Add a listener if the component is a proxy
            store.add_read_permission_universe(&mut wtxn, requester_idx, &[idx])?;
            if component.public {
                store.add_public_resources_universe(&mut wtxn, &[idx])?;
            }

            // Affected nodes: Realm and Group
            store.add_event_to_subscribers(&mut wtxn, associated_event_id, &[requester_idx])?;
            store.register_event(&mut wtxn, associated_event_id, &[requester_idx])?;

            wtxn.commit()?;
            // Create admin group, add user to admin group
            Ok::<_, ArunaError>(bincode::serialize(&CreateComponentResponse { component })?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}
