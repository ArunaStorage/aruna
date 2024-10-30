use super::{
    controller::Controller,
    request::{Request, Requester, WriteRequest},
};
use crate::{
    constants::relation_types,
    context::Context,
    error::ArunaError,
    models::{
        models::{Realm, User},
        requests::{CreateRealmResponse, RegisterUserRequest, RegisterUserResponse},
    },
    transactions::{request::SerializedResponse, transaction::ArunaTransaction},
};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

impl Request for RegisterUserRequest {
    type Response = RegisterUserResponse;
    fn get_context(&self) -> &Context {
        &Context::UserOnly
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        let request_tx = RegisterUserRequestTx {
            id: Ulid::new(),
            req: self,
            requester: requester.ok_or_else(|| ArunaError::Unauthorized)?,
        };

        let response = controller
            .transaction(
                Ulid::new().0,
                ArunaTransaction(bincode::serialize(&request_tx)?),
            )
            .await?;

        Ok(bincode::deserialize(&response)?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterUserRequestTx {
    id: Ulid,
    req: RegisterUserRequest,
    requester: Requester,
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for RegisterUserRequestTx {
    async fn execute(
        &self,
        id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, crate::error::ArunaError> {
        controller.authorize(&self.requester, &self.req).await?;

        let user = User {
            id: self.id,
            first_name: self.req.first_name.clone(),
            last_name: self.req.last_name.clone(),
            email: self.req.email.clone(),
            // TODO: Multiple identifiers ?
            identifiers: self.req.identifier.clone(),
            global_admin: false,
        };

        // TODO: Extract OIDC context from Requester -> Add oidc mapping support

        let store = controller.get_store();
        Ok(tokio::task::spawn_blocking(move || {
            let store = store.write().expect("Failed to lock store");
            let mut wtxn = store.write_txn()?;

            // Create user
            let _user_idx = store.create_node(&mut wtxn, id, &user)?;

            wtxn.commit()?;
            // Create admin group, add user to admin group
            Ok::<_, ArunaError>(bincode::serialize(&RegisterUserResponse { user })?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}
