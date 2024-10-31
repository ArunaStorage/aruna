use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::{constants::relation_types, context::Context, error::ArunaError, logerr, models::{models::Group, requests::{CreateGroupRequest, CreateGroupResponse}}, transactions::request::WriteRequest};

use super::{controller::Controller, request::{Request, Requester, SerializedResponse}};

impl Request for CreateGroupRequest {
    type Response = CreateGroupResponse;
    fn get_context(&self) -> &Context {
        &Context::UserOnly
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &super::controller::Controller,
    ) -> Result<Self::Response, ArunaError> {
        let request_tx = CreateGroupRequestTx {
            id: Ulid::new(),
            req: self,
            requester: requester.ok_or_else(|| ArunaError::Unauthorized).inspect_err(logerr!())?,
        };

        let response = controller.transaction(Ulid::new().0, &request_tx).await?;

        Ok(bincode::deserialize(&response)?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateGroupRequestTx {
    id: Ulid,
    req: CreateGroupRequest,
    requester: Requester,
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for CreateGroupRequestTx {
    async fn execute(
        &self,
        id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, ArunaError> {

        controller.authorize(&self.requester, &self.req).await?;

        let group = Group{
            id: self.id,
            name: self.req.name.clone(),
            description: self.req.description.clone(),
        };
        let requester_id = self.requester.get_id();

        let store = controller.get_store();
        Ok(tokio::task::spawn_blocking(move || {
            let store = store.write().expect("Failed to lock store");
            let mut wtxn = store.write_txn()?;

            let Some(user_idx) = store.get_idx_from_ulid(&requester_id, wtxn.get_txn()) else {
                return Err(ArunaError::NotFound(requester_id.to_string()));
            };

            // Create group
            let group_idx = store.create_node(&mut wtxn, id, &group)?;

            // Add relation user --ADMIN--> group
            store.create_relation(
                &mut wtxn,
                user_idx,
                group_idx,
                relation_types::PERMISSION_ADMIN,
            )?;

            wtxn.commit()?;
            // Create admin group, add user to admin group
            Ok::<_, ArunaError>(bincode::serialize(&CreateGroupResponse {
                group,
            })?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}




// use super::{
//     auth::Auth,
//     controller::{Controller, Get, Transaction},
//     transaction::{ArunaTransaction, Fields, Metadata, Requester, Requests, TransactionOk},
//     utils::get_group_field,
// };
// use crate::{
//     error::ArunaError,
//     models::{
//         self, PERMISSION_ADMIN, PERMISSION_APPEND, PERMISSION_NONE, PERMISSION_READ,
//         PERMISSION_WRITE, SHARES_PERMISSION,
//     },
// };
// use ulid::Ulid;

// pub trait WriteGroupRequestHandler: Transaction + Get + Auth {
//     async fn create_group(
//         &self,
//         token: Option<String>,
//         request: models::CreateGroupRequest,
//     ) -> Result<models::CreateGroupResponse, ArunaError> {
//         let transaction_id = u128::from_be_bytes(Ulid::new().to_bytes());

//         let requester = self
//             .authorize_token(token, &request)
//             .await?
//             .ok_or_else(|| {
//                 tracing::error!("Requester not found");
//                 ArunaError::Unauthorized
//             })?;

//         let TransactionOk::CreateGroupResponse(response) = self
//             .transaction(
//                 transaction_id,
//                 ArunaTransaction {
//                     request: Requests::CreateGroupRequest(request),
//                     metadata: Metadata { requester },
//                     generated_fields: Some(vec![Fields::GroupId(Ulid::new())]),
//                 },
//             )
//             .await?
//         else {
//             tracing::error!("Unexpected response: Not CreateGroupResponse");
//             return Err(ArunaError::TransactionFailure(
//                 "Unexpected response: Not CreateGroupResponse".to_string(),
//             ));
//         };
//         Ok(response)
//     }
// }
// pub trait ReadGroupHandler: Auth + Get {
//     async fn get_group(
//         &self,
//         token: Option<String>,
//         request: models::GetGroupRequest,
//     ) -> Result<models::GetGroupResponse, ArunaError> {
//         let _ = self.authorize_token(token, &request).await?;

//         let id = request.id;
//         let Some(models::NodeVariantValue::Group(group)) = self.get(id).await? else {
//             tracing::error!("Group not found");
//             return Err(ArunaError::NotFound(id.to_string()));
//         };
//         let members = self
//             .get_incoming_relations(
//                 models::NodeVariantId::Group(id),
//                 vec![
//                     PERMISSION_NONE,
//                     PERMISSION_READ,
//                     PERMISSION_WRITE,
//                     PERMISSION_ADMIN,
//                     PERMISSION_APPEND,
//                     SHARES_PERMISSION,
//                 ],
//             )
//             .await
//             .into_iter()
//             .map(|rel| *rel.source.get_ref())
//             .collect();

//         Ok(models::GetGroupResponse { group, members })
//     }
// }

// impl ReadGroupHandler for Controller {}

// impl WriteGroupRequestHandler for Controller {}

// pub trait WriteGroupExecuteHandler: Get + Auth {
//     async fn create_group(
//         &self,
//         request: models::CreateGroupRequest,
//         metadata: Metadata,
//         fields: Option<Vec<Fields>>,
//     ) -> Result<TransactionOk, ArunaError>;
// }

// impl WriteGroupExecuteHandler for Controller {
//     async fn create_group(
//         &self,
//         request: models::CreateGroupRequest,
//         metadata: Metadata,
//         fields: Option<Vec<Fields>>,
//     ) -> Result<TransactionOk, ArunaError> {
//         let Requester::User { user_id, .. } = metadata.requester else {
//             tracing::error!("User not found");
//             return Err(ArunaError::TransactionFailure("User not found".to_string()));
//         };
//         self.authorize(&metadata.requester, &request).await?;
//         let group_id = get_group_field(&fields)?;

//         let group = models::Group {
//             id: group_id,
//             name: request.name,
//             description: request.description,
//         };

//         let mut lock = self.store.write().await;

//         let env = lock.view_store.get_env();

//         lock.view_store
//             .add_node(models::NodeVariantValue::Group(group.clone()))?;
//         lock.graph.add_node(models::NodeVariantId::Group(group_id));
//         lock.graph
//             .add_relation(
//                 models::NodeVariantId::User(user_id),
//                 models::NodeVariantId::Group(group_id),
//                 PERMISSION_ADMIN,
//                 env,
//             )
//             .await?;
//         Ok(TransactionOk::CreateGroupResponse(
//             models::CreateGroupResponse { group },
//         ))
//     }
// }
