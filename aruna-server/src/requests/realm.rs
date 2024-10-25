// use super::{
//     auth::Auth,
//     controller::{Controller, Get, Transaction},
//     transaction::{ArunaTransaction, TransactionOk},
//     utils::{get_group_field, get_realm_field},
// };
// use crate::{
//     error::ArunaError,
//     models::{self, GROUP_ADMINISTRATES_REALM, GROUP_PART_OF_REALM, PERMISSION_ADMIN},
//     requests::transaction::{Fields, Metadata, Requester, Requests},
// };
// use ulid::Ulid;

use std::sync::Arc;

use super::{
    controller::Controller,
    request::{Request, Requester, WriteRequest},
};
use crate::{
    context::Context,
    error::ArunaError,
    models::{CreateRealmRequest, CreateRealmResponse},
    requests::transaction::ArunaTransaction,
};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

impl Request for CreateRealmRequest {
    type Response = CreateRealmResponse;
    fn get_context(&self) -> &Context {
        unimplemented!()
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        let request_tx = CreateRealmRequestTx {
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
pub struct CreateRealmRequestTx {
    id: Ulid,
    req: CreateRealmRequest,
    requester: Requester,
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for CreateRealmRequestTx {
    async fn execute(
        &self,
        controller: &Controller,
    ) -> Result<super::request::SerializedResponse, crate::error::ArunaError> {
        tokio::task::spawn_blocking(move || {});
        unimplemented!()
    }
}

// pub trait WriteRealmRequestHandler: Transaction + Get + Auth {
//     async fn create_realm(
//         &self,
//         token: Option<String>,
//         request: models::CreateRealmRequest,
//     ) -> Result<models::CreateRealmResponse, ArunaError> {
//         let transaction_id = u128::from_be_bytes(Ulid::new().to_bytes());

//         // TODO: Auth

//         let requester = self
//             .authorize_token(token, &request)
//             .await?
//             .ok_or_else(|| {
//                 tracing::error!("Requester not found");
//                 ArunaError::Unauthorized
//             })?;

//         let TransactionOk::CreateRealmResponse(response) = self
//             .transaction(
//                 transaction_id,
//                 ArunaTransaction {
//                     request: Requests::CreateRealmRequest(request),
//                     metadata: Metadata { requester },
//                     generated_fields: Some(vec![
//                         Fields::RealmId(Ulid::new()),
//                         Fields::GroupId(Ulid::new()),
//                     ]),
//                 },
//             )
//             .await?
//         else {
//             tracing::error!("Unexpected response: Not CreateRealmResponse");
//             return Err(ArunaError::TransactionFailure(
//                 "Unexpected response: Not CreateRealmResponse".to_string(),
//             ));
//         };
//         Ok(response)
//     }
//     async fn add_group(
//         &self,
//         token: Option<String>,
//         request: models::AddGroupRequest,
//     ) -> Result<models::AddGroupResponse, ArunaError> {
//         let transaction_id = u128::from_be_bytes(Ulid::new().to_bytes());

//         let requester = self
//             .authorize_token(token, &request)
//             .await?
//             .ok_or_else(|| {
//                 tracing::error!("Requester not found");
//                 ArunaError::Unauthorized
//             })?;

//         let TransactionOk::AddGroupResponse(response) = self
//             .transaction(
//                 transaction_id,
//                 ArunaTransaction {
//                     request: Requests::AddGroupRequest(request),
//                     metadata: Metadata { requester },
//                     generated_fields: None,
//                 },
//             )
//             .await?
//         else {
//             tracing::error!("Unexpected response: Not AddGroupResponse");
//             return Err(ArunaError::TransactionFailure(
//                 "Unexpected response: Not AddGroupResponse".to_string(),
//             ));
//         };
//         Ok(response)
//     }
// }

// pub trait ReadRealmHandler: Auth + Get {
//     async fn get_realm(
//         &self,
//         token: Option<String>,
//         request: models::GetRealmRequest,
//     ) -> Result<models::GetRealmResponse, ArunaError> {
//         let _ = self.authorize_token(token, &request).await?;

//         let id = request.id;
//         let Some(models::NodeVariantValue::Realm(realm)) = self.get(id).await? else {
//             tracing::error!("Realm not found");
//             return Err(ArunaError::NotFound(id.to_string()));
//         };
//         let groups = self
//             .get_incoming_relations(
//                 models::NodeVariantId::Realm(id),
//                 vec![GROUP_PART_OF_REALM, GROUP_ADMINISTRATES_REALM],
//             )
//             .await
//             .into_iter()
//             .map(|rel| *rel.source.get_ref())
//             .collect();

//         Ok(models::GetRealmResponse { realm, groups })
//     }
// }

// impl ReadRealmHandler for Controller {}

// impl WriteRealmRequestHandler for Controller {}

// pub trait WriteRealmExecuteHandler: Get + Auth {
//     async fn create_realm(
//         &self,
//         request: models::CreateRealmRequest,
//         metadata: Metadata,
//         fields: Option<Vec<Fields>>,
//     ) -> Result<TransactionOk, ArunaError>;
//     async fn add_group(
//         &self,
//         request: models::AddGroupRequest,
//         metadata: Metadata,
//         fields: Option<Vec<Fields>>,
//     ) -> Result<TransactionOk, ArunaError>;
// }

// impl WriteRealmExecuteHandler for Controller {
//     async fn create_realm(
//         &self,
//         request: models::CreateRealmRequest,
//         metadata: Metadata,
//         fields: Option<Vec<Fields>>,
//     ) -> Result<TransactionOk, ArunaError> {
//         let Requester::User { user_id, .. } = metadata.requester else {
//             tracing::error!("User not found");
//             return Err(ArunaError::TransactionFailure("User not found".to_string()));
//         };
//         self.authorize(&metadata.requester, &request).await?;

//         let realm_id = get_realm_field(&fields)?;
//         let group_id = get_group_field(&fields)?;

//         let realm = models::Realm {
//             id: realm_id,
//             tag: request.tag.clone(),
//             name: request.name,
//             description: request.description,
//         };
//         let admin_group = models::Group {
//             id: group_id,
//             name: format!("{}-admin-group", request.tag),
//             description: String::new(),
//         };

//         let mut lock = self.store.write().await;
//         let env = lock.view_store.get_env();

//         // Add nodes
//         lock.view_store
//             .add_node(models::NodeVariantValue::Realm(realm.clone()))?;
//         lock.view_store
//             .add_node(models::NodeVariantValue::Group(admin_group))?;
//         lock.graph.add_node(models::NodeVariantId::Group(group_id));
//         lock.graph.add_node(models::NodeVariantId::Realm(realm_id));

//         // Add relations
//         lock.graph
//             .add_relation(
//                 models::NodeVariantId::User(user_id),
//                 models::NodeVariantId::Group(group_id),
//                 PERMISSION_ADMIN,
//                 env.clone(),
//             )
//             .await?;
//         lock.graph
//             .add_relation(
//                 models::NodeVariantId::Group(group_id),
//                 models::NodeVariantId::Realm(realm_id),
//                 GROUP_ADMINISTRATES_REALM,
//                 env,
//             )
//             .await?;

//         Ok(TransactionOk::CreateRealmResponse(
//             models::CreateRealmResponse {
//                 realm,
//                 admin_group_id: group_id,
//             },
//         ))
//     }

//     async fn add_group(
//         &self,
//         request: models::AddGroupRequest,
//         metadata: Metadata,
//         _fields: Option<Vec<Fields>>,
//     ) -> Result<TransactionOk, ArunaError> {
//         self.authorize(&metadata.requester, &request).await?;

//         let mut lock = self.store.write().await;
//         let env = lock.view_store.get_env();
//         lock.graph
//             .add_relation(
//                 models::NodeVariantId::Group(request.group_id),
//                 models::NodeVariantId::Realm(request.realm_id),
//                 GROUP_PART_OF_REALM,
//                 env.clone(),
//             )
//             .await?;

//         Ok(TransactionOk::AddGroupResponse(models::AddGroupResponse {}))
//     }
// }
