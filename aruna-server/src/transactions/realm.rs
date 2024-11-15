use super::{
    controller::Controller,
    request::{Request, Requester, WriteRequest},
};
use crate::{
    constants::relation_types::{self, GROUP_PART_OF_REALM},
    context::Context,
    error::ArunaError,
    models::{
        models::{Group, Realm},
        requests::{
            AddGroupRequest, AddGroupResponse, CreateRealmRequest, CreateRealmResponse, GetGroupsFromRealmRequest, GetGroupsFromRealmResponse, GetRealmRequest, GetRealmResponse
        },
    },
    transactions::request::SerializedResponse,
};
use petgraph::Direction;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

impl Request for CreateRealmRequest {
    type Response = CreateRealmResponse;
    fn get_context(&self) -> Context {
        Context::UserOnly
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        let request_tx = CreateRealmRequestTx {
            id: Ulid::new(),
            generated_group: Group {
                id: Ulid::new(),
                name: format!("{}-admin-group", self.tag),
                description: format!("Auto-generated admin group for: {}", self.name),
            },
            req: self,
            requester: requester.ok_or_else(|| ArunaError::Unauthorized)?,
        };

        let response = controller.transaction(Ulid::new().0, &request_tx).await?;

        Ok(bincode::deserialize(&response)?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateRealmRequestTx {
    id: Ulid,
    generated_group: Group,
    req: CreateRealmRequest,
    requester: Requester,
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for CreateRealmRequestTx {
    async fn execute(
        &self,
        associated_event_id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, crate::error::ArunaError> {
        controller.authorize(&self.requester, &self.req).await?;

        let realm = Realm {
            id: self.id,
            tag: self.req.tag.clone(),
            name: self.req.name.clone(),
            description: self.req.description.clone(),
        };

        let group = self.generated_group.clone();
        let requester_id = self.requester.get_id().ok_or_else(|| ArunaError::Forbidden("Unregistered".to_string()))?;

        let store = controller.get_store();
        Ok(tokio::task::spawn_blocking(move || {
            // Create realm, add user to realm

            let mut wtxn = store.write_txn()?;

            // Exemplary check via the universe filter
            // TODO: escape tag filter
            if !store
                .filtered_universe(
                    Some(&format!("tag='{}' AND variant=6", realm.tag.clone())),
                    &wtxn.get_txn(),
                )?
                .is_empty()
            {
                drop(wtxn);
                return Err(ArunaError::ConflictParameter {
                    name: "tag".to_string(),
                    error: "Realm tag not unique".to_string(),
                });
            };

            let Some(user_idx) = store.get_idx_from_ulid(&requester_id, wtxn.get_txn()) else {
                return Err(ArunaError::NotFound(requester_id.to_string()));
            };

            // Create realm
            let realm_idx = store.create_node(&mut wtxn, &realm)?;
            // Create group
            let group_idx = store.create_node(&mut wtxn, &group)?;

            // Add relation user --ADMIN--> group
            store.create_relation(
                &mut wtxn,
                user_idx,
                group_idx,
                relation_types::PERMISSION_ADMIN,
            )?;

            // Add relation group --ADMINISTRATES--> realm
            store.create_relation(
                &mut wtxn,
                group_idx,
                realm_idx,
                relation_types::GROUP_ADMINISTRATES_REALM,
            )?;

            // Affected nodes: User, Realm and Group
            store.register_event(
                &mut wtxn,
                associated_event_id,
                &[user_idx, realm_idx, group_idx],
            )?;

            wtxn.commit()?;
            // Create admin group, add user to admin group
            Ok::<_, ArunaError>(bincode::serialize(&CreateRealmResponse {
                realm,
                admin_group_id: group.id,
            })?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}

impl Request for AddGroupRequest {
    type Response = AddGroupResponse;
    fn get_context(&self) -> Context {
        Context::Permission {
            min_permission: crate::models::models::Permission::Admin,
            source: self.realm_id,
        }
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        let request_tx = AddGroupRequestTx {
            req: self,
            requester: requester.ok_or_else(|| ArunaError::Unauthorized)?,
        };

        let response = controller.transaction(Ulid::new().0, &request_tx).await?;

        Ok(bincode::deserialize(&response)?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddGroupRequestTx {
    req: AddGroupRequest,
    requester: Requester,
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for AddGroupRequestTx {
    async fn execute(
        &self,
        associated_event_id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, crate::error::ArunaError> {
        controller.authorize(&self.requester, &self.req).await?;
        let group_id = self.req.group_id;
        let realm_id = self.req.realm_id;
        let store = controller.get_store();
        Ok(tokio::task::spawn_blocking(move || {
            // Create realm, add user to realm

            let mut wtxn = store.write_txn()?;

            let Some(group_idx) = store.get_idx_from_ulid(&group_id, wtxn.get_txn()) else {
                return Err(ArunaError::NotFound(group_id.to_string()));
            };

            let Some(realm_idx) = store.get_idx_from_ulid(&realm_id, wtxn.get_txn()) else {
                return Err(ArunaError::NotFound(realm_id.to_string()));
            };

            // Add relation group --GROUP_PART_OF_REALM--> realm
            store.create_relation(
                &mut wtxn,
                group_idx,
                realm_idx,
                relation_types::GROUP_PART_OF_REALM,
            )?;

            // Affected nodes: Realm and Group
            store.register_event(&mut wtxn, associated_event_id, &[realm_idx, group_idx])?;

            wtxn.commit()?;
            // Create admin group, add user to admin group
            Ok::<_, ArunaError>(bincode::serialize(&AddGroupResponse {})?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}

impl Request for GetRealmRequest {
    type Response = GetRealmResponse;
    fn get_context(&self) -> Context {
        Context::UserOnly
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        if let Some(requester) = requester {
            controller.authorize(&requester, &self).await?;
        } else {
            return Err(ArunaError::Unauthorized);
        }
        let store = controller.get_store();
        let response = tokio::task::spawn_blocking(move || {
            // Create realm, add user to realm
            let rtxn = store.read_txn()?;

            let Some(realm_idx) = store.get_idx_from_ulid(&self.id, &rtxn) else {
                return Err(ArunaError::NotFound(self.id.to_string()));
            };

            let realm = store
                .get_node::<Realm>(&rtxn, realm_idx)
                .ok_or_else(|| ArunaError::NotFound(self.id.to_string()))?;

            Ok::<_, ArunaError>(GetRealmResponse { realm })
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??;

        Ok(response)
    }
}

impl Request for GetGroupsFromRealmRequest {
    type Response = GetGroupsFromRealmResponse;
    fn get_context(&self) -> Context {
        Context::UserOnly
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        if let Some(requester) = requester {
            controller.authorize(&requester, &self).await?;
        } else {
            return Err(ArunaError::Unauthorized);
        }
        let store = controller.get_store();
        let response = tokio::task::spawn_blocking(move || {
            // Create realm, add user to realm
            let rtxn = store.read_txn()?;

            let Some(realm_idx) = store.get_idx_from_ulid(&self.realm_id, &rtxn) else {
                return Err(ArunaError::NotFound(self.realm_id.to_string()));
            };

            let mut groups = Vec::new();
            for source in store
                .get_relations(realm_idx, &[GROUP_PART_OF_REALM], Direction::Incoming, &rtxn)?
                .into_iter()
                .map(|r| r.from_id)
            {
                let source_idx = store
                    .get_idx_from_ulid(&source, &rtxn)
                    .ok_or_else(|| return ArunaError::NotFound(source.to_string()))?;

                if let Some(user) = store.get_node(&rtxn, source_idx) {
                    groups.push(user);
                } else {
                    tracing::error!("Idx not found in database");
                };
            }
            rtxn.commit()?;

            Ok::<_, ArunaError>(GetGroupsFromRealmResponse { groups })
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??;

        Ok(response)
    }
}

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
