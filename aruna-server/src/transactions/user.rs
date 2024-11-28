use super::{
    auth::TokenHandler,
    controller::Controller,
    request::{Request, Requester, WriteRequest},
};
use crate::{
    constants::relation_types::{PERMISSION_ADMIN, PERMISSION_NONE},
    context::Context,
    error::ArunaError,
    logerr,
    models::{
        models::{Group, NodeVariant, Permission, Subscriber, Token, User},
        requests::{
            CreateTokenRequest, CreateTokenResponse, GetGroupsFromUserRequest,
            GetGroupsFromUserResponse, GetRealmsFromUserRequest, GetRealmsFromUserResponse,
            GetUserRequest, GetUserResponse, RegisterUserRequest, RegisterUserResponse,
        },
    },
    transactions::request::SerializedResponse,
};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

impl Request for RegisterUserRequest {
    type Response = RegisterUserResponse;
    fn get_context(&self) -> Context {
        Context::Public
    }

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        let request_tx = RegisterUserRequestTx {
            id: Ulid::new(),
            req: self,
            requester: requester.ok_or_else(|| {
                tracing::error!("Missing requester");
                ArunaError::Unauthorized
            })?,
        };

        let response = controller.transaction(Ulid::new().0, &request_tx).await?;

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
    #[tracing::instrument(level = "trace", skip(self, controller))]
    async fn execute(
        &self,
        event_id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, crate::error::ArunaError> {
        tracing::trace!("Executing RegisterUserRequestTx");
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

        let oidc_mapping = match &self.requester {
            Requester::Unregistered {
                oidc_realm,
                oidc_subject,
            } => (oidc_subject.clone(), oidc_realm.clone()),
            _ => {
                return Err(ArunaError::Unauthorized);
            }
        };

        let store = controller.get_store();
        Ok(tokio::task::spawn_blocking(move || {
            let mut wtxn = store.write_txn()?;

            // Create user
            let user_idx = store.create_node(&mut wtxn, &user)?;
            store.add_oidc_mapping(&mut wtxn, user_idx, oidc_mapping)?;

            store.add_subscriber(
                &mut wtxn,
                Subscriber {
                    id: user.id,
                    owner: user.id,
                    target_idx: user_idx,
                    cascade: false,
                },
            )?;

            // Affected nodes: User
            store.register_event(&mut wtxn, event_id, &[user_idx])?;
            store.add_event_to_subscribers(&mut wtxn, event_id, &[user_idx])?;

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

impl Request for CreateTokenRequest {
    type Response = CreateTokenResponse;
    fn get_context(&self) -> Context {
        Context::Public
    }

    async fn run_request(
        mut self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        if self.expires_at.is_none() {
            self.expires_at = Some(chrono::Utc::now() + chrono::Duration::days(365));
        }

        let request_tx = CreateTokenRequestTx {
            req: self,
            requester: requester
                .ok_or_else(|| ArunaError::Unauthorized)
                .inspect_err(logerr!())?,
        };

        let response = controller.transaction(Ulid::new().0, &request_tx).await?;

        Ok(bincode::deserialize(&response)?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateTokenRequestTx {
    req: CreateTokenRequest,
    requester: Requester,
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for CreateTokenRequestTx {
    async fn execute(
        &self,
        event_id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, crate::error::ArunaError> {
        controller.authorize(&self.requester, &self.req).await?;
        let user_id = self
            .requester
            .get_id()
            .ok_or_else(|| ArunaError::Forbidden("Unregistered".to_string()))?;

        let token = Token {
            id: 0, // This id is created in the store
            user_id,
            name: self.req.name.clone(),
            expires_at: self.req.expires_at.expect("Got generated in Request"),
            constraints: None, // TODO: Constraints
        };

        let is_service_account = matches!(self.requester, Requester::ServiceAccount { .. });

        let store = controller.get_store();
        Ok(tokio::task::spawn_blocking(move || {
            let mut wtxn = store.write_txn()?;

            // Create user
            let token = store.add_token(wtxn.get_txn(), event_id, &token.user_id.clone(), token)?;
            // Add event to user

            wtxn.commit()?;

            let secret = TokenHandler::sign_user_token(
                &store,
                is_service_account,
                token.id,
                &token.user_id,
                None,
                Some(token.expires_at.timestamp() as u64),
            )?;
            // Create admin group, add user to admin group
            Ok::<_, ArunaError>(bincode::serialize(&CreateTokenResponse { token, secret })?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}

impl Request for GetUserRequest {
    type Response = GetUserResponse;

    fn get_context<'a>(&'a self) -> Context {
        Context::NotRegistered
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        let requester_ulid = requester
            .ok_or_else(|| {
                tracing::error!("Missing requester");
                ArunaError::Unauthorized
            })?
            .get_id()
            .ok_or_else(|| {
                tracing::error!("Missing requester id");
                ArunaError::NotFound("User not reqistered".to_string())
            })?;

        let store = controller.get_store();
        tokio::task::spawn_blocking(move || {
            let rtxn = store.read_txn()?;

            let idx = store
                .get_idx_from_ulid(&requester_ulid, &rtxn)
                .ok_or_else(|| ArunaError::NotFound("Requester not found".to_string()))?;

            Ok::<GetUserResponse, ArunaError>(GetUserResponse {
                user: store
                    .get_node::<User>(&rtxn, idx)
                    .ok_or_else(|| ArunaError::NotFound("User not found".to_string()))?,
            })
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })?
    }
}

impl Request for GetGroupsFromUserRequest {
    type Response = GetGroupsFromUserResponse;

    fn get_context<'a>(&'a self) -> Context {
        Context::UserOnly
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        let requester = if let Some(requester) = requester {
            requester.get_id().ok_or_else(|| ArunaError::Unauthorized)?
        } else {
            return Err(ArunaError::Unauthorized);
        };

        let store = controller.get_store();
        tokio::task::spawn_blocking(move || {
            // TODO: Optimize!
            let mut groups = Vec::new();
            let filter = (PERMISSION_NONE..=PERMISSION_ADMIN).collect::<Vec<u32>>();

            let rtxn = store.read_txn()?;
            let user_idx = store
                .get_idx_from_ulid(&requester, &rtxn)
                .ok_or_else(|| ArunaError::NotFound("Requester not found".to_string()))?;

            let relations =
                store.get_relations(user_idx, &filter, petgraph::Direction::Outgoing, &rtxn)?;
            for relation in &relations {
                let target = relation.to_id;

                let node_idx = store
                    .get_idx_from_ulid(&target, &rtxn)
                    .ok_or_else(|| ArunaError::NotFound("Requester not found".to_string()))?;
                let raw_node = store
                    .get_raw_node(&rtxn, node_idx)
                    .expect("Idx exist but no node in documents -> corrupted database");

                let variant: NodeVariant = serde_json::from_slice::<u8>(
                    raw_node
                        .get(1)
                        .expect("Missing variant -> corrupted database"),
                )
                .inspect_err(logerr!())?
                .try_into()
                .inspect_err(logerr!())?;
                if matches!(variant, NodeVariant::Group) {
                    let group = Group::try_from(&raw_node)?;

                    let perm = match relation.relation_type.as_str() {
                        "PermissionNone" => Permission::None,
                        "PermissionAppend" => Permission::Append,
                        "PermissionRead" => Permission::Read,
                        "PermissionWrite" => Permission::Write,
                        "PermissionAdmin" => Permission::Admin,
                        _ => {
                            // Should not happen, because filter is set for all permission
                            // relations
                            continue;
                        }
                    };
                    groups.push((group, perm));
                }
            }
            Ok::<GetGroupsFromUserResponse, ArunaError>(GetGroupsFromUserResponse { groups })
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })?
    }
}

impl Request for GetRealmsFromUserRequest {
    type Response = GetRealmsFromUserResponse;

    fn get_context<'a>(&'a self) -> Context {
        Context::UserOnly
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        let requester = if let Some(requester) = requester {
            requester.get_id().ok_or_else(|| ArunaError::Unauthorized)?
        } else {
            return Err(ArunaError::Unauthorized);
        };

        let store = controller.get_store();
        tokio::task::spawn_blocking(move || {
            let read_txn = store.read_txn()?;
            let realms = store.get_realms_for_user(&read_txn, requester)?;
            Ok::<GetRealmsFromUserResponse, ArunaError>(GetRealmsFromUserResponse { realms })
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })?
    }
}
