use super::{
    auth::TokenHandler,
    request::{Request, SerializedResponse},
    transaction::ArunaTransaction,
};
use crate::{
    error::ArunaError,
    logerr,
    requests::{auth::Auth, request::Requester},
    storage::store::Store,
};
use std::sync::{RwLock as StdRwLock, RwLockReadGuard, RwLockWriteGuard};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use synevi::network::GrpcNetwork;
use synevi::Node as SyneviNode;
use synevi::SyneviResult;
use tokio::sync::RwLock;
use tracing::trace;
use ulid::Ulid;

type ConsensusNode = RwLock<Option<Arc<SyneviNode<GrpcNetwork, Arc<Controller>>>>>;

pub struct Controller {
    pub(super) store: Arc<StdRwLock<Store>>,
    node: ConsensusNode,
}

pub type KeyConfig = (u32, String, String);

impl Controller {
    #[tracing::instrument(level = "trace", skip(key_config))]
    pub async fn new(
        path: String,
        node_id: Ulid,
        serial: u16,
        store_addr: SocketAddr,
        members: Vec<(Ulid, u16, String)>,
        key_config: KeyConfig,
    ) -> Result<Arc<Self>, ArunaError> {
        let store = Store::new(path, key_config)?;

        let controller = Arc::new(Controller {
            store: Arc::new(StdRwLock::new(store)),
            node: RwLock::new(None),
        });
        let node = SyneviNode::new_with_network_and_executor(
            node_id,
            serial,
            GrpcNetwork::new(store_addr),
            controller.clone(),
        )
        .await
        .inspect_err(logerr!())?;

        for (id, serial, host) in members {
            let mut counter = 0;
            loop {
                let Err(_) = node.add_member(id, serial, host.clone()).await else {
                    break;
                };
                tokio::time::sleep(Duration::from_secs(10)).await;
                if counter > 6 {
                    tracing::error!("Failed to add member");
                    return Err(ArunaError::ServerError("Failed to add member".to_string()));
                }
                counter += 1;
            }
        }
        *controller.node.write().await = Some(node);

        Ok(controller)
    }

    pub fn read_store(&self) -> RwLockReadGuard<'_, Store> {
        self.store.read().unwrap()
    }

    pub fn write_store(&self) -> RwLockWriteGuard<'_, Store> {
        self.store.write().unwrap()
    }

    pub async fn transaction(
        &self,
        transaction_id: u128,
        transaction: ArunaTransaction,
    ) -> Result<SerializedResponse, ArunaError> {
        self.node
            .read()
            .await
            .as_ref()
            .ok_or_else(|| {
                tracing::error!("Node not set");
                ArunaError::ServerError("Node not set".to_string())
            })?
            .clone()
            .transaction(transaction_id, transaction)
            .await?
    }

    pub async fn request<R: Request>(
        &self,
        request: R,
        token: Option<String>,
    ) -> Result<R::Response, ArunaError> {
        let requester: Option<Requester> = self.authorize_token(token, &request).await?;
        request.run_request(requester, self).await
    }

    pub fn get_token_handler(&self) -> TokenHandler {
        TokenHandler::new(self.store.clone())
    }

    // pub async fn init_testing(&self) {
    //     let mut lock = self.store.write().await;

    //     let user = User {
    //         id: Ulid::from_string("01J6HRXQ5MTR3H3NHMY7XKCPWP").unwrap(),
    //         ..Default::default()
    //     };

    //     let token = Token {
    //         id: Ulid::from_string("01J6HS33NTB81SM73NE4DTC968").unwrap(),
    //         name: "dummy_token".to_string(),
    //         expires_at: chrono::DateTime::<Utc>::MAX_UTC,
    //     };

    //     lock.view_store
    //         .add_node(NodeVariantValue::User(user.clone()))
    //         .unwrap();
    //     lock.view_store
    //         .add_node(NodeVariantValue::Token(token.clone()))
    //         .unwrap();

    //     lock.graph.add_node(NodeVariantId::User(user.id));
    //     lock.graph.add_node(NodeVariantId::Token(token.id));

    //     let env = lock.view_store.get_env();
    //     let encoded_token = self.sign_user_token(&token.id, None).await.unwrap();

    //     tracing::info!(?encoded_token, "DEBUG_TOKEN");
    //     lock.graph
    //         .add_relation(
    //             NodeVariantId::Token(token.id),
    //             NodeVariantId::User(user.id),
    //             SHARES_PERMISSION,
    //             env,
    //         )
    //         .await
    //         .unwrap();
    // }

    // pub async fn get_issuer_key(&self, issuer_name: String, kid: String) -> Option<DecodingKey> {
    //     self.read_store()
    //         .await
    //         .get_issuer_key(issuer_name, kid)
    //         .cloned()
    // }

    // // TODO
    // pub async fn get_user_by_token(&self, _token_id: Ulid) -> Option<User> {
    //     todo!()
    // }
    // pub async fn get_issuer(&self, issuer: String) -> Result<Issuer, ArunaError> {
    //     self.read_store().await.get_issuer(issuer)?.ok_or_else(|| {
    //         tracing::error!("Issuer not found");
    //         ArunaError::ServerError("Issuer not found".to_string())
    //     })
    // }

    // pub async fn get_requester_from_aruna_token(
    //     &self,
    //     token_id: Ulid,
    // ) -> Result<Requester, ArunaError> {
    //     let lock = self.store.read().await;
    //     lock.graph
    //         .get_requester_from_aruna_token(token_id)
    //         .ok_or_else(|| {
    //             tracing::error!("Requester not found");
    //             ArunaError::Unauthorized
    //         })
    // }
}

#[async_trait::async_trait]
impl synevi::Executor for Controller {
    type Tx = ArunaTransaction;
    async fn execute(&self, transaction: Self::Tx) -> SyneviResult<Self> {
        trace!("Executing transaction");
        Ok(self.process_transaction(transaction).await)
    }
}

// pub trait Get {
//     fn get(&self, id: Ulid) -> impl Future<Output = Result<Option<Node>, ArunaError>> + Send;
//     fn get_incoming_relations(
//         &self,
//         id: Ulid,
//         filter: Vec<EdgeType>,
//     ) -> impl Future<Output = Vec<RawRelation>> + Send;
//     fn get_outgoing_relations(
//         &self,
//         id: Ulid,
//         filter: Vec<EdgeType>,
//     ) -> impl Future<Output = Vec<RawRelation>> + Send;
// }

// pub trait Transaction {
//     fn transaction(
//         &self,
//         transaction_id: u128,
//         transaction: ArunaTransaction,
//     ) -> impl Future<Output = Result<TransactionOk, ArunaError>> + Send;
// }

// impl Get for Controller {
//     async fn get(&self, id: Ulid) -> Result<Option<NodeVariantValue>, ArunaError> {
//         self.store.read().await.view_store.get_value(&id)
//     }
//     async fn get_incoming_relations(&self, id: Ulid, filter: Vec<EdgeType>) -> Vec<RawRelation> {
//         self.store
//             .read()
//             .await
//             .graph
//             .get_relations(id, filter, petgraph::Direction::Incoming)
//     }
//     async fn get_outgoing_relations(
//         &self,
//         id: NodeVariantId,
//         filter: Vec<EdgeType>,
//     ) -> Vec<RawRelation> {
//         self.store
//             .read()
//             .await
//             .graph
//             .get_relations(id, filter, petgraph::Direction::Outgoing)
//     }
// }

// impl Transaction for Controller {
//     async fn transaction(
//         &self,
//         transaction_id: u128,
//         transaction: ArunaTransaction,
//     ) -> Result<TransactionOk, ArunaError> {
//         self.node
//             .read()
//             .await
//             .as_ref()
//             .ok_or_else(|| {
//                 tracing::error!("Node not set");
//                 ArunaError::ServerError("Node not set".to_string())
//             })?
//             .clone()
//             .transaction(transaction_id, transaction)
//             .await?
//     }
// }
