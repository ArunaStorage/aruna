use super::transaction::{ArunaTransaction, Requester, TransactionOk};
use crate::{
    error::ArunaError,
    graph::ArunaGraph,
    logerr,
    models::{
        EdgeType, Issuer, NodeVariantId, NodeVariantValue, RawRelation,  User,
    },
    storage::viewstore::ViewStore,
};
use jsonwebtoken::{DecodingKey, EncodingKey};
use std::{future::Future, net::SocketAddr, sync::Arc, time::Duration};
use synevi::network::GrpcNetwork;
use synevi::Node as SyneviNode;
use synevi::SyneviResult;
use tokio::sync::RwLock;
use tracing::trace;
use ulid::Ulid;

type Node = RwLock<Option<Arc<SyneviNode<GrpcNetwork, Arc<Controller>>>>>;

pub struct Controller {
    // view_store: RwLock<ViewStore>,
    // graph: RwLock<ArunaGraph>,
    pub(super) store: RwLock<GraphStore>,
    node: Node,
    // Auth signing info
    pub(super) signing_info: Arc<RwLock<(u32, EncodingKey, DecodingKey)>>, //<PublicKey Serial; PrivateKey; PublicKey>
}

pub(super) struct GraphStore {
    pub(super) view_store: ViewStore,
    pub(super) graph: ArunaGraph,
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
        let key_config = Self::config_into_keys(key_config)?;
        let view_store = ViewStore::new(path, &key_config.0, &key_config.2)?;
        let env = view_store.get_env();

        let graph = ArunaGraph::from_env(env)?;

        let controller = Arc::new(Controller {
            store: RwLock::new(GraphStore {
                view_store,
                graph,
            }),
            node: RwLock::new(None),
            signing_info: Arc::new(RwLock::new(key_config)),
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

    fn config_into_keys(
        (serial, encode_secret, decode_secret): KeyConfig,
    ) -> Result<(u32, EncodingKey, DecodingKey), ArunaError> {
        let private_pem = format!(
            "-----BEGIN PRIVATE KEY-----{}-----END PRIVATE KEY-----",
            encode_secret
        );
        let public_pem = format!(
            "-----BEGIN PUBLIC KEY-----{}-----END PUBLIC KEY-----",
            decode_secret
        );

        Ok((
            serial,
            EncodingKey::from_ed_pem(private_pem.as_bytes()).map_err(|_| {
                tracing::error!("Error creating encoding key");
                ArunaError::ConfigError("Error creating encoding key".to_string())
            })?,
            DecodingKey::from_ed_pem(public_pem.as_bytes()).map_err(|_| {
                tracing::error!("Error creating decoding key");
                ArunaError::ConfigError("Error creating decoding key".to_string())
            })?,
        ))
    }

    pub async fn get_issuer_key(&self, issuer_name: String, kid: String) -> Option<DecodingKey> {
        self.store
            .read()
            .await
            .view_store
            .get_issuer_key(issuer_name, kid)
            .cloned()
    }

    // TODO
    pub async fn get_user_by_token(&self, _token_id: Ulid) -> Option<User> {
        todo!()
    }
    pub async fn get_issuer(&self, issuer: String) -> Result<Issuer, ArunaError> {
        self.store
            .read()
            .await
            .view_store
            .get_issuer(issuer)?
            .ok_or_else(|| {
                tracing::error!("Issuer not found");
                ArunaError::ServerError("Issuer not found".to_string())
            })
    }

    pub async fn get_requester_from_aruna_token(
        &self,
        token_id: Ulid,
    ) -> Result<Requester, ArunaError> {
        let lock = self.store.read().await;
        lock.graph
            .get_requester_from_aruna_token(token_id)
            .ok_or_else(|| {
                tracing::error!("Requester not found");
                ArunaError::Unauthorized
            })
    }
}

#[async_trait::async_trait]
impl synevi::Executor for Controller {
    type Tx = ArunaTransaction;
    async fn execute(&self, transaction: Self::Tx) -> SyneviResult<Self> {
        trace!("Executing transaction");
        Ok(self.process_transaction(transaction).await)
    }
}

pub trait Get {
    fn get(
        &self,
        id: Ulid,
    ) -> impl Future<Output = Result<Option<NodeVariantValue>, ArunaError>> + Send;
    fn get_incoming_relations(
        &self,
        id: NodeVariantId,
        filter: Vec<EdgeType>,
    ) -> impl Future<Output = Vec<RawRelation>> + Send;
    fn get_outgoing_relations(
        &self,
        id: NodeVariantId,
        filter: Vec<EdgeType>,
    ) -> impl Future<Output = Vec<RawRelation>> + Send;
}

pub trait Transaction {
    fn transaction(
        &self,
        transaction_id: u128,
        transaction: ArunaTransaction,
    ) -> impl Future<Output = Result<TransactionOk, ArunaError>> + Send;
}

impl Get for Controller {
    async fn get(&self, id: Ulid) -> Result<Option<NodeVariantValue>, ArunaError> {
        self.store.read().await.view_store.get_value(&id)
    }
    async fn get_incoming_relations(
        &self,
        id: NodeVariantId,
        filter: Vec<EdgeType>,
    ) -> Vec<RawRelation> {
        self.store
            .read()
            .await
            .graph
            .get_relations(id, filter, petgraph::Direction::Incoming)
    }
    async fn get_outgoing_relations(
        &self,
        id: NodeVariantId,
        filter: Vec<EdgeType>,
    ) -> Vec<RawRelation> {
        self.store
            .read()
            .await
            .graph
            .get_relations(id, filter, petgraph::Direction::Outgoing)
    }
}

impl Transaction for Controller {
    async fn transaction(
        &self,
        transaction_id: u128,
        transaction: ArunaTransaction,
    ) -> Result<TransactionOk, ArunaError> {
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
}
