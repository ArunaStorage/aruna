use super::{
    auth::TokenHandler,
    request::{Request, SerializedResponse, WriteRequest},
};
use crate::{error::ArunaError, logerr, storage::store::Store, transactions::request::Requester};
use std::{
    fs,
    sync::{RwLock as StdRwLock, RwLockReadGuard, RwLockWriteGuard},
};
use std::{net::SocketAddr, sync::Arc};
use synevi::{network::GrpcNetwork, SyneviError, Transaction};
use synevi::storage::LmdbStore;
use synevi::Node as SyneviNode;
use synevi::SyneviResult;
use tokio::sync::RwLock;
use tracing::trace;
use serde::Serialize;
use ulid::Ulid;

type ConsensusNode = RwLock<Option<Arc<SyneviNode<GrpcNetwork, Arc<Controller>, LmdbStore>>>>;

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
        let store = Store::new(path.clone(), key_config)?;

        let controller = Arc::new(Controller {
            store: Arc::new(StdRwLock::new(store)),
            node: RwLock::new(None),
        });

        let path = format!("{path}/events");
        fs::create_dir_all(&path)?;
        let synevi_lmdb: synevi::storage::LmdbStore =
            synevi::storage::LmdbStore::new(path, serial)?;

        let node = SyneviNode::new(
            node_id,
            serial,
            GrpcNetwork::new(
                store_addr,
                format!("http://{}", store_addr),
                node_id,
                serial,
            ),
            controller.clone(),
            synevi_lmdb,
        )
        .await
        .inspect_err(logerr!())?;

        // for (id, serial, host) in members {
        //     let mut counter = 0;
        //     loop {
        //         let Err(_) = node.add_member(id, serial, host.clone(), true).await else {
        //             break;
        //         };
        //         tokio::time::sleep(Duration::from_secs(10)).await;
        //         if counter > 6 {
        //             tracing::error!("Failed to add member");
        //             return Err(ArunaError::ServerError("Failed to add member".to_string()));
        //         }
        //         counter += 1;
        //     }
        // }
        *controller.node.write().await = Some(node);

        Ok(controller)
    }

    pub fn read_store(&self) -> RwLockReadGuard<'_, Store> {
        self.store.read().unwrap()
    }

    pub fn write_store(&self) -> RwLockWriteGuard<'_, Store> {
        self.store.write().unwrap()
    }

    pub async fn transaction<R: WriteRequest>(
        &self,
        transaction_id: u128,
        transaction: &R,
    ) -> Result<SerializedResponse, ArunaError> {
        // This dyn cast ist necessary because otherwise typetag will not work
        let transaction = ArunaTransaction(
            bincode::serialize(transaction as &dyn WriteRequest).inspect_err(logerr!())?,
        );

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

    #[tracing::instrument(level = "trace", skip(self))]
    pub(crate) async fn request<R: Request>(
        &self,
        request: R,
        token: Option<String>,
    ) -> Result<R::Response, ArunaError> {
        tracing::info!("Received request");
        let requester: Option<Requester> = self.authorize_token(token, &request).await?;
        tracing::trace!(?requester, "Requester authorized");
        request.run_request(requester, self).await
    }

    pub async fn process_transaction(
        &self,
        id: u128,
        transaction: ArunaTransaction,
    ) -> Result<SerializedResponse, ArunaError> {
        tracing::trace!(?id, "Deserializing transaction");
        tracing::debug!(transaction = ?transaction.0.len());
        let tx: Box<dyn WriteRequest> =
            bincode::deserialize(&transaction.0).inspect_err(logerr!())?;
        tx.execute(id, self).await
    }

    pub fn get_token_handler(&self) -> TokenHandler {
        TokenHandler::new(self.store.clone())
    }

    pub fn get_store(&self) -> Arc<StdRwLock<Store>> {
        self.store.clone()
    }
}

#[async_trait::async_trait]
impl synevi::Executor for Controller {
    type Tx = ArunaTransaction;
    async fn execute(&self, id: u128, transaction: Self::Tx) -> SyneviResult<Self> {
        trace!("Executing transaction");
        Ok(self.process_transaction(id, transaction).await)
    }
}


#[derive(Debug, Clone, Serialize)]
pub struct ArunaTransaction(pub Vec<u8>);

impl Transaction for ArunaTransaction {
    type TxErr = ArunaError;
    type TxOk = SerializedResponse;

    fn as_bytes(&self) -> Vec<u8> {
        self.0.clone()
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, SyneviError>
    where
        Self: Sized,
    {
        Ok(Self(bytes))
    }
}