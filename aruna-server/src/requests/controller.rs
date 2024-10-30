use super::{
    auth::TokenHandler,
    request::{Request, SerializedResponse},
    transaction::ArunaTransaction,
};
use crate::{error::ArunaError, logerr, requests::request::Requester, storage::store::Store};
use heed::LmdbVersion;
use std::{
    fs,
    sync::{RwLock as StdRwLock, RwLockReadGuard, RwLockWriteGuard},
};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use synevi::network::GrpcNetwork;
use synevi::storage::LmdbStore;
use synevi::Node as SyneviNode;
use synevi::SyneviResult;
use tokio::sync::RwLock;
use tracing::trace;
use ulid::Ulid;

type ConsensusNode = RwLock<Option<Arc<SyneviNode<GrpcNetwork, Arc<Controller>, LmdbStore>>>>;

pub struct Controller {
    pub(super) store: Arc<StdRwLock<Store>>,
    node: ConsensusNode,
}

pub type KeyConfig = (u32, String, String);

impl Controller {
    //#[tracing::instrument(level = "trace", skip(key_config))]
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

    pub(crate) async fn request<R: Request>(
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
