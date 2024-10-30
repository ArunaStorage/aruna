use crate::{
    error::ArunaError,
    logerr,
    models::models::{
        EdgeType, Issuer, IssuerType, Node, NodeVariant, Permission, RawRelation, RelationInfo,
        ServerState, Token,
    },
    storage::{
        graph::load_graph,
        init::{self, init_issuer},
        milli_helpers::prepopulate_fields,
        utils::config_into_keys,
    },
    transactions::controller::KeyConfig,
};
use ahash::RandomState;
use heed::{
    byteorder::BigEndian,
    types::{SerdeBincode, Str, U128},
    Database, DatabaseFlags, EnvOpenOptions, RoTxn, RwTxn,
};
use jsonwebtoken::{DecodingKey, EncodingKey};
use milli::{
    documents::{DocumentsBatchBuilder, DocumentsBatchReader},
    update::{IndexDocuments, IndexDocumentsConfig, IndexerConfig},
    CboRoaringBitmapCodec, Index, ObkvCodec, BEU32,
};
use petgraph::{visit::EdgeRef, Graph};
use serde_json::Value;
use std::{
    collections::HashMap,
    fs,
    io::Cursor,
    sync::{RwLock, RwLockWriteGuard},
};
use ulid::Ulid;

use super::graph::{get_permissions, IndexHelper};

pub struct WriteTxn<'a> {
    milli_index: &'a Index,
    txn: Option<RwTxn<'a>>,
    graph_lock: RwLockWriteGuard<'a, Graph<NodeVariant, EdgeType>>,
    relations: &'a Database<BEU32, SerdeBincode<RawRelation>>,
    documents: &'a Database<BEU32, ObkvCodec>,
    tracker: u16,
}

impl<'a> WriteTxn<'a> {
    pub fn get_txn(&mut self) -> &mut RwTxn<'a> {
        self.txn.as_mut().expect("Transaction already committed")
    }

    pub fn get_graph(&mut self) -> &mut RwLockWriteGuard<'a, Graph<NodeVariant, EdgeType>> {
        self.tracker += 1;
        &mut self.graph_lock
    }

    pub fn commit(mut self) -> Result<(), ArunaError> {
        let txn = self.txn.take().expect("Transaction already committed");
        txn.commit().inspect_err(logerr!())?;
        Ok(())
    }
}

impl<'a> Drop for WriteTxn<'a> {
    fn drop(&mut self) {
        if self.txn.is_some() && self.tracker != 0 {
            // Recovering graph state
            tracing::error!("Transaction dropped without commit -> Recovering graph state");
            // Abort the transaction
            self.txn
                .take()
                .expect("Transaction already committed")
                .abort();

            // Reset the graph
            let write_txn = self
                .milli_index
                .write_txn()
                .expect("Failed to create write transaction");

            *self.graph_lock = load_graph(&write_txn, self.relations, self.documents)
                .expect("Failed to load graph");
        }
    }
}

// LMBD database names
pub mod db_names {
    pub const RELATION_INFO_DB_NAME: &str = "relation_infos";
    pub const NODE_DB_NAME: &str = "nodes";
    pub const RELATION_DB_NAME: &str = "relations"; // -> HashSet with Source/Type/Target
    pub const OIDC_MAPPING_DB_NAME: &str = "oidc_mappings";
    pub const PUBKEY_DB_NAME: &str = "pubkeys";
    pub const TOKENS_DB_NAME: &str = "tokens";
    pub const ISSUER_DB_NAME: &str = "issuers";
    pub const SERVER_INFO_DB_NAME: &str = "server_infos";
    pub const EVENT_DB_NAME: &str = "events";
    pub const TOKENS: &str = "tokens";
    pub const USER: &str = "users";
    pub const READ_GROUP_PERMS: &str = "read_group_perms";
}

pub(super) type DecodingKeyIdentifier = (String, String); // (IssuerName, KeyID)
pub type IssuerInfo = (IssuerType, DecodingKey, Vec<String>); // (IssuerType, DecodingKey, Audiences)

pub struct Store {
    // Milli index to store objects and allow for search
    milli_index: Index,
    // Store it in an increasing list of relations
    relations: Database<BEU32, SerdeBincode<RawRelation>>,
    // Relations info
    relation_infos: Database<BEU32, SerdeBincode<RelationInfo>>,
    // events db
    events: Database<BEU32, U128<BigEndian>>,
    // TODO:
    // Database for event_subscriber / status
    // Roaring bitmap for subscriber resources + last acknowledged event

    // Database for read permissions of groups (and users)
    read_permissions: Database<BEU32, CboRoaringBitmapCodec>,

    // Database for tokens with user_idx as key and a list of tokens as value
    tokens: Database<BEU32, SerdeBincode<Vec<Option<Token>>>>,

    // Database for issuers with name as key
    issuers: Database<Str, SerdeBincode<Issuer>>,
    // -------------------
    // Volatile data
    // Component status
    status: HashMap<Ulid, ServerState, RandomState>,
    issuer_decoding_keys: HashMap<DecodingKeyIdentifier, IssuerInfo, RandomState>,
    signing_info: (u32, EncodingKey, DecodingKey),
    // This has to be a RwLock because the graph is mutable
    graph: RwLock<Graph<NodeVariant, EdgeType>>,
}

impl Store {
    #[tracing::instrument(level = "trace", skip(key_config))]
    pub fn new(path: String, key_config: KeyConfig) -> Result<Self, ArunaError> {
        use db_names::*;
        let path = format!("{path}/store");
        fs::create_dir_all(&path).inspect_err(logerr!())?;
        // SAFETY: This opens a memory mapped file that may introduce UB
        //         if handled incorrectly
        //         see: https://docs.rs/heed/latest/heed/struct.EnvOpenOptions.html#safety-1
        let milli_index = Index::new(EnvOpenOptions::new(), path).inspect_err(logerr!())?;

        let mut write_txn = milli_index.write_txn().inspect_err(logerr!())?;

        prepopulate_fields(&milli_index, &mut write_txn).inspect_err(logerr!())?;

        let env = &milli_index.env;
        let relations = env
            .create_database(&mut write_txn, Some(RELATION_DB_NAME))
            .inspect_err(logerr!())?;
        let relation_infos = env
            .create_database(&mut write_txn, Some(RELATION_INFO_DB_NAME))
            .inspect_err(logerr!())?;
        let tokens = env
            .create_database(&mut write_txn, Some(TOKENS_DB_NAME))
            .inspect_err(logerr!())?;
        let issuers = env
            .create_database(&mut write_txn, Some(ISSUER_DB_NAME))
            .inspect_err(logerr!())?;
        let read_permissions = env
            .create_database(&mut write_txn, Some(READ_GROUP_PERMS))
            .inspect_err(logerr!())?;

        // Special events database allowing for duplicates
        let events = env
            .database_options()
            .types::<BEU32, U128<BigEndian>>()
            .flags(DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED | DatabaseFlags::INTEGER_KEY)
            .name(EVENT_DB_NAME)
            .create(&mut write_txn)
            .inspect_err(logerr!())?;

        // INIT relations
        init::init_relations(&mut write_txn, &relation_infos)?;
        // INIT issuer
        let key_config = config_into_keys(key_config).inspect_err(logerr!())?;

        let issuer_decoding_keys =
            init_issuer(&mut write_txn, &issuers, &key_config.0, &key_config.2)?;
        write_txn.commit().inspect_err(logerr!())?;

        let graph = load_graph(
            &milli_index.read_txn().inspect_err(logerr!())?,
            &relations,
            &milli_index.documents,
        )
        .inspect_err(logerr!())?;

        Ok(Self {
            milli_index,
            relations,
            relation_infos,
            events,
            tokens,
            issuers,
            read_permissions,
            status: HashMap::default(),
            issuer_decoding_keys,
            signing_info: key_config,
            graph: RwLock::new(graph),
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn read_txn(&self) -> Result<heed::RoTxn, ArunaError> {
        Ok(self.milli_index.read_txn().inspect_err(logerr!())?)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn write_txn(&self) -> Result<WriteTxn, ArunaError> {
        // Lock the graph first
        // Invariant: You can only aquire a write transaction if the graph is locked
        let graph_lock = self.graph.write().expect("Poisoned lock");

        Ok(WriteTxn {
            milli_index: &self.milli_index,
            graph_lock,
            txn: Some(self.milli_index.write_txn().inspect_err(logerr!())?),
            relations: &self.relations,
            documents: &self.milli_index.documents,
            tracker: 0,
        })
    }

    #[tracing::instrument(level = "trace", skip(self, id, rtxn))]
    pub fn get_idx_from_ulid(&self, id: &Ulid, rtxn: &RoTxn) -> Option<u32> {
        self.milli_index
            .external_documents_ids
            .get(rtxn, &id.to_string())
            .inspect_err(logerr!())
            .ok()
            .flatten()
    }

    #[tracing::instrument(level = "trace", skip(self, id, rtxn))]
    pub fn get_ulid_from_idx(&self, id: &u32, rtxn: &RoTxn) -> Option<Ulid> {
        let response = self
            .milli_index
            .documents
            .get(rtxn, &id)
            .inspect_err(logerr!())
            .ok()
            .flatten()?;
        // 0u16 is the primary key
        Some(Ulid::from_bytes(
            response.get(0u16)?.try_into().inspect_err(logerr!()).ok()?,
        ))
    }

    #[tracing::instrument(level = "trace", skip(self, wtxn))]
    pub fn create_relation(
        &self,
        wtxn: &mut WriteTxn,
        source: u32,
        target: u32,
        edge_type: EdgeType,
    ) -> Result<(), ArunaError> {
        let relation = RawRelation {
            source: source,
            target: target,
            edge_type,
        };

        self.relations
            .put(wtxn.get_txn(), &relation.source, &relation)
            .inspect_err(logerr!())?;

        wtxn.get_graph()
            .add_edge(source.into(), target.into(), edge_type);

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, rtxn))]
    pub fn get_node<T: Node>(&self, rtxn: &RoTxn<'_>, node_idx: u32) -> Option<T>
    where
        for<'a> &'a T: TryInto<serde_json::Map<String, Value>, Error = ArunaError>,
    {
        let response = self
            .milli_index
            .documents
            .get(rtxn, &node_idx)
            .inspect_err(logerr!())
            .ok()
            .flatten()?;

        T::try_from(&response).ok()
    }

    #[tracing::instrument(level = "trace", skip(self, node, wtxn))]
    pub fn create_node<'a, T: Node>(
        &'a self,
        wtxn: &mut WriteTxn<'a>,
        event_id: u128,
        node: &T,
    ) -> Result<u32, ArunaError>
    where
        for<'b> &'b T: TryInto<serde_json::Map<String, Value>, Error = ArunaError>,
    {
        // Use the milli transaction directly to prevent lifetime interference

        let indexer_config = IndexerConfig::default();
        let builder = IndexDocuments::new(
            wtxn.get_txn(),
            &self.milli_index,
            &indexer_config,
            IndexDocumentsConfig::default(),
            |_| (),
            || false,
        )?;

        // Request the ulid from the node
        let id = node.get_id();
        // Request the variant from the node
        let variant = node.get_variant();

        // Create a document batch
        let mut documents_batch = DocumentsBatchBuilder::new(Vec::new());
        // Add the json object to the batch
        documents_batch.append_json_object(&node.try_into()?)?;
        // Create a reader for the batch
        let reader = DocumentsBatchReader::from_reader(Cursor::new(documents_batch.into_inner()?))
            .map_err(|_| {
                tracing::error!(?id, "Unable to index document");
                ArunaError::DatabaseError("Unable to index document".to_string())
            })?;
        // Add the batch to the reader
        let (builder, error) = builder.add_documents(reader)?;
        error.map_err(|e| {
            tracing::error!(?id, ?e, "Error adding document");
            ArunaError::DatabaseError("Error adding document".to_string())
        })?;

        // Execute the indexing
        builder.execute()?;

        // Get the idx of the node
        let idx = self
            .get_idx_from_ulid(&id, &wtxn.get_txn())
            .ok_or_else(|| ArunaError::DatabaseError("Missing idx".to_string()))?;

        // Add the node to the graph
        let index = wtxn.get_graph().add_node(variant);

        // Ensure that the index in graph and milli stays in sync
        assert_eq!(index.index() as u32, idx,);

        // Associate the event with the new node
        self.events
            .put(wtxn.get_txn(), &idx, &event_id)
            .inspect_err(logerr!())?;

        Ok(idx)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_encoding_key(&self) -> (&u32, &EncodingKey) {
        (&self.signing_info.0, &self.signing_info.1)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_issuer_info(
        &self,
        issuer_name: String,
        key_id: String,
    ) -> Option<(&IssuerType, &DecodingKey, &[String])> {
        let result = self.issuer_decoding_keys.get(&(issuer_name, key_id))?;
        Some((&result.0, &result.1, result.2.as_slice()))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_issuer(&self, issuer: String) -> Result<Option<Issuer>, ArunaError> {
        let read_txn = self.read_txn()?;
        let issuer = self
            .issuers
            .get(&read_txn, &issuer)
            .inspect_err(logerr!())?;
        Ok(issuer)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_permissions(&self, resource: &Ulid, user: &Ulid) -> Result<Permission, ArunaError> {
        let rtxn = self.read_txn()?;
        let resource_idx = self.get_idx_from_ulid(resource, &rtxn).ok_or_else(|| {
            tracing::error!("From not found");
            ArunaError::Unauthorized
        })?;
        let user_idx = self.get_idx_from_ulid(user, &rtxn).ok_or_else(|| {
            tracing::error!("To not found");
            ArunaError::Unauthorized
        })?;
        drop(rtxn);
        let graph = self.graph.read().expect("Poisoned lock");
        get_permissions(&graph, resource_idx, user_idx)
    }

    /// Returns the type of user and additional information
    /// based on the token Ulid
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn ensure_token_exists(
        &self,
        requester_id: &Ulid,
        token_idx: u16,
    ) -> Result<(), ArunaError> {
        let read_txn = self.read_txn()?;

        // Get the internal idx of the token
        let requester_internal_idx =
            self.get_idx_from_ulid(requester_id, &read_txn)
                .ok_or_else(|| {
                    tracing::error!("User not found");
                    ArunaError::Unauthorized
                })?;

        let Some(tokens) = self
            .tokens
            .get(&read_txn, &requester_internal_idx)
            .inspect_err(logerr!())?
        else {
            tracing::error!("No tokens found");
            return Err(ArunaError::Unauthorized);
        };

        let Some(Some(_token)) = tokens.get(token_idx as usize) else {
            tracing::error!("Token not found");
            return Err(ArunaError::Unauthorized);
        };
        Ok(())
    }

    // Returns the group_id of the service account
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_group_from_sa(&self, service_account: &Ulid) -> Result<Ulid, ArunaError> {
        use crate::constants::relation_types::*;
        let read_txn = self.read_txn()?;

        let sa_idx = self
            .get_idx_from_ulid(service_account, &read_txn)
            .ok_or_else(|| {
                tracing::error!("User not found");
                ArunaError::Unauthorized
            })?;

        let graph = self.graph.read().expect("Poisoned lock");
        for edge in graph.edges_directed(sa_idx.into(), petgraph::Direction::Outgoing) {
            match edge.weight() {
                PERMISSION_NONE..=PERMISSION_ADMIN => {
                    let group_idx = edge.target().as_u32();
                    let group = self
                        .get_ulid_from_idx(&group_idx, &read_txn)
                        .ok_or_else(|| {
                            tracing::error!("Group not found");
                            ArunaError::Unauthorized
                        })?;
                    return Ok(group);
                }
                _ => {}
            }
        }

        tracing::error!("Group not found");
        Err(ArunaError::Unauthorized)
    }

    #[tracing::instrument(level = "trace", skip(self, rtxn))]
    pub fn add_token(
        &self,
        rtxn: &mut RwTxn,
        event_id: u128,
        user_id: &Ulid,
        mut token: Token,
    ) -> Result<Token, ArunaError> {
        let user_idx = self
            .get_idx_from_ulid(user_id, rtxn)
            .ok_or_else(|| ArunaError::NotFound(user_id.to_string()))?;

        let mut tokens = self
            .tokens
            .get(rtxn, &user_idx)
            .inspect_err(logerr!())?
            .unwrap_or_default();

        token.id = tokens.len() as u16;
        self.tokens
            .put(rtxn, &user_idx, &tokens)
            .inspect_err(logerr!())?;

        // Add token creation event to user
        self.events
            .put(rtxn, &user_idx, &event_id)
            .inspect_err(logerr!())?;

        Ok(tokens.pop().flatten().expect("Added token before"))
    }
}
