use crate::{
    error::ArunaError,
    logerr,
    models::models::{
        EdgeType, GenericNode, Group, IssuerKey, IssuerType, Node, NodeVariant, Permission,
        RawRelation, Realm, Relation, RelationInfo, Resource, ServerState, ServiceAccount, Token,
        User,
    },
    storage::{
        graph::load_graph, init, milli_helpers::prepopulate_fields, utils::SigningInfoCodec,
    },
    transactions::controller::KeyConfig,
};
use ahash::RandomState;
use heed::{
    byteorder::BigEndian,
    types::{SerdeBincode, Str, U128},
    Database, DatabaseFlags, EnvFlags, EnvOpenOptions, PutFlags, RoTxn, RwTxn, Unspecified,
};
use jsonwebtoken::{DecodingKey, EncodingKey};
use milli::{
    documents::{DocumentsBatchBuilder, DocumentsBatchReader},
    execute_search, filtered_universe,
    update::{IndexDocuments, IndexDocumentsConfig, IndexerConfig},
    CboRoaringBitmapCodec, DefaultSearchLogger, Filter, GeoSortStrategy, Index, ObkvCodec,
    SearchContext, TermsMatchingStrategy, TimeBudget, BEU32, BEU64,
};
use obkv::KvReader;
use petgraph::{visit::EdgeRef, Direction, Graph};
use roaring::RoaringBitmap;
use serde_json::Value;
use std::{
    collections::HashMap,
    fs,
    io::Cursor,
    sync::{atomic::AtomicU64, RwLock, RwLockWriteGuard},
};
use ulid::Ulid;

use super::graph::{get_permissions, get_realm_and_groups, IndexHelper};

pub struct WriteTxn<'a> {
    milli_index: &'a Index,
    txn: Option<RwTxn<'a>>,
    graph_lock: RwLockWriteGuard<'a, Graph<NodeVariant, EdgeType>>,
    relation_idx: &'a AtomicU64,
    relations: &'a Database<BEU64, SerdeBincode<RawRelation>>,
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

    // This is a read-only function that allows for the graph to be accessed
    // SAFETY: The caller must guarantee that the graph is not modified with this guard
    pub fn get_ro_graph(&self) -> &RwLockWriteGuard<'a, Graph<NodeVariant, EdgeType>> {
        &self.graph_lock
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

            *self.graph_lock = load_graph(
                &write_txn,
                self.relation_idx,
                self.relations,
                self.documents,
            )
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
    pub const SERVER_INFO_DB_NAME: &str = "server_infos";
    pub const EVENT_DB_NAME: &str = "events";
    pub const TOKENS: &str = "tokens";
    pub const USER: &str = "users";
    pub const READ_GROUP_PERMS: &str = "read_group_perms";
    pub const SINGLE_ENTRY_DB: &str = "single_entry_database";
}

pub mod single_entry_names {
    pub const ISSUER_KEYS: &str = "issuer_keys";
    pub const SIGNING_KEYS: &str = "signing_keys";
    pub const PUBLIC_RESOURCES: &str = "public_resources";
    pub const SEARCHABLE_USERS: &str = "searchable_users";
}

pub(super) type DecodingKeyIdentifier = (String, String); // (IssuerName, KeyID)
pub type IssuerInfo = (IssuerType, DecodingKey, Vec<String>); // (IssuerType, DecodingKey, Audiences)

pub struct Store {
    // Milli index to store objects and allow for search
    milli_index: Index,

    // Contains the following entries
    // ISSUER_KEYS
    // SigningKeys
    // Config?
    single_entry_database: Database<Unspecified, Unspecified>,

    // Store it in an increasing list of relations
    relations: Database<BEU64, SerdeBincode<RawRelation>>,
    // Increasing relation index
    relation_idx: AtomicU64,
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

    // -------------------
    // Volatile data
    // Component status
    status: RwLock<HashMap<Ulid, ServerState, RandomState>>,
    //issuer_decoding_keys: HashMap<DecodingKeyIdentifier, IssuerInfo, RandomState>,
    //signing_info: (u32, EncodingKey, DecodingKey),
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

        let mut env_options = EnvOpenOptions::new();
        unsafe { env_options.flags(EnvFlags::MAP_ASYNC | EnvFlags::WRITE_MAP) };
        env_options.map_size(10 * 1024 * 1024 * 1024); // 1GB

        let milli_index = Index::new(env_options, path).inspect_err(logerr!())?;

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
        let read_permissions = env
            .create_database(&mut write_txn, Some(READ_GROUP_PERMS))
            .inspect_err(logerr!())?;
        let single_entry_database = env
            .create_database(&mut write_txn, Some(SINGLE_ENTRY_DB))
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
        // INIT encoding_keys
        init::init_encoding_keys(&mut write_txn, &key_config, &single_entry_database)?;
        // INIT issuer_keys
        init::init_issuers(&mut write_txn, &key_config, &single_entry_database)?;

        write_txn.commit().inspect_err(logerr!())?;

        let relation_idx = AtomicU64::new(0);
        let graph = load_graph(
            &milli_index.read_txn().inspect_err(logerr!())?,
            &relation_idx,
            &relations,
            &milli_index.documents,
        )
        .inspect_err(logerr!())?;

        Ok(Self {
            milli_index,
            relations,
            relation_idx,
            relation_infos,
            events,
            tokens,
            read_permissions,
            status: RwLock::new(HashMap::default()),
            single_entry_database,
            //issuer_decoding_keys,
            //signing_info: key_config,
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
            relation_idx: &self.relation_idx,
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
            source,
            target,
            edge_type,
        };

        self.relations
            .put_with_flags(
                wtxn.get_txn(),
                PutFlags::APPEND,
                &self
                    .relation_idx
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                &relation,
            )
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

    #[tracing::instrument(level = "trace", skip(self, rtxn))]
    pub fn get_raw_node<'a>(
        &self,
        rtxn: &'a RoTxn<'a>,
        node_idx: u32,
    ) -> Option<KvReader<'a, u16>> {
        self.milli_index
            .documents
            .get(rtxn, &node_idx)
            .inspect_err(logerr!())
            .ok()
            .flatten()
    }

    #[tracing::instrument(level = "trace", skip(self, node, wtxn))]
    pub fn create_node<'a, T: Node>(
        &'a self,
        wtxn: &mut WriteTxn<'a>,
        node: &T,
    ) -> Result<u32, ArunaError>
    where
        for<'b> &'b T: TryInto<serde_json::Map<String, Value>, Error = ArunaError>,
    {
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
        assert_eq!(index.index() as u32, idx);

        Ok(idx)
    }

    pub fn create_nodes_batch<'a, T: Node>(
        &'a self,
        wtxn: &mut WriteTxn<'a>,
        nodes: Vec<&T>,
    ) -> Result<Vec<(Ulid, u32)>, ArunaError>
    where
        for<'b> &'b T: TryInto<serde_json::Map<String, Value>, Error = ArunaError>,
    {
        let indexer_config = IndexerConfig::default();
        let builder = IndexDocuments::new(
            wtxn.get_txn(),
            &self.milli_index,
            &indexer_config,
            IndexDocumentsConfig::default(),
            |_| (),
            || false,
        )?;

        // Create a document batch
        let mut documents_batch = DocumentsBatchBuilder::new(Vec::new());

        let mut ids = Vec::new();
        for node in nodes {
            // Request the ulid from the node
            let id = node.get_id();
            // Request the variant from the node
            let variant = node.get_variant();

            ids.push((id, variant));

            // Add the json object to the batch
            documents_batch.append_json_object(&node.try_into()?)?;
        }
        // Create a reader for the batch
        let reader = DocumentsBatchReader::from_reader(Cursor::new(documents_batch.into_inner()?))
            .map_err(|_| {
                tracing::error!(?ids, "Unable to index document");
                ArunaError::DatabaseError("Unable to index document".to_string())
            })?;
        // Add the batch to the reader
        let (builder, error) = builder.add_documents(reader)?;
        error.map_err(|e| {
            tracing::error!(?ids, ?e, "Error adding document");
            ArunaError::DatabaseError("Error adding document".to_string())
        })?;

        // Execute the indexing
        builder.execute()?;

        let mut result = Vec::new();
        for (id, variant) in ids {
            // Get the idx of the node
            let idx = self
                .get_idx_from_ulid(&id, &wtxn.get_txn())
                .ok_or_else(|| ArunaError::DatabaseError("Missing idx".to_string()))?;

            // Add the node to the graph
            let index = wtxn.get_graph().add_node(variant);

            // Ensure that the index in graph and milli stays in sync
            assert_eq!(index.index() as u32, idx);

            result.push((id, idx));
        }

        Ok(result)
    }

    #[tracing::instrument(level = "trace", skip(self, wtxn))]
    pub fn register_event(
        &self,
        wtxn: &mut WriteTxn<'_>,
        event_id: u128,
        affected: &[u32],
    ) -> Result<(), ArunaError> {
        for idx in affected {
            self.events
                .put(wtxn.get_txn(), idx, &event_id)
                .inspect_err(logerr!())?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_encoding_key(&self) -> Result<(u32, EncodingKey), ArunaError> {
        let rtxn = self.read_txn()?;

        let signing_info = self
            .single_entry_database
            .remap_types::<Str, SigningInfoCodec>()
            .get(&rtxn, single_entry_names::SIGNING_KEYS)
            .inspect_err(logerr!())
            .expect("Signing info not found")
            .expect("Signing info not found");

        Ok((signing_info.0, signing_info.1))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_issuer_info(
        &self,
        issuer_name: String,
        key_id: String,
    ) -> Option<(IssuerType, DecodingKey, Vec<String>)> {
        let read_txn = self.read_txn().ok()?;

        let issuers = self
            .single_entry_database
            .remap_types::<Str, SerdeBincode<Vec<IssuerKey>>>()
            .get(&read_txn, single_entry_names::ISSUER_KEYS)
            .inspect_err(logerr!())
            .ok()??;

        issuers
            .into_iter()
            .find(|issuer| issuer.key_id == key_id && issuer.issuer_name == issuer_name)
            .map(|issuer| (issuer.issuer_type, issuer.decoding_key, issuer.audiences))
    }

    #[tracing::instrument(level = "trace", skip(self, rtxn))]
    pub fn get_relations(
        &self,
        idx: u32,
        filter: &[EdgeType],
        direction: Direction,
        rtxn: &RoTxn,
    ) -> Result<Vec<Relation>, ArunaError> {
        let graph_lock = self.graph.read().expect("Poisoned lock");

        let relations = super::graph::get_relations(&graph_lock, idx, filter, direction);

        let mut result = Vec::new();
        for raw_relation in relations {
            let relation = match direction {
                Direction::Outgoing => Relation {
                    from_id: self
                        .get_ulid_from_idx(&raw_relation.source, rtxn)
                        .ok_or_else(|| ArunaError::NotFound("Index not found".to_string()))?,
                    to_id: self
                        .get_ulid_from_idx(&raw_relation.target, rtxn)
                        .ok_or_else(|| ArunaError::NotFound("Index not found".to_string()))?,
                    relation_type: self
                        .relation_infos
                        .get(&rtxn, &raw_relation.edge_type)?
                        .ok_or_else(|| ArunaError::NotFound("Edge type not found".to_string()))?
                        .forward_type,
                },
                Direction::Incoming => Relation {
                    from_id: self
                        .get_ulid_from_idx(&raw_relation.source, rtxn)
                        .ok_or_else(|| ArunaError::NotFound("Index not found".to_string()))?,
                    to_id: self
                        .get_ulid_from_idx(&raw_relation.target, rtxn)
                        .ok_or_else(|| ArunaError::NotFound("Index not found".to_string()))?,
                    relation_type: self
                        .relation_infos
                        .get(&rtxn, &raw_relation.edge_type)?
                        .ok_or_else(|| ArunaError::NotFound("Edge type not found".to_string()))?
                        .backward_type,
                },
            };
            result.push(relation);
        }

        Ok(result)
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

        tokens.push(Some(token));
        self.tokens
            .put(rtxn, &user_idx, &tokens)
            .inspect_err(logerr!())?;

        // Add token creation event to user
        self.events
            .put(rtxn, &user_idx, &event_id)
            .inspect_err(logerr!())?;

        Ok(tokens.pop().flatten().expect("Added token before"))
    }

    #[tracing::instrument(level = "trace", skip(self, rtxn))]
    pub fn search(
        &self,
        query: String,
        from: usize,
        limit: usize,
        filter: Option<&str>,
        rtxn: &RoTxn,
        filter_universe: RoaringBitmap,
    ) -> Result<(usize, Vec<GenericNode>), ArunaError> {
        let mut universe = self.filtered_universe(filter, rtxn)?;
        universe &= filter_universe;

        let mut ctx = SearchContext::new(&self.milli_index, &rtxn).inspect_err(logerr!())?;
        let result = execute_search(
            &mut ctx,                                         // Search context
            (!query.trim().is_empty()).then(|| query.trim()), // Query
            TermsMatchingStrategy::Last,                      // Terms matching strategy
            milli::score_details::ScoringStrategy::Skip,      // Scoring strategy
            false,                                            // exhaustive number of hits ?
            universe,                                         // Universe
            &None,                                            // Sort criteria
            &None,                                            // Distinct criteria
            GeoSortStrategy::default(),                       // Geo sort strategy
            from,                                             // From (for pagination)
            limit,                                            // Limit (for pagination)
            None,                                             // Words limit
            &mut DefaultSearchLogger,                         // Search logger
            &mut DefaultSearchLogger,                         // Search logger
            TimeBudget::max(),                                // Time budget
            None,                                             // Ranking score threshold
            None,                                             // Locales (Languages)
        )
        .inspect_err(logerr!())?;

        Ok((
            result.candidates.len() as usize,
            self.milli_index
                .documents(&rtxn, result.documents_ids)
                .inspect_err(logerr!())?
                .into_iter()
                .map(|(_idx, obkv)| {
                    // TODO: More efficient conversion for found nodes
                    let variant: serde_json::Number =
                        serde_json::from_slice(obkv.get(1).expect("Obkv variant key not found"))
                            .inspect_err(logerr!())?;
                    let variant: NodeVariant = variant.try_into().inspect_err(logerr!())?;

                    Ok(match variant {
                        NodeVariant::ResourceProject
                        | NodeVariant::ResourceFolder
                        | NodeVariant::ResourceObject => {
                            GenericNode::Resource(Resource::try_from(&obkv).inspect_err(logerr!())?)
                        }
                        NodeVariant::User => {
                            GenericNode::User(User::try_from(&obkv).inspect_err(logerr!())?)
                        }
                        NodeVariant::ServiceAccount => GenericNode::ServiceAccount(
                            ServiceAccount::try_from(&obkv).inspect_err(logerr!())?,
                        ),
                        NodeVariant::Group => {
                            GenericNode::Group(Group::try_from(&obkv).inspect_err(logerr!())?)
                        }
                        NodeVariant::Realm => {
                            GenericNode::Realm(Realm::try_from(&obkv).inspect_err(logerr!())?)
                        }
                    })
                })
                .collect::<Result<Vec<_>, ArunaError>>()?,
        ))
    }

    // This is a lower level function that only returns the filtered universe
    // We can use this to check for generic "exists" queries
    // For full search results use the search function
    #[tracing::instrument(level = "trace", skip(self, rtxn))]
    pub fn filtered_universe(
        &self,
        filter: Option<&str>,
        rtxn: &RoTxn,
    ) -> Result<RoaringBitmap, ArunaError> {
        let filter = if let Some(filter) = filter {
            Filter::from_str(filter).inspect_err(logerr!())?
        } else {
            None
        };

        Ok(filtered_universe(&self.milli_index, rtxn, &filter).inspect_err(logerr!())?)
    }

    #[tracing::instrument(level = "trace", skip(self, rtxn))]
    pub fn add_public_resources_universe(
        &self,
        rtxn: &mut WriteTxn,
        universe: &[u32],
    ) -> Result<(), ArunaError> {
        let mut universe = RoaringBitmap::from_iter(universe.iter().copied());

        let existing = self
            .single_entry_database
            .remap_types::<Str, CboRoaringBitmapCodec>()
            .get(&rtxn.get_txn(), single_entry_names::PUBLIC_RESOURCES)
            .inspect_err(logerr!())?
            .unwrap_or_default();

        // Union of the newly added universe and the existing universe
        universe |= existing;

        self.single_entry_database
            .remap_types::<Str, CboRoaringBitmapCodec>()
            .put(
                rtxn.get_txn(),
                single_entry_names::PUBLIC_RESOURCES,
                &universe,
            )
            .inspect_err(logerr!())?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, rtxn))]
    pub fn add_user_universe(&self, rtxn: &mut WriteTxn, user: u32) -> Result<(), ArunaError> {
        let mut universe = RoaringBitmap::new();
        universe.insert(user);

        let existing = self
            .single_entry_database
            .remap_types::<Str, CboRoaringBitmapCodec>()
            .get(&rtxn.get_txn(), single_entry_names::SEARCHABLE_USERS)
            .inspect_err(logerr!())?
            .unwrap_or_default();

        // Union of the newly added user and the existing universe
        universe |= existing;

        self.single_entry_database
            .remap_types::<Str, CboRoaringBitmapCodec>()
            .put(
                rtxn.get_txn(),
                single_entry_names::SEARCHABLE_USERS,
                &universe,
            )
            .inspect_err(logerr!())?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, rtxn))]
    pub fn add_read_permission_universe(
        &self,
        rtxn: &mut WriteTxn,
        group: u32,
        universe: &[u32],
    ) -> Result<(), ArunaError> {
        let mut universe = RoaringBitmap::from_iter(universe.iter().copied());

        let existing = self
            .read_permissions
            .get(&rtxn.get_txn(), &group)
            .inspect_err(logerr!())?
            .unwrap_or_default();

        // Union of the newly added user and the existing universe
        universe |= existing;

        self.read_permissions
            .put(rtxn.get_txn(), &group, &universe)
            .inspect_err(logerr!())?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, rtxn))]
    pub fn get_read_permission_universe(
        &self,
        rtxn: &RoTxn,
        read_resources: &[u32],
    ) -> Result<RoaringBitmap, ArunaError> {
        let mut universe = RoaringBitmap::new();
        for read_resource in read_resources {
            universe |= self
                .read_permissions
                .get(rtxn, read_resource)
                .inspect_err(logerr!())?
                .unwrap_or_default();
        }
        Ok(universe)
    }

    #[tracing::instrument(level = "trace", skip(self, rtxn))]
    pub fn get_public_universe(&self, rtxn: &RoTxn) -> Result<RoaringBitmap, ArunaError> {
        Ok(self
            .single_entry_database
            .remap_types::<Str, CboRoaringBitmapCodec>()
            .get(&rtxn, single_entry_names::PUBLIC_RESOURCES)
            .inspect_err(logerr!())?
            .unwrap_or_default())
    }

    #[tracing::instrument(level = "trace", skip(self, rtxn))]
    pub fn get_user_universe(&self, rtxn: &RoTxn) -> Result<RoaringBitmap, ArunaError> {
        Ok(self
            .single_entry_database
            .remap_types::<Str, CboRoaringBitmapCodec>()
            .get(&rtxn, single_entry_names::SEARCHABLE_USERS)
            .inspect_err(logerr!())?
            .unwrap_or_default())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_realm_and_groups(&self, user_idx: u32) -> Result<Vec<u32>, ArunaError> {
        get_realm_and_groups(&self.graph.read().expect("RWLock poison error"), user_idx)
    }
}
