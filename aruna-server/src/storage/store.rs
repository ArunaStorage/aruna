use crate::{
    error::ArunaError,
    logerr,
    models::{
        Audience, EdgeType, Issuer, IssuerType, NodeVariant, RawRelation, RelationInfo, ServerState,
    },
    requests::controller::KeyConfig,
    storage::{
        graph::load_graph,
        init::{self, init_issuer},
        milli_helpers::prepopulate_fields,
        utils::config_into_keys,
    },
};
use ahash::RandomState;
use heed::{
    types::{SerdeBincode, Str},
    Database, EnvOpenOptions,
};
use jsonwebtoken::{DecodingKey, EncodingKey};
use milli::{CboRoaringBitmapCodec, Index, BEU32};
use std::{collections::HashMap, fs};
use ulid::Ulid;

// LMBD database names
pub mod db_names {
    pub const RELATION_INFO_DB_NAME: &str = "relation_infos";
    pub const NODE_DB_NAME: &str = "nodes";
    pub const RELATION_DB_NAME: &str = "relations"; // -> HashSet with Source/Type/Target
    pub const OIDC_MAPPING_DB_NAME: &str = "oidc_mappings";
    pub const PUBKEY_DB_NAME: &str = "pubkeys";
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
    events: Database<BEU32, Vec<u128>>,
    // TODO:
    // Database for event_subscriber / status
    // Roaring bitmap for subscriber resources + last acknowledged event

    // Database for read permissions of groups
    read_permissions: Database<BEU32, CboRoaringBitmapCodec>,

    // Database for issuers with name as key
    issuers: Database<Str, SerdeBincode<Issuer>>,
    // -------------------
    // Volatile data
    // Component status
    status: HashMap<Ulid, ServerState, RandomState>,
    issuer_decoding_keys: HashMap<DecodingKeyIdentifier, IssuerInfo, RandomState>,
    signing_info: (u32, EncodingKey, DecodingKey),
    graph: petgraph::graph::Graph<NodeVariant, EdgeType>,
}

impl Store {
    #[tracing::instrument(level = "trace", skip(key_config))]
    pub fn new(path: String, key_config: KeyConfig) -> Result<Self, ArunaError> {
        use db_names::*;
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
        let events = env
            .create_database(&mut write_txn, Some(EVENT_DB_NAME))
            .inspect_err(logerr!())?;
        let issuers = env
            .create_database(&mut write_txn, Some(ISSUER_DB_NAME))
            .inspect_err(logerr!())?;
        let read_permissions = env
            .create_database(&mut write_txn, Some(READ_GROUP_PERMS))
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
            issuers,
            read_permissions,
            status: HashMap::default(),
            issuer_decoding_keys,
            signing_info: key_config,
            graph,
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn read_txn(&self) -> Result<heed::RoTxn, ArunaError> {
        Ok(self.milli_index.read_txn().inspect_err(logerr!())?)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn write_txn(&self) -> Result<heed::RwTxn, ArunaError> {
        Ok(self.milli_index.write_txn().inspect_err(logerr!())?)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_idx_from_ulid(&self, id: &Ulid) -> Option<u32> {
        // TODO: Decide if the transaction should be handled here or by the caller
        let txn = self.read_txn().ok()?;
        self.milli_index
            .external_documents_ids
            .get(&txn, &id.to_string())
            .inspect_err(logerr!())
            .ok()
            .flatten()
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

    // #[tracing::instrument(level = "trace", skip(self))]
    // pub fn get_value(&self, id: &Ulid) -> Result<Option<NodeVariantValue>, ArunaError> {
    //     let read_txn = self.database_env.read_txn()?;
    //     let db: NodeDb = self
    //         .database_env
    //         .open_database(&read_txn, Some(NODE_DB_NAME))?
    //         .ok_or_else(|| ArunaError::DatabaseDoesNotExist(NODE_DB_NAME))
    //         .inspect_err(logerr!())?;

    //     Ok(db.get(&read_txn, id).inspect_err(logerr!())?)
    // }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_issuer(&self, issuer: String) -> Result<Option<Issuer>, ArunaError> {
        let read_txn = self.read_txn()?;
        let issuer = self
            .issuers
            .get(&read_txn, &issuer)
            .inspect_err(logerr!())?;
        Ok(issuer)
    }

    // #[tracing::instrument(level = "trace", skip(self))]
    // pub fn get_issuer_key(&self, issuer_name: String, key_id: String) -> Option<&DecodingKey> {
    //     self.issuer_decoding_keys.get(&(issuer_name, key_id))
    // }

    // #[tracing::instrument(level = "trace", skip(self))]
    // pub fn add_node(&self, node: NodeVariantValue) -> Result<(), ArunaError> {
    //     let mut write_txn = self.database_env.write_txn().inspect_err(logerr!())?;
    //     let db: NodeDb = self
    //         .database_env
    //         .create_database(&mut write_txn, Some(NODE_DB_NAME))
    //         .inspect_err(logerr!())?;
    //     db.put_with_flags(&mut write_txn, PutFlags::NO_OVERWRITE, node.get_id(), &node)
    //         .inspect_err(logerr!())?;
    //     write_txn.commit().inspect_err(logerr!())?;
    //     Ok(())
    // }
}
