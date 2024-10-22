use crate::{
    error::ArunaError,
    logerr,
    models::{EdgeType, Issuer, NodeVariant, RawRelation, RelationInfo, ServerState},
};
use ahash::RandomState;
use heed::{
    byteorder::BigEndian,
    types::{SerdeBincode, Str, U32},
    Database, EnvOpenOptions, PutFlags,
};
use jsonwebtoken::DecodingKey;
use milli::Index;
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
}

type DecodingKeyIdentifier = (String, String); // (IssuerName, KeyID)
type BEU32 = U32<BigEndian>;

pub struct Store<'a> {
    // Milli index to store objects and allow for search
    milli_index: Index,
    // Store it in an increasing list of relations
    relations: Database<BEU32, SerdeBincode<RawRelation>>,
    // Relations info
    relation_infos: Database<BEU32, SerdeBincode<RelationInfo<'a>>>,
    // events db
    events: Database<BEU32, Vec<u128>>,
    // Database for issuers with name as key
    issuers: Database<Str, SerdeBincode<Issuer>>,
    // -------------------
    // Volatile data
    // Component status
    status: HashMap<Ulid, ServerState, RandomState>,
    issuer_decoding_keys: HashMap<DecodingKeyIdentifier, DecodingKey, RandomState>,
    graph: petgraph::graph::Graph<NodeVariant, EdgeType>,
}

impl Store<'_> {
    #[tracing::instrument(level = "trace", skip(key_serial, decoding_key))]
    pub fn new(
        path: String,
        key_serial: &u32,
        decoding_key: &DecodingKey,
    ) -> Result<Self, ArunaError> {
        use db_names::*;
        fs::create_dir_all(&path).inspect_err(logerr!())?;
        // SAFETY: This opens a memory mapped file that may introduce UB
        //         if handled incorrectly
        //         see: https://docs.rs/heed/latest/heed/struct.EnvOpenOptions.html#safety-1
        let milli_index = Index::new(EnvOpenOptions::new(), path).inspect_err(logerr!())?;

        let mut write_txn = milli_index.write_txn().inspect_err(logerr!())?;
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

        // INIT relations
        init_relations(&mut write_txn, &relation_infos)?;
        // INIT issuer
        let issuer_decoding_keys = init_issuer(&mut write_txn, &issuers, key_serial, decoding_key)?;
        write_txn.commit().inspect_err(logerr!())?;

        Ok(Self {
            milli_index,
            relations,
            relation_infos,
            events,
            issuers,
            status: HashMap::default(),
            issuer_decoding_keys,
            graph: petgraph::graph::Graph::new(),
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_value(&self, id: &Ulid) -> Result<Option<NodeVariantValue>, ArunaError> {
        let read_txn = self.database_env.read_txn()?;
        let db: NodeDb = self
            .database_env
            .open_database(&read_txn, Some(NODE_DB_NAME))?
            .ok_or_else(|| ArunaError::DatabaseDoesNotExist(NODE_DB_NAME))
            .inspect_err(logerr!())?;

        Ok(db.get(&read_txn, id).inspect_err(logerr!())?)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_issuer(&self, issuer: String) -> Result<Option<Issuer>, ArunaError> {
        let read_txn = self.database_env.read_txn().inspect_err(logerr!())?;
        let db: IssuerDb = self
            .database_env
            .open_database(&read_txn, Some(ISSUER_DB_NAME))
            .inspect_err(logerr!())?
            .ok_or_else(|| ArunaError::DatabaseDoesNotExist(ISSUER_DB_NAME))
            .inspect_err(logerr!())?;

        Ok(db.get(&read_txn, &issuer).inspect_err(logerr!())?)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_issuer_key(&self, issuer_name: String, key_id: String) -> Option<&DecodingKey> {
        self.issuer_decoding_keys.get(&(issuer_name, key_id))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn add_node(&self, node: NodeVariantValue) -> Result<(), ArunaError> {
        let mut write_txn = self.database_env.write_txn().inspect_err(logerr!())?;
        let db: NodeDb = self
            .database_env
            .create_database(&mut write_txn, Some(NODE_DB_NAME))
            .inspect_err(logerr!())?;
        db.put_with_flags(&mut write_txn, PutFlags::NO_OVERWRITE, node.get_id(), &node)
            .inspect_err(logerr!())?;
        write_txn.commit().inspect_err(logerr!())?;
        Ok(())
    }
}

#[tracing::instrument(level = "trace", skip(key_id, decoding_key, write_txn))]
fn init_issuer(
    mut write_txn: &mut heed::RwTxn,
    issuers: &Database<Str, SerdeBincode<Issuer>>,
    key_id: &u32,
    decoding_key: &DecodingKey,
) -> Result<HashMap<DecodingKeyIdentifier, DecodingKey, RandomState>, ArunaError> {
    issuers
        .put(
            &mut write_txn,
            "aruna",
            &Issuer {
                issuer_name: "aruna".to_string(),
                pubkey_endpoint: None,
                audiences: Some(vec!["aruna".to_string()]),
                issuer_type: crate::models::IssuerType::ARUNA,
            },
        )
        .inspect_err(logerr!())?;
    let mut iss: HashMap<DecodingKeyIdentifier, DecodingKey, RandomState> = HashMap::default();
    iss.insert(
        ("aruna".to_string(), key_id.to_string()),
        decoding_key.clone(),
    );

    Ok(iss)
}

fn init_relations(
    mut write_txn: &mut heed::RwTxn,
    relation_infos: &Database<BEU32, SerdeBincode<RelationInfo<'_>>>,
) -> Result<(), ArunaError> {
    RELATION_INFOS.iter().try_for_each(|info| {
        relation_infos
            .put(&mut write_txn, &info.idx, info)
            .inspect_err(logerr!())
    })?;
    Ok(())
}

const RELATION_INFOS: [RelationInfo; 12] = [
    // Resource only
    // Target can only have one origin
    RelationInfo {
        idx: 0,
        forward_type: "HasPart",
        backward_type: "PartOf",
        internal: false,
    },
    // Group -> Project only
    RelationInfo {
        idx: 1,
        forward_type: "OwnsProject",
        backward_type: "ProjectOwnedBy",
        internal: false,
    },
    //  User / Group / Token / ServiceAccount -> Resource only
    RelationInfo {
        idx: 2,
        forward_type: "PermissionNone",
        backward_type: "PermissionNone",
        internal: true, // -> Displayed by resource request
    },
    RelationInfo {
        idx: 3,
        forward_type: "PermissionRead",
        backward_type: "PermissionRead",
        internal: true,
    },
    RelationInfo {
        idx: 4,
        forward_type: "PermissionAppend",
        backward_type: "PermissionAppend",
        internal: true,
    },
    RelationInfo {
        idx: 5,
        forward_type: "PermissionWrite",
        backward_type: "PermissionWrite",
        internal: true,
    },
    RelationInfo {
        idx: 6,
        forward_type: "PermissionAdmin",
        backward_type: "PermissionAdmin",
        internal: true,
    },
    // Group -> Group only
    RelationInfo {
        idx: 7,
        forward_type: "SharesPermissionTo",
        backward_type: "PermissionSharedFrom",
        internal: true,
    },
    // Token -> User only
    RelationInfo {
        idx: 8,
        forward_type: "OwnedByUser",
        backward_type: "UserOwnsToken",
        internal: true,
    },
    // Group -> Realm
    RelationInfo {
        idx: 9,
        forward_type: "GroupPartOfRealm",
        backward_type: "RealmHasGroup",
        internal: true,
    },
    // Mutually exclusive with GroupPartOfRealm
    // Can only have a connection to one realm
    // Group -> Realm
    RelationInfo {
        idx: 10,
        forward_type: "GroupAdministratesRealm",
        backward_type: "RealmAdministratedBy",
        internal: true,
    },
    // Realm -> Endpoint
    RelationInfo {
        idx: 11,
        forward_type: "RealmUsesEndpoint",
        backward_type: "EndpointUsedByRealm",
        internal: true,
    },
];
