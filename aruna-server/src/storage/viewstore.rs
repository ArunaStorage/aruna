use crate::{
    error::ArunaError,
    logerr,
    models::{Issuer, NodeVariantValue, RawRelation, RelationInfo, ServerState},
};
use ahash::RandomState;
use heed::{
    byteorder::BigEndian,
    types::{SerdeBincode, Str, Unit, U32},
    Database, Env, EnvOpenOptions, PutFlags,
};
use jsonwebtoken::DecodingKey;
use milli::Index;
use std::{collections::HashMap, fs};
use ulid::Ulid;

// LMBD database names
const RELATION_INFO_DB_NAME: &str = "relation_infos";
pub const NODE_DB_NAME: &str = "nodes";
pub type NodeDb = Database<SerdeBincode<Ulid>, SerdeBincode<NodeVariantValue>>;
pub const RELATION_DB_NAME: &str = "relations"; // -> HashSet with Source/Type/Target
const OIDC_MAPPING_DB_NAME: &str = "oidc_mappings";
const PUBKEY_DB_NAME: &str = "pubkeys";
const ISSUER_DB_NAME: &str = "issuers";
const SERVER_INFO_DB_NAME: &str = "server_infos";

type DecodingKeyIdentifier = (String, String); // (IssuerName, KeyID)

pub struct ViewStore {
    env: heed::Env,
    // Milli index to store objects and allow for search
    milli_index: Index,

    // Database for bloom filters with name as key
    // This is not needed anymore since we can use milli to find all objects that match the name
    // and then query their parents
    // name_bloom_filter: Database<Ulid, growable_bloom_filter::GrowableBloom>,

    // Database for issuers with name as key
    issuers: Database<Str, SerdeBincode<Issuer>>,
    // Do we need a database for this ?
    // Store it in an increasing list of relations
    relations: Database<U32<BigEndian>, SerdeBincode<RawRelation>>,
    // Relations info
    relation_infos: Database<U32<BigEndian>, SerdeBincode<RelationInfo>>,

    // Volatile data
    // -------------------
    // Component status
    status: HashMap<Ulid, ServerState, RandomState>,
    issuer_decoding_keys: HashMap<DecodingKeyIdentifier, DecodingKey, RandomState>,
}

impl ViewStore {
    #[tracing::instrument(level = "trace", skip(key_serial, decoding_key))]
    pub fn new(
        path: String,
        key_serial: &u32,
        decoding_key: &DecodingKey,
    ) -> Result<Self, ArunaError> {
        fs::create_dir_all(&path)?;
        // SAFETY: This opens a memory mapped file that may introduce UB
        //         if handled incorrectly
        //         see: https://docs.rs/heed/latest/heed/struct.EnvOpenOptions.html#safety-1
        let env = unsafe {
            EnvOpenOptions::new()
                .max_dbs(16)
                .open(path)
                .inspect_err(logerr!())?
        };

        init_relations(&env)?;
        let issuer_decoding_keys = init_issuer(&env, key_serial, decoding_key)?;

        Ok(Self {
            database_env: env.clone(),
            _bloom_filter: HashMap::default(),
            status: HashMap::default(),
            issuer_decoding_keys,
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
    pub fn get_env(&self) -> Env {
        self.database_env.clone()
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

#[tracing::instrument(level = "trace", skip(key_id, decoding_key))]
fn init_issuer(
    env: &Env,
    key_id: &u32,
    decoding_key: &DecodingKey,
) -> Result<HashMap<DecodingKeyIdentifier, DecodingKey, RandomState>, ArunaError> {
    let mut write_txn = env.write_txn()?;

    let iss: IssuerDb = env
        .create_database(&mut write_txn, Some(ISSUER_DB_NAME))
        .inspect_err(logerr!())?;
    iss.put(
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
    write_txn.commit().inspect_err(logerr!())?;

    let mut iss: HashMap<DecodingKeyIdentifier, DecodingKey, RandomState> = HashMap::default();
    iss.insert(
        ("aruna".to_string(), key_id.to_string()),
        decoding_key.clone(),
    );

    Ok(iss)
}
#[tracing::instrument(level = "trace")]
fn init_relations(env: &Env) -> Result<(), ArunaError> {
    let relations = vec![
        // Resource only
        // Target can only have one origin
        RelationInfo {
            idx: 0,
            forward_type: "HasPart".to_string(),
            backward_type: "PartOf".to_string(),
            internal: false,
        },
        // Group -> Project only
        RelationInfo {
            idx: 1,
            forward_type: "OwnsProject".to_string(),
            backward_type: "ProjectOwnedBy".to_string(),
            internal: false,
        },
        //  User / Group / Token / ServiceAccount -> Resource only
        RelationInfo {
            idx: 2,
            forward_type: "PermissionNone".to_string(),
            backward_type: "PermissionNone".to_string(),
            internal: true, // -> Displayed by resource request
        },
        RelationInfo {
            idx: 3,
            forward_type: "PermissionRead".to_string(),
            backward_type: "PermissionRead".to_string(),
            internal: true,
        },
        RelationInfo {
            idx: 4,
            forward_type: "PermissionAppend".to_string(),
            backward_type: "PermissionAppend".to_string(),
            internal: true,
        },
        RelationInfo {
            idx: 5,
            forward_type: "PermissionWrite".to_string(),
            backward_type: "PermissionWrite".to_string(),
            internal: true,
        },
        RelationInfo {
            idx: 6,
            forward_type: "PermissionAdmin".to_string(),
            backward_type: "PermissionAdmin".to_string(),
            internal: true,
        },
        // Group -> Group only
        RelationInfo {
            idx: 7,
            forward_type: "SharesPermissionTo".to_string(),
            backward_type: "PermissionSharedFrom".to_string(),
            internal: true,
        },
        // Token -> User only
        RelationInfo {
            idx: 8,
            forward_type: "OwnedByUser".to_string(),
            backward_type: "UserOwnsToken".to_string(),
            internal: true,
        },
        // Group -> Realm
        RelationInfo {
            idx: 9,
            forward_type: "GroupPartOfRealm".to_string(),
            backward_type: "RealmHasGroup".to_string(),
            internal: true,
        },
        // Mutually exclusive with GroupPartOfRealm
        // Can only have a connection to one realm
        // Group -> Realm
        RelationInfo {
            idx: 10,
            forward_type: "GroupAdministratesRealm".to_string(),
            backward_type: "RealmAdministratedBy".to_string(),
            internal: true,
        },
        // Realm -> Endpoint
        RelationInfo {
            idx: 11,
            forward_type: "RealmUsesEndpoint".to_string(),
            backward_type: "EndpointUsedByRealm".to_string(),
            internal: true,
        },
    ];
    let mut write_txn = env.write_txn().inspect_err(logerr!())?;
    let db: RelationInfoDb = env
        .create_database(&mut write_txn, Some(RELATION_INFO_DB_NAME))
        .inspect_err(logerr!())?;
    for relation in relations {
        db.put(&mut write_txn, &relation.idx, &relation)
            .inspect_err(logerr!())?;
    }
    write_txn.commit().inspect_err(logerr!())?;
    Ok(())
}
