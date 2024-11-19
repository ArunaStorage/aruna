use crate::structs::ObjectInfo;
use heed::{
    types::{SerdeBincode, Str},
    Database, Env, EnvOpenOptions, Unspecified,
};
use ulid::Ulid;

use crate::{error::ProxyError, logerr};

pub mod db_names {
    pub const MAIN: &str = "main";
    pub const KEYS: &str = "keys";
    pub const INFO: &str = "info";
}

pub struct LmdbStore {
    env: Env,
    main: Database<Unspecified, Unspecified>,
    keys: Database<Str, Ulid>,
    info: Database<Ulid, SerdeBincode<ObjectInfo>>,
}

impl LmdbStore {
    pub fn new(path: &str) -> Result<Self, ProxyError> {
        use crate::lmdbstore::db_names::*;

        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024)
                .open(path)
                .inspect_err(logerr!())?
        };
        let mut write_txn = env.write_txn()?;

        let main = env
            .create_database(&mut write_txn, Some(MAIN))
            .inspect_err(logerr!())?;
        let keys = env
            .create_database(&mut write_txn, Some(KEYS))
            .inspect_err(logerr!())?;
        let info = env
            .create_database(&mut write_txn, Some(INFO))
            .inspect_err(logerr!())?;

        write_txn.commit().inspect_err(logerr!())?;

        Ok(Self {
            env,
            main,
            keys,
            info,
        })
    }
}
