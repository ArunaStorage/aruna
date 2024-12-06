use std::{borrow::Cow, fs};

use crate::structs::{ObjectInfo, UploadPart};
use heed::{
    types::{SerdeBincode, Str},
    BoxedError, BytesDecode, BytesEncode, Database, Env, EnvOpenOptions, Unspecified,
};
use ulid::Ulid;

pub struct UlidCodec;

impl<'a> BytesEncode<'a> for UlidCodec {
    /// The type to encode.
    type EItem = Ulid;

    /// Encode the given item as bytes.
    fn bytes_encode(item: &'a Ulid) -> Result<Cow<'a, [u8]>, BoxedError> {
        Ok(Cow::Owned(item.to_bytes().into()))
    }
}

impl<'a> BytesDecode<'a> for UlidCodec {
    type DItem = Ulid;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        Ok(Ulid::from_bytes(bytes.try_into()?))
    }
}

use crate::{error::ProxyError, logerr};

pub mod db_names {
    pub const MAIN: &str = "main";
    pub const KEYS: &str = "keys";
    pub const INFO: &str = "info";
    pub const PARTS: &str = "parts";
}

pub struct LmdbStore {
    env: Env,
    main: Database<Unspecified, Unspecified>,
    keys: Database<Str, UlidCodec>,
    info: Database<UlidCodec, SerdeBincode<ObjectInfo>>,
    parts: Database<Str, SerdeBincode<Vec<UploadPart>>>, // UploadId <-> Vec<UploadPart>
}

impl LmdbStore {
    pub fn new(path: &str) -> Result<Self, ProxyError> {
        use crate::lmdbstore::db_names::*;

        fs::create_dir_all(path).inspect_err(logerr!())?;

        let env = unsafe {
            EnvOpenOptions::new()
                .max_dbs(10)
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
        let parts = env
            .create_database(&mut write_txn, Some(PARTS))
            .inspect_err(logerr!())?;

        write_txn.commit().inspect_err(logerr!())?;

        Ok(Self {
            env,
            main,
            keys,
            info,
            parts,
        })
    }

    pub fn get_object(&self, path: &str) -> Option<(Ulid, ObjectInfo)> {
        let read_txn = self.env.read_txn().ok()?;
        let key = self.keys.get(&read_txn, path).ok()??;
        self.info.get(&read_txn, &key).ok()?.map(|info| (key, info))
    }

    pub fn get_object_id(&self, path: &str) -> Option<Ulid> {
        let read_txn = self.env.read_txn().ok()?;
        self.keys.get(&read_txn, path).ok()?
    }

    pub fn put_key(&self, ulid: Ulid, path: &str) -> Result<(), ProxyError> {
        let mut write_txn = self.env.write_txn()?;
        self.keys
            .put(&mut write_txn, path, &ulid)
            .inspect_err(logerr!())?;
        write_txn.commit().inspect_err(logerr!())?;
        Ok(())
    }

    pub fn put_object(&self, ulid: &Ulid, path: &str, info: &ObjectInfo) -> Result<(), ProxyError> {
        let mut write_txn = self.env.write_txn()?;
        self.keys
            .put(&mut write_txn, path, ulid)
            .inspect_err(logerr!())?;
        self.info
            .put(&mut write_txn, ulid, info)
            .inspect_err(logerr!())?;
        write_txn.commit().inspect_err(logerr!())?;
        Ok(())
    }
}
