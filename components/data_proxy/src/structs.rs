use aruna_rust_api::api::storage::models::v2::{DataClass, KeyValue, Status};
use diesel_ulid::DieselUlid;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ObjectType {
    PROJECT,
    COLLECTION,
    DATASET,
    OBJECT,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Location {
    pub bucket: String,
    pub key: String,
    pub encryption_key: Option<String>,
    pub compressed: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Object {
    id: DieselUlid,
    revision_number: i64,
    name: String,
    raw_content_len: i64,
    disk_content_len: i64,
    count: i64,
    key_values: Vec<KeyValue>,
    object_status: Status,
    data_class: DataClass,
    object_type: ObjectType,
    hashes: HashMap<String, String>,
    dynamic: bool,
    endpoints: Vec<DieselUlid>,
    children: HashSet<DieselUlid>,
    location: Location,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PartETag {
    pub part_number: i32,
    pub etag: String,
}
