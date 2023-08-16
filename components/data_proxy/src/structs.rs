use crate::database::persistence::{GenericBytes, Table, WithGenericBytes};
use anyhow::anyhow;
use anyhow::Result;
use aruna_rust_api::api::storage::models::v2::Collection;
use aruna_rust_api::api::storage::models::v2::Dataset;
use aruna_rust_api::api::storage::models::v2::{
    relation::Relation, DataClass, InternalRelationVariant, KeyValue, Object as GrpcObject,
    PermissionLevel, Project, RelationDirection, Status,
};
use aruna_rust_api::api::storage::services::v2::CreateCollectionRequest;
use aruna_rust_api::api::storage::services::v2::CreateDatasetRequest;
use aruna_rust_api::api::storage::services::v2::CreateObjectRequest;
use aruna_rust_api::api::storage::services::v2::CreateProjectRequest;
use aruna_rust_api::api::storage::services::v2::Pubkey;
use diesel_ulid::DieselUlid;
use http::Method;
use s3s::dto::CreateBucketInput;
use s3s::path::S3Path;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub enum DbPermissionLevel {
    DENY,
    NONE,
    READ,
    APPEND,
    WRITE,
    ADMIN,
}

impl From<&Method> for DbPermissionLevel {
    fn from(method: &Method) -> Self {
        match *method {
            Method::GET | Method::OPTIONS => DbPermissionLevel::READ,
            Method::POST => DbPermissionLevel::APPEND,
            Method::PUT | Method::DELETE => DbPermissionLevel::WRITE,
            _ => DbPermissionLevel::ADMIN,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct User {
    pub access_key: String,
    pub user_id: DieselUlid,
    pub secret: String,
    pub permissions: HashMap<DieselUlid, DbPermissionLevel>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ObjectType {
    PROJECT,
    COLLECTION,
    DATASET,
    OBJECT,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObjectLocation {
    pub id: DieselUlid,
    pub bucket: String,
    pub key: String,
    pub encryption_key: Option<String>,
    pub compressed: bool,
    pub raw_content_len: i64,
    pub disk_content_len: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Object {
    pub id: DieselUlid,
    pub name: String,
    pub key_values: Vec<KeyValue>,
    pub object_status: Status,
    pub data_class: DataClass,
    pub object_type: ObjectType,
    pub hashes: HashMap<String, String>,
    pub dynamic: bool,
    pub children: HashSet<DieselUlid>,
    pub parents: HashSet<DieselUlid>,
    pub synced: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PartETag {
    pub part_number: i32,
    pub etag: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PubKey {
    pub id: i32,
    pub key: String,
    pub is_proxy: bool,
}

impl From<Pubkey> for PubKey {
    fn from(value: Pubkey) -> Self {
        Self {
            id: value.id,
            key: value.key,
            is_proxy: value.location.contains("proxy"),
        }
    }
}

impl TryFrom<GenericBytes<i32>> for PubKey {
    type Error = anyhow::Error;
    fn try_from(value: GenericBytes<i32>) -> Result<Self, Self::Error> {
        Ok(bincode::deserialize(&value.data)?)
    }
}

impl TryInto<GenericBytes<i32>> for PubKey {
    type Error = anyhow::Error;
    fn try_into(self) -> Result<GenericBytes<i32>, Self::Error> {
        let data = bincode::serialize(&self)?;
        Ok(GenericBytes {
            id: self.id,
            data: data.into(),
            table: Self::get_table(),
        })
    }
}

impl WithGenericBytes<i32> for PubKey {
    fn get_table() -> Table {
        Table::PubKeys
    }
}

impl TryFrom<GenericBytes<DieselUlid>> for Object {
    type Error = anyhow::Error;
    fn try_from(value: GenericBytes<DieselUlid>) -> Result<Self, Self::Error> {
        Ok(bincode::deserialize(&value.data)?)
    }
}

impl TryInto<GenericBytes<DieselUlid>> for Object {
    type Error = anyhow::Error;
    fn try_into(self) -> Result<GenericBytes<DieselUlid>, Self::Error> {
        let data = bincode::serialize(&self)?;
        Ok(GenericBytes {
            id: self.id,
            data: data.into(),
            table: Self::get_table(),
        })
    }
}

impl WithGenericBytes<DieselUlid> for Object {
    fn get_table() -> Table {
        Table::Objects
    }
}

impl TryFrom<GenericBytes<DieselUlid>> for ObjectLocation {
    type Error = anyhow::Error;
    fn try_from(value: GenericBytes<DieselUlid>) -> Result<Self, Self::Error> {
        Ok(bincode::deserialize(&value.data)?)
    }
}

impl TryInto<GenericBytes<DieselUlid>> for ObjectLocation {
    type Error = anyhow::Error;
    fn try_into(self) -> Result<GenericBytes<DieselUlid>, Self::Error> {
        let data = bincode::serialize(&self)?;
        Ok(GenericBytes {
            id: self.id,
            data: data.into(),
            table: Self::get_table(),
        })
    }
}

impl WithGenericBytes<DieselUlid> for ObjectLocation {
    fn get_table() -> Table {
        Table::ObjectLocations
    }
}

impl TryFrom<GenericBytes<String>> for User {
    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    fn try_from(value: GenericBytes<String>) -> Result<Self, Self::Error> {
        Ok(bincode::deserialize(&value.data)?)
    }
}

impl TryInto<GenericBytes<String>> for User {
    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    fn try_into(self) -> Result<GenericBytes<String>, Self::Error> {
        let data = bincode::serialize(&self)?;
        Ok(GenericBytes {
            id: self.access_key,
            data: data.into(),
            table: Self::get_table(),
        })
    }
}

impl WithGenericBytes<String> for User {
    fn get_table() -> Table {
        Table::Users
    }
}

impl From<PermissionLevel> for DbPermissionLevel {
    fn from(level: PermissionLevel) -> Self {
        match level {
            PermissionLevel::Read => DbPermissionLevel::READ,
            PermissionLevel::Append => DbPermissionLevel::APPEND,
            PermissionLevel::Write => DbPermissionLevel::WRITE,
            PermissionLevel::Admin => DbPermissionLevel::ADMIN,
            _ => DbPermissionLevel::NONE,
        }
    }
}

impl TryFrom<Project> for Object {
    type Error = anyhow::Error;
    fn try_from(value: Project) -> Result<Self, Self::Error> {
        let (inbound, outbound): (Vec<Result<_>>, Vec<Result<_>>) = value
            .relations
            .iter()
            .filter_map(|x| {
                if let Some(rel) = &x.relation {
                    match rel {
                        Relation::Internal(var) => {
                            if var.defined_variant() == InternalRelationVariant::BelongsTo {
                                match var.direction() {
                                    RelationDirection::Inbound => {
                                        match DieselUlid::from_str(&var.resource_id) {
                                            Ok(id) => Some(Ok((id, true))),
                                            Err(e) => {
                                                println!("Error: {}", e);
                                                Some(Err(anyhow!("Invalid ULID")))
                                            }
                                        }
                                    }
                                    RelationDirection::Outbound => {
                                        match DieselUlid::from_str(&var.resource_id) {
                                            Ok(id) => Some(Ok((id, false))),
                                            Err(e) => {
                                                println!("Error: {}", e);
                                                Some(Err(anyhow!("Invalid ULID")))
                                            }
                                        }
                                    }
                                    _ => None,
                                }
                            } else {
                                None
                            }
                        }
                        _ => None,
                    }
                } else {
                    None
                }
            })
            .partition(|e| match e {
                Ok((_, inbound)) => *inbound,
                _ => false,
            });

        let inbounds = inbound
            .into_iter()
            .map(|e| e.map(|(id, _)| id))
            .collect::<Result<HashSet<DieselUlid>>>()?;
        let outbounds = outbound
            .into_iter()
            .map(|e| e.map(|(id, _)| id))
            .collect::<Result<HashSet<DieselUlid>>>()?;

        Ok(Object {
            id: DieselUlid::from_str(&value.id)?,
            name: value.name.to_string(),
            key_values: value.key_values.clone(),
            object_status: value.status(),
            data_class: value.data_class(),
            object_type: ObjectType::PROJECT,
            hashes: HashMap::default(),
            dynamic: value.dynamic,
            parents: inbounds,
            children: outbounds,
            synced: false,
        })
    }
}

impl TryFrom<Collection> for Object {
    type Error = anyhow::Error;
    fn try_from(value: Collection) -> Result<Self, Self::Error> {
        let (inbound, outbound): (Vec<Result<_>>, Vec<Result<_>>) = value
            .relations
            .iter()
            .filter_map(|x| {
                if let Some(rel) = &x.relation {
                    match rel {
                        Relation::Internal(var) => {
                            if var.defined_variant() == InternalRelationVariant::BelongsTo {
                                match var.direction() {
                                    RelationDirection::Inbound => {
                                        match DieselUlid::from_str(&var.resource_id) {
                                            Ok(id) => Some(Ok((id, true))),
                                            Err(e) => {
                                                println!("Error: {}", e);
                                                Some(Err(anyhow!("Invalid ULID")))
                                            }
                                        }
                                    }
                                    RelationDirection::Outbound => {
                                        match DieselUlid::from_str(&var.resource_id) {
                                            Ok(id) => Some(Ok((id, false))),
                                            Err(e) => {
                                                println!("Error: {}", e);
                                                Some(Err(anyhow!("Invalid ULID")))
                                            }
                                        }
                                    }
                                    _ => None,
                                }
                            } else {
                                None
                            }
                        }
                        _ => None,
                    }
                } else {
                    None
                }
            })
            .partition(|e| match e {
                Ok((_, inbound)) => *inbound,
                _ => false,
            });

        let inbounds = inbound
            .into_iter()
            .map(|e| e.map(|(id, _)| id))
            .collect::<Result<HashSet<DieselUlid>>>()?;
        let outbounds = outbound
            .into_iter()
            .map(|e| e.map(|(id, _)| id))
            .collect::<Result<HashSet<DieselUlid>>>()?;

        Ok(Object {
            id: DieselUlid::from_str(&value.id)?,
            name: value.name.to_string(),
            key_values: value.key_values.clone(),
            object_status: value.status(),
            data_class: value.data_class(),
            object_type: ObjectType::COLLECTION,
            hashes: HashMap::default(),
            dynamic: value.dynamic,
            parents: inbounds,
            children: outbounds,
            synced: false,
        })
    }
}

impl TryFrom<Dataset> for Object {
    type Error = anyhow::Error;
    fn try_from(value: Dataset) -> Result<Self, Self::Error> {
        let (inbound, outbound): (Vec<Result<_>>, Vec<Result<_>>) = value
            .relations
            .iter()
            .filter_map(|x| {
                if let Some(rel) = &x.relation {
                    match rel {
                        Relation::Internal(var) => {
                            if var.defined_variant() == InternalRelationVariant::BelongsTo {
                                match var.direction() {
                                    RelationDirection::Inbound => {
                                        match DieselUlid::from_str(&var.resource_id) {
                                            Ok(id) => Some(Ok((id, true))),
                                            Err(e) => {
                                                println!("Error: {}", e);
                                                Some(Err(anyhow!("Invalid ULID")))
                                            }
                                        }
                                    }
                                    RelationDirection::Outbound => {
                                        match DieselUlid::from_str(&var.resource_id) {
                                            Ok(id) => Some(Ok((id, false))),
                                            Err(e) => {
                                                println!("Error: {}", e);
                                                Some(Err(anyhow!("Invalid ULID")))
                                            }
                                        }
                                    }
                                    _ => None,
                                }
                            } else {
                                None
                            }
                        }
                        _ => None,
                    }
                } else {
                    None
                }
            })
            .partition(|e| match e {
                Ok((_, inbound)) => *inbound,
                _ => false,
            });

        let inbounds = inbound
            .into_iter()
            .map(|e| e.map(|(id, _)| id))
            .collect::<Result<HashSet<DieselUlid>>>()?;
        let outbounds = outbound
            .into_iter()
            .map(|e| e.map(|(id, _)| id))
            .collect::<Result<HashSet<DieselUlid>>>()?;

        Ok(Object {
            id: DieselUlid::from_str(&value.id)?,
            name: value.name.to_string(),
            key_values: value.key_values.clone(),
            object_status: value.status(),
            data_class: value.data_class(),
            object_type: ObjectType::DATASET,
            hashes: HashMap::default(),
            dynamic: value.dynamic,
            parents: inbounds,
            children: outbounds,
            synced: false,
        })
    }
}

impl TryFrom<GrpcObject> for Object {
    type Error = anyhow::Error;
    fn try_from(value: GrpcObject) -> Result<Self, Self::Error> {
        let (inbound, outbound): (Vec<Result<_>>, Vec<Result<_>>) = value
            .relations
            .iter()
            .filter_map(|x| {
                if let Some(rel) = &x.relation {
                    match rel {
                        Relation::Internal(var) => {
                            if var.defined_variant() == InternalRelationVariant::BelongsTo {
                                match var.direction() {
                                    RelationDirection::Inbound => {
                                        match DieselUlid::from_str(&var.resource_id) {
                                            Ok(id) => Some(Ok((id, true))),
                                            Err(e) => {
                                                println!("Error: {}", e);
                                                Some(Err(anyhow!("Invalid ULID")))
                                            }
                                        }
                                    }
                                    RelationDirection::Outbound => {
                                        match DieselUlid::from_str(&var.resource_id) {
                                            Ok(id) => Some(Ok((id, false))),
                                            Err(e) => {
                                                println!("Error: {}", e);
                                                Some(Err(anyhow!("Invalid ULID")))
                                            }
                                        }
                                    }
                                    _ => None,
                                }
                            } else {
                                None
                            }
                        }
                        _ => None,
                    }
                } else {
                    None
                }
            })
            .partition(|e| match e {
                Ok((_, inbound)) => *inbound,
                _ => false,
            });

        let inbounds = inbound
            .into_iter()
            .map(|e| e.map(|(id, _)| id))
            .collect::<Result<HashSet<DieselUlid>>>()?;
        let outbounds = outbound
            .into_iter()
            .map(|e| e.map(|(id, _)| id))
            .collect::<Result<HashSet<DieselUlid>>>()?;

        Ok(Object {
            id: DieselUlid::from_str(&value.id)?,
            name: value.name.to_string(),
            key_values: value.key_values.clone(),
            object_status: value.status(),
            data_class: value.data_class(),
            object_type: ObjectType::OBJECT,
            hashes: HashMap::default(),
            dynamic: value.dynamic,
            parents: inbounds,
            children: outbounds,
            synced: false,
        })
    }
}

impl From<Object> for CreateProjectRequest {
    fn from(value: Object) -> Self {
        CreateProjectRequest {
            name: value.name,
            description: "".to_string(),
            key_values: vec![],
            external_relations: vec![],
            data_class: value.data_class.into(),
        }
    }
}

impl From<Object> for CreateCollectionRequest {
    fn from(value: Object) -> Self {
        CreateCollectionRequest {
            name: value.name,
            description: "".to_string(),
            key_values: vec![],
            external_relations: vec![],
            data_class: value.data_class.into(),
            parent: value.parents.iter().next().map(|x| aruna_rust_api::api::storage::services::v2::create_collection_request::Parent::ProjectId(x.to_string())),
        }
    }
}

impl From<Object> for CreateDatasetRequest {
    fn from(value: Object) -> Self {
        CreateDatasetRequest {
            name: value.name,
            description: "".to_string(),
            key_values: vec![],
            external_relations: vec![],
            data_class: value.data_class.into(),
            parent: value.parents.iter().next().map(|x| aruna_rust_api::api::storage::services::v2::create_dataset_request::Parent::ProjectId(x.to_string())),
        }
    }
}

impl From<CreateBucketInput> for Object {
    fn from(value: CreateBucketInput) -> Self {
        Object {
            id: DieselUlid::generate(),
            name: value.bucket,
            key_values: vec![],
            object_status: Status::Available,
            data_class: DataClass::Private,
            object_type: ObjectType::PROJECT,
            hashes: HashMap::default(),
            dynamic: false,
            parents: HashSet::default(),
            children: HashSet::default(),
            synced: false,
        }
    }
}

impl From<Object> for CreateObjectRequest {
    fn from(value: Object) -> Self {
        CreateObjectRequest {
            name: value.name,
            description: "".to_string(),
            key_values: vec![],
            external_relations: vec![],
            data_class: value.data_class.into(),
            parent: value.parents.iter().next().map(|x| aruna_rust_api::api::storage::services::v2::create_object_request::Parent::ProjectId(x.to_string())),
            hashes: vec![],
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Ord)]
pub enum ResourceString {
    Project(String),
    Collection(String, String),
    Dataset(String, Option<String>, String),
    Object(String, Option<String>, Option<String>, String),
}

impl PartialOrd for ResourceString {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self {
            ResourceString::Project(_) => match other {
                ResourceString::Project(_) => Some(std::cmp::Ordering::Equal),
                _ => Some(std::cmp::Ordering::Greater),
            },
            ResourceString::Collection(_, _) => match other {
                ResourceString::Project(_) => Some(std::cmp::Ordering::Less),
                ResourceString::Collection(_, _) => Some(std::cmp::Ordering::Equal),
                _ => Some(std::cmp::Ordering::Greater),
            },
            ResourceString::Dataset(_, _, _) => match other {
                ResourceString::Project(_) => Some(std::cmp::Ordering::Less),
                ResourceString::Collection(_, _) => Some(std::cmp::Ordering::Less),
                ResourceString::Dataset(_, _, _) => Some(std::cmp::Ordering::Equal),
                _ => Some(std::cmp::Ordering::Greater),
            },
            ResourceString::Object(_, _, _, _) => match other {
                ResourceString::Project(_) => Some(std::cmp::Ordering::Less),
                ResourceString::Collection(_, _) => Some(std::cmp::Ordering::Less),
                ResourceString::Dataset(_, _, _) => Some(std::cmp::Ordering::Less),
                ResourceString::Object(_, _, _, _) => Some(std::cmp::Ordering::Equal),
            },
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, PartialOrd, Eq, Ord)]
pub struct ResourceStrings(pub Vec<ResourceString>);

#[derive(Debug, Clone, Hash, PartialEq, PartialOrd, Eq, Ord, Default)]
pub struct Missing {
    pub p: Option<String>,
    pub c: Option<String>,
    pub d: Option<String>,
    pub o: Option<String>,
}

impl ResourceStrings {
    pub fn permutate(mut self) -> (Vec<ResourceString>, Vec<(ResourceString, Missing)>) {
        let mut orig = Vec::new();
        let mut permutations = Vec::new();

        for x in self.0.drain(..) {
            match x {
                ResourceString::Project(p) => {
                    orig.push(ResourceString::Project(p.clone()));
                }
                ResourceString::Collection(p, c) => {
                    orig.push(ResourceString::Collection(p.clone(), c.clone()));
                    permutations.push((
                        ResourceString::Project(p.clone()),
                        Missing {
                            c: Some(c.clone()),
                            ..Default::default()
                        },
                    ));
                }
                ResourceString::Dataset(p, c, d) => {
                    orig.push(ResourceString::Dataset(p.clone(), c.clone(), d.clone()));
                    if let Some(c) = c {
                        permutations.push((
                            ResourceString::Collection(p.clone(), c.clone()),
                            Missing {
                                d: Some(d.clone()),
                                ..Default::default()
                            },
                        ));
                        permutations.push((
                            ResourceString::Project(p.clone()),
                            Missing {
                                c: Some(c.clone()),
                                d: Some(d.clone()),
                                ..Default::default()
                            },
                        ));
                    }
                    permutations.push((
                        ResourceString::Project(p.clone()),
                        Missing {
                            d: Some(d.clone()),
                            ..Default::default()
                        },
                    ));
                }
                ResourceString::Object(p, c, d, o) => {
                    orig.push(ResourceString::Object(
                        p.clone(),
                        c.clone(),
                        d.clone(),
                        o.clone(),
                    ));
                    if let Some(c) = &c {
                        permutations.push((
                            ResourceString::Project(p.clone()),
                            Missing {
                                c: Some(c.clone()),
                                o: Some(o.clone()),
                                ..Default::default()
                            },
                        ));

                        permutations.push((
                            ResourceString::Collection(p.clone(), c.clone()),
                            Missing {
                                o: Some(o.clone()),
                                ..Default::default()
                            },
                        ));

                        if let Some(d) = &d {
                            permutations.push((
                                ResourceString::Project(p.clone()),
                                Missing {
                                    c: Some(c.clone()),
                                    d: Some(d.clone()),
                                    o: Some(o.clone()),
                                    ..Default::default()
                                },
                            ));

                            permutations.push((
                                ResourceString::Collection(p.clone(), c.clone()),
                                Missing {
                                    d: Some(d.clone()),
                                    o: Some(o.clone()),
                                    ..Default::default()
                                },
                            ));

                            permutations.push((
                                ResourceString::Dataset(p.clone(), Some(c.clone()), d.clone()),
                                Missing {
                                    c: Some(c.clone()),
                                    d: Some(d.clone()),
                                    o: Some(o.clone()),
                                    ..Default::default()
                                },
                            ));
                        }
                    } else if let Some(d) = &d {
                        permutations.push((
                            ResourceString::Project(p.clone()),
                            Missing {
                                d: Some(d.clone()),
                                o: Some(o.clone()),
                                ..Default::default()
                            },
                        ));

                        permutations.push((
                            ResourceString::Dataset(p.clone(), None, d.clone()),
                            Missing {
                                o: Some(o.clone()),
                                ..Default::default()
                            },
                        ));
                    }
                    permutations.push((
                        ResourceString::Project(p.clone()),
                        Missing {
                            o: Some(o.clone()),
                            ..Default::default()
                        },
                    ));
                }
            }
        }

        (orig, permutations)
    }
}

impl TryFrom<&S3Path> for ResourceStrings {
    type Error = anyhow::Error;
    fn try_from(value: &S3Path) -> Result<Self> {
        if let Some((b, k)) = value.as_object() {
            let mut results = Vec::new();

            let pathvec = k.split('/').collect::<Vec<&str>>();
            match pathvec.len() {
                0 => {
                    results.push(ResourceString::Project(b.to_string()));
                }
                1 => {
                    results.push(ResourceString::Collection(
                        b.to_string(),
                        pathvec[0].to_string(),
                    ));
                    results.push(ResourceString::Dataset(
                        b.to_string(),
                        None,
                        pathvec[0].to_string(),
                    ));
                    results.push(ResourceString::Object(
                        b.to_string(),
                        None,
                        None,
                        pathvec[0].to_string(),
                    ));
                }
                2 => {
                    results.push(ResourceString::Dataset(
                        b.to_string(),
                        Some(pathvec[0].to_string()),
                        pathvec[1].to_string(),
                    ));
                    results.push(ResourceString::Object(
                        b.to_string(),
                        Some(pathvec[0].to_string()),
                        None,
                        pathvec[1].to_string(),
                    ));
                    results.push(ResourceString::Object(
                        b.to_string(),
                        None,
                        Some(pathvec[0].to_string()),
                        pathvec[1].to_string(),
                    ));
                }
                3 => {
                    results.push(ResourceString::Object(
                        b.to_string(),
                        Some(pathvec[0].to_string()),
                        Some(pathvec[1].to_string()),
                        pathvec[2].to_string(),
                    ));
                }
                _ => {
                    results.push(ResourceString::Object(
                        b.to_string(),
                        None,
                        None,
                        k.to_string(),
                    ));
                }
            }
            return Ok(ResourceStrings(results));
        } else {
            return Err(anyhow!("Invalid path"));
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, PartialOrd, Eq, Ord)]
pub enum ResourceIds {
    Project(DieselUlid),
    Collection(DieselUlid, DieselUlid),
    Dataset(DieselUlid, Option<DieselUlid>, DieselUlid),
    Object(
        DieselUlid,
        Option<DieselUlid>,
        Option<DieselUlid>,
        DieselUlid,
    ),
}

impl PartialEq<DieselUlid> for ResourceIds {
    fn eq(&self, other: &DieselUlid) -> bool {
        match self {
            ResourceIds::Project(id) => id == other,
            ResourceIds::Collection(_, id) => id == other,
            ResourceIds::Dataset(_, _, id) => id == other,
            ResourceIds::Object(_, _, _, id) => id == other,
        }
    }
}

impl ResourceIds {
    pub fn get_id(&self) -> DieselUlid {
        match self {
            ResourceIds::Project(id) => id.clone(),
            ResourceIds::Collection(_, id) => id.clone(),
            ResourceIds::Dataset(_, _, id) => id.clone(),
            ResourceIds::Object(_, _, _, id) => id.clone(),
        }
    }

    pub fn check_if_in(&self, id: DieselUlid) -> bool {
        match self {
            ResourceIds::Project(pid) => pid == &id,
            ResourceIds::Collection(pid, cid) => pid == &id || cid == &id,
            ResourceIds::Dataset(pid, cid, did) => {
                pid == &id || cid.unwrap_or_default() == id || did == &id
            }
            ResourceIds::Object(pid, cid, did, oid) => {
                pid == &id
                    || cid.unwrap_or_default() == id
                    || did.unwrap_or_default() == id
                    || oid == &id
            }
        }
    }
}
