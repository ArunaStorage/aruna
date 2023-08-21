use crate::database::persistence::{GenericBytes, Table, WithGenericBytes};
use anyhow::anyhow;
use anyhow::Result;
use aruna_rust_api::api::storage::models::v2::Collection;
use aruna_rust_api::api::storage::models::v2::Dataset;
use aruna_rust_api::api::storage::models::v2::Hash;
use aruna_rust_api::api::storage::models::v2::{
    relation::Relation, DataClass, InternalRelationVariant, KeyValue, Object as GrpcObject,
    PermissionLevel, Project, RelationDirection, Status,
};
use aruna_rust_api::api::storage::services::v2::create_collection_request;
use aruna_rust_api::api::storage::services::v2::create_dataset_request;
use aruna_rust_api::api::storage::services::v2::create_object_request;
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

#[derive(Hash, Debug, Clone, PartialEq, Serialize, Deserialize, Eq, PartialOrd, Ord)]
pub enum TypedRelation {
    Project(DieselUlid),
    Collection(DieselUlid),
    Dataset(DieselUlid),
    Object(DieselUlid),
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
    pub children: Option<HashSet<TypedRelation>>,
    pub parents: Option<HashSet<TypedRelation>>,
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

impl TypedRelation {
    fn get_id(&self) -> DieselUlid {
        match self {
            TypedRelation::Project(i)
            | TypedRelation::Collection(i)
            | TypedRelation::Dataset(i)
            | TypedRelation::Object(i) => i.clone(),
        }
    }
}

impl From<&Object> for TypedRelation {
    fn from(value: &Object) -> Self {
        match value.object_type {
            ObjectType::PROJECT => TypedRelation::Project(value.id),
            ObjectType::COLLECTION => TypedRelation::Collection(value.id),
            ObjectType::DATASET => TypedRelation::Dataset(value.id),
            ObjectType::OBJECT => TypedRelation::Object(value.id),
        }
    }
}

impl TryFrom<&Relation> for TypedRelation {
    type Error = anyhow::Error;
    fn try_from(value: &Relation) -> Result<Self> {
        match value {
            Relation::External(_) => return Err(anyhow!("Invalid External rel")),
            Relation::Internal(int) => {
                let resource_id = DieselUlid::from_str(&int.resource_id)?;

                match int.resource_variant() {
                    aruna_rust_api::api::storage::models::v2::ResourceVariant::Unspecified => {
                        return Err(anyhow!("Invalid target"))
                    }
                    aruna_rust_api::api::storage::models::v2::ResourceVariant::Project => {
                        Ok(Self::Project(resource_id))
                    }
                    aruna_rust_api::api::storage::models::v2::ResourceVariant::Collection => {
                        Ok(Self::Collection(resource_id))
                    }
                    aruna_rust_api::api::storage::models::v2::ResourceVariant::Dataset => {
                        Ok(Self::Dataset(resource_id))
                    }
                    aruna_rust_api::api::storage::models::v2::ResourceVariant::Object => {
                        Ok(Self::Object(resource_id))
                    }
                }
            }
        }
    }
}
impl TryInto<create_collection_request::Parent> for TypedRelation {
    type Error = anyhow::Error;
    fn try_into(self) -> Result<create_collection_request::Parent> {
        match self {
            TypedRelation::Project(i) => {
                Ok(create_collection_request::Parent::ProjectId(i.to_string()))
            }
            _ => Err(anyhow!("Invalid ")),
        }
    }
}

impl TryInto<create_dataset_request::Parent> for TypedRelation {
    type Error = anyhow::Error;
    fn try_into(self) -> Result<create_dataset_request::Parent> {
        match self {
            TypedRelation::Project(i) => {
                Ok(create_dataset_request::Parent::ProjectId(i.to_string()))
            }
            TypedRelation::Collection(i) => {
                Ok(create_dataset_request::Parent::CollectionId(i.to_string()))
            }
            _ => Err(anyhow!("Invalid ")),
        }
    }
}

impl TryInto<create_object_request::Parent> for TypedRelation {
    type Error = anyhow::Error;
    fn try_into(self) -> Result<create_object_request::Parent> {
        match self {
            TypedRelation::Project(i) => {
                Ok(create_object_request::Parent::ProjectId(i.to_string()))
            }
            TypedRelation::Collection(i) => {
                Ok(create_object_request::Parent::CollectionId(i.to_string()))
            }
            TypedRelation::Dataset(i) => {
                Ok(create_object_request::Parent::DatasetId(i.to_string()))
            }
            _ => Err(anyhow!("Invalid ")),
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
        let (inbound, outbound): (Vec<_>, Vec<_>) = value
            .relations
            .iter()
            .filter_map(|x| {
                if let Some(rel) = &x.relation {
                    match rel {
                        Relation::Internal(var) => {
                            if var.defined_variant() == InternalRelationVariant::BelongsTo {
                                match var.direction() {
                                    RelationDirection::Inbound => {
                                        Some((TypedRelation::try_from(rel).ok()?, true))
                                    }
                                    RelationDirection::Outbound => {
                                        Some((TypedRelation::try_from(rel).ok()?, false))
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
            .partition(|(_, e)| *e);

        let inbounds = inbound
            .into_iter()
            .map(|(id, _)| id)
            .collect::<HashSet<TypedRelation>>();
        let outbounds = outbound
            .into_iter()
            .map(|(id, _)| id)
            .collect::<HashSet<TypedRelation>>();

        Ok(Object {
            id: DieselUlid::from_str(&value.id)?,
            name: value.name.to_string(),
            key_values: value.key_values.clone(),
            object_status: value.status(),
            data_class: value.data_class(),
            object_type: ObjectType::PROJECT,
            hashes: HashMap::default(),
            dynamic: value.dynamic,
            parents: Some(inbounds),
            children: Some(outbounds),
            synced: false,
        })
    }
}

impl TryFrom<Collection> for Object {
    type Error = anyhow::Error;
    fn try_from(value: Collection) -> Result<Self, Self::Error> {
        let (inbound, outbound): (Vec<_>, Vec<_>) = value
            .relations
            .iter()
            .filter_map(|x| {
                if let Some(rel) = &x.relation {
                    match rel {
                        Relation::Internal(var) => {
                            if var.defined_variant() == InternalRelationVariant::BelongsTo {
                                match var.direction() {
                                    RelationDirection::Inbound => {
                                        Some((TypedRelation::try_from(rel).ok()?, true))
                                    }
                                    RelationDirection::Outbound => {
                                        Some((TypedRelation::try_from(rel).ok()?, false))
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
            .partition(|(_, e)| *e);

        let inbounds = inbound
            .into_iter()
            .map(|(id, _)| id)
            .collect::<HashSet<TypedRelation>>();
        let outbounds = outbound
            .into_iter()
            .map(|(id, _)| id)
            .collect::<HashSet<TypedRelation>>();

        Ok(Object {
            id: DieselUlid::from_str(&value.id)?,
            name: value.name.to_string(),
            key_values: value.key_values.clone(),
            object_status: value.status(),
            data_class: value.data_class(),
            object_type: ObjectType::COLLECTION,
            hashes: HashMap::default(),
            dynamic: value.dynamic,
            parents: Some(inbounds),
            children: Some(outbounds),
            synced: false,
        })
    }
}

impl TryFrom<Dataset> for Object {
    type Error = anyhow::Error;
    fn try_from(value: Dataset) -> Result<Self, Self::Error> {
        let (inbound, outbound): (Vec<_>, Vec<_>) = value
            .relations
            .iter()
            .filter_map(|x| {
                if let Some(rel) = &x.relation {
                    match rel {
                        Relation::Internal(var) => {
                            if var.defined_variant() == InternalRelationVariant::BelongsTo {
                                match var.direction() {
                                    RelationDirection::Inbound => {
                                        Some((TypedRelation::try_from(rel).ok()?, true))
                                    }
                                    RelationDirection::Outbound => {
                                        Some((TypedRelation::try_from(rel).ok()?, false))
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
            .partition(|(_, e)| *e);

        let inbounds = inbound
            .into_iter()
            .map(|(id, _)| id)
            .collect::<HashSet<TypedRelation>>();
        let outbounds = outbound
            .into_iter()
            .map(|(id, _)| id)
            .collect::<HashSet<TypedRelation>>();

        Ok(Object {
            id: DieselUlid::from_str(&value.id)?,
            name: value.name.to_string(),
            key_values: value.key_values.clone(),
            object_status: value.status(),
            data_class: value.data_class(),
            object_type: ObjectType::DATASET,
            hashes: HashMap::default(),
            dynamic: value.dynamic,
            parents: Some(inbounds),
            children: Some(outbounds),
            synced: false,
        })
    }
}

impl TryFrom<GrpcObject> for Object {
    type Error = anyhow::Error;
    fn try_from(value: GrpcObject) -> Result<Self, Self::Error> {
        let (inbound, outbound): (Vec<_>, Vec<_>) = value
            .relations
            .iter()
            .filter_map(|x| {
                if let Some(rel) = &x.relation {
                    match rel {
                        Relation::Internal(var) => {
                            if var.defined_variant() == InternalRelationVariant::BelongsTo {
                                match var.direction() {
                                    RelationDirection::Inbound => {
                                        Some((TypedRelation::try_from(rel).ok()?, true))
                                    }
                                    RelationDirection::Outbound => {
                                        Some((TypedRelation::try_from(rel).ok()?, false))
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
            .partition(|(_, e)| *e);

        let inbounds = inbound
            .into_iter()
            .map(|(id, _)| id)
            .collect::<HashSet<TypedRelation>>();
        let outbounds = outbound
            .into_iter()
            .map(|(id, _)| id)
            .collect::<HashSet<TypedRelation>>();

        Ok(Object {
            id: DieselUlid::from_str(&value.id)?,
            name: value.name.to_string(),
            key_values: value.key_values.clone(),
            object_status: value.status(),
            data_class: value.data_class(),
            object_type: ObjectType::OBJECT,
            hashes: HashMap::default(),
            dynamic: value.dynamic,
            parents: Some(inbounds),
            children: Some(outbounds),
            synced: false,
        })
    }
}

impl From<&ResourceIds> for DieselUlid {
    fn from(value: &ResourceIds) -> Self {
        match value {
            ResourceIds::Project(id) => *id,
            ResourceIds::Collection(_, id) => *id,
            ResourceIds::Dataset(_, _, id) => *id,
            ResourceIds::Object(_, _, _, id) => *id,
        }
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
            parent: value
                .parents
                .map(|x| x.iter().next().map(|y| y.clone().try_into().ok()))
                .flatten()
                .flatten(),
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
            parent: value
                .parents
                .map(|x| x.iter().next().map(|y| y.clone().try_into().ok()))
                .flatten()
                .flatten(),
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
            parents: None,
            children: None,
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
            parent: value
                .parents
                .map(|x| x.iter().next().map(|y| y.clone().try_into().ok()))
                .flatten()
                .flatten(),
            hashes: vec![],
        }
    }
}

impl Object {
    pub fn get_hashes(&self) -> Vec<Hash> {
        self.hashes
            .iter()
            .map(|(k, v)| {
                let alg = if k == "MD5" {
                    2
                } else if k == "SHA256" {
                    1
                } else {
                    0
                };

                Hash {
                    alg,
                    hash: v.to_string(),
                }
            })
            .collect()
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum ResourceString {
    Project(String),
    Collection(String, String),
    Dataset(String, Option<String>, String),
    Object(String, Option<String>, Option<String>, String),
}

impl PartialOrd for ResourceString {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ResourceString {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self {
            ResourceString::Project(_) => match other {
                ResourceString::Project(_) => std::cmp::Ordering::Equal,
                _ => std::cmp::Ordering::Less,
            },
            ResourceString::Collection(_, _) => match other {
                ResourceString::Project(_) => std::cmp::Ordering::Greater,
                ResourceString::Collection(_, _) => std::cmp::Ordering::Equal,
                _ => std::cmp::Ordering::Less,
            },
            ResourceString::Dataset(_, _, _) => match other {
                ResourceString::Project(_) => std::cmp::Ordering::Greater,
                ResourceString::Collection(_, _) => std::cmp::Ordering::Greater,
                ResourceString::Dataset(_, _, _) => std::cmp::Ordering::Equal,
                _ => std::cmp::Ordering::Less,
            },
            ResourceString::Object(_, c1, d1, _) => match other {
                ResourceString::Object(_, c2, d2, _) => {
                    if c1.is_some() && c2.is_none() {
                        std::cmp::Ordering::Greater
                    } else if c2.is_none() && c1.is_some() {
                        std::cmp::Ordering::Less
                    } else if d1.is_some() && d2.is_none() {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Less
                    }
                }
                _ => std::cmp::Ordering::Greater,
            },
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, PartialOrd, Eq, Ord)]
pub struct ResourceStrings(pub Vec<ResourceString>);

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct Missing {
    pub p: Option<String>,
    pub c: Option<String>,
    pub d: Option<String>,
    pub o: Option<String>,
}

// s3://foo/bar/baz

impl ResourceStrings {
    pub fn permute(mut self) -> (Vec<ResourceString>, Vec<(ResourceString, Missing)>) {
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
                    results.push(ResourceString::Object(
                        b.to_string(),
                        None,
                        None,
                        pathvec[0].to_string(),
                    ));
                }
                2 => {
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
            Ok(ResourceStrings(results))
        } else {
            Err(anyhow!("Invalid path"))
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
            ResourceIds::Project(id) => *id,
            ResourceIds::Collection(_, id) => *id,
            ResourceIds::Dataset(_, _, id) => *id,
            ResourceIds::Object(_, _, _, id) => *id,
        }
    }

    pub fn get_project(&self) -> DieselUlid {
        match self {
            ResourceIds::Project(id) => *id,
            ResourceIds::Collection(id, _) => *id,
            ResourceIds::Dataset(id, _, _) => *id,
            ResourceIds::Object(id, _, _, _) => *id,
        }
    }

    pub fn get_collection(&self) -> Option<DieselUlid> {
        match self {
            ResourceIds::Project(_) => None,
            ResourceIds::Collection(_, id) => Some(*id),
            ResourceIds::Dataset(_, id, _) => *id,
            ResourceIds::Object(_, id, _, _) => *id,
        }
    }

    pub fn get_dataset(&self) -> Option<DieselUlid> {
        match self {
            ResourceIds::Project(_) => None,
            ResourceIds::Collection(_, _) => None,
            ResourceIds::Dataset(_, _, id) => Some(*id),
            ResourceIds::Object(_, _, id, _) => *id,
        }
    }

    pub fn get_object(&self) -> Option<DieselUlid> {
        match self {
            ResourceIds::Project(_) => None,
            ResourceIds::Collection(_, _) => None,
            ResourceIds::Dataset(_, _, _) => None,
            ResourceIds::Object(_, _, _, id) => Some(*id),
        }
    }

    pub fn get_typed_parent(&self) -> Option<TypedRelation> {
        match self {
            ResourceIds::Project(_) => None,
            ResourceIds::Collection(p, _) => Some(TypedRelation::Project(p.clone())),
            ResourceIds::Dataset(p, c, _) => {
                if let Some(c) = c {
                    Some(TypedRelation::Collection(c.clone()))
                } else {
                    Some(TypedRelation::Project(p.clone()))
                }
            }
            ResourceIds::Object(p, c, d, _) => {
                if let Some(d) = d {
                    Some(TypedRelation::Dataset(d.clone()))
                } else if let Some(c) = c {
                    Some(TypedRelation::Collection(c.clone()))
                } else {
                    Some(TypedRelation::Project(p.clone()))
                }
            }
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

    pub fn destructurize(
        &self,
    ) -> (
        DieselUlid,
        Option<DieselUlid>,
        Option<DieselUlid>,
        Option<DieselUlid>,
    ) {
        match self {
            ResourceIds::Project(p) => (p.clone(), None, None, None),
            ResourceIds::Collection(p, c) => (p.clone(), Some(c.clone()), None, None),
            ResourceIds::Dataset(p, c, d) => (p.clone(), c.clone(), Some(d.clone()), None),
            ResourceIds::Object(p, c, d, o) => (p.clone(), c.clone(), d.clone(), Some(o.clone())),
        }
    }
}

#[derive(Clone)]
pub struct CheckAccessResult {
    pub user_id: Option<String>,
    pub token_id: Option<String>,
    pub resource_ids: Option<ResourceIds>,
    pub missing_resources: Option<Missing>,
    pub object: Option<Object>,
}

impl CheckAccessResult {
    pub fn new(
        user_id: Option<String>,
        token_id: Option<String>,
        resource_ids: Option<ResourceIds>,
        missing_resources: Option<Missing>,
        object: Option<Object>,
    ) -> Self {
        Self {
            resource_ids,
            missing_resources,
            user_id,
            token_id,
            object,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ResourceString;

    #[test]

    fn test_resource_strings_cmp() {
        let p_a = ResourceString::Project("a".to_string());
        let o_a = ResourceString::Object(
            "a".to_string(),
            Some("a".to_string()),
            Some("a".to_string()),
            "a".to_string(),
        );
        let o_b = ResourceString::Object(
            "a".to_string(),
            None,
            Some("a".to_string()),
            "a".to_string(),
        );

        let o_c = ResourceString::Object(
            "a".to_string(),
            Some("a".to_string()),
            None,
            "a".to_string(),
        );

        let o_d = ResourceString::Object("a".to_string(), None, None, "a".to_string());

        assert!(p_a < o_a);
        assert!(o_a > o_b);
        assert!(o_b > o_c);
        assert!(o_c > o_d);
    }
}
