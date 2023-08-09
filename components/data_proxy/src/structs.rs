use crate::database::persistence::{GenericBytes, Table, WithGenericBytes};
use anyhow::bail;
use anyhow::Result;
use aruna_rust_api::api::storage::models::v2::Collection;
use aruna_rust_api::api::storage::models::v2::Dataset;
use aruna_rust_api::api::storage::models::v2::{
    relation::Relation, DataClass, InternalRelationVariant, KeyValue, PermissionLevel, Project,
    RelationDirection, Status,
};
use diesel_ulid::DieselUlid;
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
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Object {
    pub id: DieselUlid,
    pub name: String,
    pub raw_content_len: i64,
    pub disk_content_len: i64,
    pub count: i64,
    pub key_values: Vec<KeyValue>,
    pub object_status: Status,
    pub data_class: DataClass,
    pub object_type: ObjectType,
    pub hashes: HashMap<String, String>,
    pub dynamic: bool,
    pub children: HashSet<DieselUlid>,
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
        let filtered_relations = value
            .relations
            .iter()
            .filter(|x| {
                if let Some(rel) = &x.relation {
                    match rel {
                        Relation::Internal(var) => {
                            var.defined_variant() == InternalRelationVariant::BelongsTo
                                && var.direction() == RelationDirection::Outbound
                        }
                        _ => false,
                    }
                } else {
                    false
                }
            })
            .map(|x| {
                if let Some(rel) = &x.relation {
                    match rel {
                        Relation::Internal(var) => Ok(DieselUlid::from_str(&var.resource_id)?),
                        _ => bail!("No relation found"),
                    }
                } else {
                    bail!("No relation found")
                }
            })
            .collect::<Result<HashSet<DieselUlid>>>()?;

        Ok(Object {
            id: DieselUlid::from_str(&value.id)?,
            name: value.name.to_string(),
            raw_content_len: 0,
            disk_content_len: 0,
            count: 0,
            key_values: value.key_values.clone(),
            object_status: value.status(),
            data_class: value.data_class(),
            object_type: ObjectType::PROJECT,
            hashes: HashMap::default(),
            dynamic: value.dynamic,
            children: filtered_relations,
            synced: false,
        })
    }
}

impl TryFrom<Collection> for Object {
    type Error = anyhow::Error;
    fn try_from(value: Collection) -> Result<Self, Self::Error> {
        let filtered_relations = value
            .relations
            .iter()
            .filter(|x| {
                if let Some(rel) = &x.relation {
                    match rel {
                        Relation::Internal(var) => {
                            var.defined_variant() == InternalRelationVariant::BelongsTo
                                && var.direction() == RelationDirection::Outbound
                        }
                        _ => false,
                    }
                } else {
                    false
                }
            })
            .map(|x| {
                if let Some(rel) = &x.relation {
                    match rel {
                        Relation::Internal(var) => Ok(DieselUlid::from_str(&var.resource_id)?),
                        _ => bail!("No relation found"),
                    }
                } else {
                    bail!("No relation found")
                }
            })
            .collect::<Result<HashSet<DieselUlid>>>()?;

        Ok(Object {
            id: DieselUlid::from_str(&value.id)?,
            name: value.name.to_string(),
            raw_content_len: 0,
            disk_content_len: 0,
            count: 0,
            key_values: value.key_values.clone(),
            object_status: value.status(),
            data_class: value.data_class(),
            object_type: ObjectType::COLLECTION,
            hashes: HashMap::default(),
            dynamic: value.dynamic,
            children: filtered_relations,
            synced: false,
        })
    }
}

impl TryFrom<Dataset> for Object {
    type Error = anyhow::Error;
    fn try_from(value: Dataset) -> Result<Self, Self::Error> {
        let filtered_relations = value
            .relations
            .iter()
            .filter(|x| {
                if let Some(rel) = &x.relation {
                    match rel {
                        Relation::Internal(var) => {
                            var.defined_variant() == InternalRelationVariant::BelongsTo
                                && var.direction() == RelationDirection::Outbound
                        }
                        _ => false,
                    }
                } else {
                    false
                }
            })
            .map(|x| {
                if let Some(rel) = &x.relation {
                    match rel {
                        Relation::Internal(var) => Ok(DieselUlid::from_str(&var.resource_id)?),
                        _ => bail!("No relation found"),
                    }
                } else {
                    bail!("No relation found")
                }
            })
            .collect::<Result<HashSet<DieselUlid>>>()?;

        Ok(Object {
            id: DieselUlid::from_str(&value.id)?,
            name: value.name.to_string(),
            raw_content_len: 0,
            disk_content_len: 0,
            count: 0,
            key_values: value.key_values.clone(),
            object_status: value.status(),
            data_class: value.data_class(),
            object_type: ObjectType::DATASET,
            hashes: HashMap::default(),
            dynamic: value.dynamic,
            children: filtered_relations,
            synced: false,
        })
    }
}
