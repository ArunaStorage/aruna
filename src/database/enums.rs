use anyhow::anyhow;
use postgres_types::{FromSql, Kind, ToSql, Type};
use serde::{Deserialize, Serialize};

#[derive(
    Debug, Default, ToSql, FromSql, PartialEq, Eq, PartialOrd, Ord, Clone, Serialize, Deserialize,
)]
pub enum ObjectStatus {
    #[default]
    INITIALIZING,
    VALIDATING,
    AVAILABLE,
    UNAVAILABLE,
    ERROR,
    DELETED,
}

impl ObjectStatus {
    pub fn get_type() -> Type {
        Type::new(
            "ObjectStatus".to_string(),
            0,
            Kind::Enum(vec![
                "INITIALIZING".to_string(),
                "VALIDATING".to_string(),
                "AVAILABLE".to_string(),
                "UNAVAILABLE".to_string(),
                "ERROR".to_string(),
                "DELETED".to_string(),
            ]),
            "".to_string(),
        )
    }
}

#[derive(Debug, ToSql, FromSql, PartialEq, Eq, PartialOrd, Ord, Clone, Serialize, Deserialize)]
pub enum DataClass {
    PUBLIC,
    PRIVATE,
    WORKSPACE,
    CONFIDENTIAL,
}
impl DataClass {
    pub fn get_type() -> Type {
        Type::new(
            "DataClass".to_string(),
            0,
            Kind::Enum(vec![
                "PUBLIC".to_string(),
                "PRIVATE".to_string(),
                "WORKSPACE".to_string(),
                "CONFIDENTIAL".to_string(),
            ]),
            "".to_string(),
        )
    }
}
#[derive(Hash, Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, PartialOrd)]
pub enum ObjectMapping<T> {
    PROJECT(T),
    COLLECTION(T),
    DATASET(T),
    OBJECT(T),
}

#[derive(
    Copy,
    Debug,
    ToSql,
    FromSql,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Clone,
    Hash,
    Serialize,
    Deserialize,
    Default,
)]
pub enum ObjectType {
    #[default]
    PROJECT = 0,
    COLLECTION = 1,
    DATASET = 2,
    OBJECT = 3,
}
impl ObjectType {
    pub fn get_type() -> Type {
        Type::new(
            "ObjectType".to_string(),
            0,
            Kind::Enum(vec![
                "PROJECT".to_string(),
                "COLLECTION".to_string(),
                "DATASET".to_string(),
                "OBJECT".to_string(),
            ]),
            "".to_string(),
        )
    }
}

impl TryFrom<i32> for ObjectType {
    type Error = anyhow::Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(ObjectType::PROJECT),
            2 => Ok(ObjectType::COLLECTION),
            3 => Ok(ObjectType::DATASET),
            4 => Ok(ObjectType::OBJECT),
            _ => Err(anyhow!("Unknown object type")),
        }
    }
}

#[derive(
    Serialize, Deserialize, Debug, ToSql, FromSql, PartialEq, Eq, PartialOrd, Ord, Clone, Copy,
)]
pub enum DbPermissionLevel {
    DENY,
    NONE,
    READ,
    APPEND,
    WRITE,
    ADMIN,
}

#[derive(
    Serialize, Deserialize, Debug, Default, Hash, ToSql, FromSql, Clone, PartialEq, Eq, PartialOrd,
)]
pub enum DataProxyFeature {
    #[default]
    GRPC,
    S3,
}
#[derive(Serialize, Deserialize, Debug, Default, ToSql, FromSql, Clone, PartialEq, PartialOrd)]
pub enum EndpointVariant {
    #[default]
    PERSISTENT,
    VOLATILE,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, ToSql, Eq, PartialOrd, FromSql)]
pub enum EndpointStatus {
    INITIALIZING,
    AVAILABLE,
    DEGRADED,
    UNAVAILABLE,
    MAINTENANCE,
}

impl TryFrom<i32> for DbPermissionLevel {
    type Error = anyhow::Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(DbPermissionLevel::DENY),
            2 => Ok(DbPermissionLevel::NONE),
            3 => Ok(DbPermissionLevel::READ),
            4 => Ok(DbPermissionLevel::APPEND),
            5 => Ok(DbPermissionLevel::WRITE),
            6 => Ok(DbPermissionLevel::ADMIN),
            _ => Err(anyhow!("Unknown permission level")),
        }
    }
}

impl TryFrom<&[u8]> for ObjectStatus {
    type Error = anyhow::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        match String::from_utf8_lossy(value).as_ref() {
            "INITIALIZING" => Ok(ObjectStatus::INITIALIZING),
            "VALIDATING" => Ok(ObjectStatus::VALIDATING),
            "AVAILABLE" => Ok(ObjectStatus::AVAILABLE),
            "ERROR" => Ok(ObjectStatus::ERROR),
            "DELETED" => Ok(ObjectStatus::DELETED),
            _ => Err(anyhow!("Unknown type")),
        }
    }
}
