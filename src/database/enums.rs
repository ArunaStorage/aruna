use anyhow::anyhow;
use postgres_types::{FromSql, ToSql};
use std::error::Error;

#[derive(Debug, ToSql, FromSql)]
pub enum ObjectStatus {
    INITIALIZING,
    VALIDATING,
    AVAILABLE,
    ERROR,
    DELETED,
}

impl TryFrom<&[u8]> for ObjectStatus {
    type Error = Box<dyn Error + Sync + Send>;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        match String::from_utf8_lossy(value).as_ref() {
            "INIALIZING" => Ok(ObjectStatus::INITIALIZING),
            "VALIDATING" => Ok(ObjectStatus::VALIDATING),
            "AVAILABLE" => Ok(ObjectStatus::AVAILABLE),
            "ERROR" => Ok(ObjectStatus::ERROR),
            "DELETED" => Ok(ObjectStatus::DELETED),
            _ => Err(anyhow!("Unknown type").into()),
        }
    }
}

#[derive(Debug, ToSql, FromSql)]
pub enum DataClass {
    PUBLIC,
    PRIVATE,
    CONFIDENTIAL,
}

#[derive(Debug, ToSql, FromSql)]
pub enum ObjectType {
    PROJECT,
    COLLECTION,
    DATASET,
    OBJECT,
}

#[derive(Debug, ToSql, FromSql)]
pub enum UserRights {
    READ,
    APPEND,
    CASCADING,
    WRITE,
    ADMIN,
}
