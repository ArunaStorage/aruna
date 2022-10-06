use crate::database::schema::*;
use crate::error::{ArunaError, TypeConversionError};
use diesel_derive_enum::*;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use tonic::{Code, Status};

#[derive(Debug, DbEnum, Clone, Copy)]
#[DieselTypePath = "sql_types::ObjectStatus"]
#[DbValueStyle = "UPPERCASE"]
pub enum ObjectStatus {
    INITIALIZING,
    AVAILABLE,
    UNAVAILABLE,
    ERROR,
    TRASH,
}

#[derive(Clone, Copy, Debug, DbEnum, Deserialize, Serialize)]
#[DieselTypePath = "sql_types::EndpointType"]
#[DbValueStyle = "UPPERCASE"]
pub enum EndpointType {
    S3,
    File,
}
impl EndpointType {
    pub fn from_i32(value: i32) -> Result<EndpointType, Status> {
        match value {
            1 => Ok(EndpointType::S3),
            2 => Ok(EndpointType::File),
            _ => Err(Status::new(Code::InvalidArgument, "unknown endpoint type")),
        }
    }
}
impl FromStr for EndpointType {
    type Err = ArunaError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            "S3" => Ok(EndpointType::S3),
            "File" => Ok(EndpointType::File),
            _ => Err(ArunaError::TypeConversionError(
                TypeConversionError::STRTOENDPOINTTYPE,
            )),
        }
    }
}

#[derive(Debug, DbEnum, Clone, Copy, PartialEq, Eq, PartialOrd)]
#[DieselTypePath = "sql_types::Dataclass"]
#[DbValueStyle = "UPPERCASE"]
pub enum Dataclass {
    PUBLIC,
    PRIVATE,
    CONFIDENTIAL,
    PROTECTED,
}

#[derive(Debug, DbEnum, Clone, Copy)]
#[DieselTypePath = "sql_types::SourceType"]
#[DbValueStyle = "UPPERCASE"]
pub enum SourceType {
    URL,
    DOI,
}
impl SourceType {
    pub fn from_i32(value: i32) -> Result<SourceType, Status> {
        match value {
            1 => Ok(SourceType::URL),
            2 => Ok(SourceType::DOI),
            _ => Err(Status::new(Code::InvalidArgument, "unknown source type")),
        }
    }
}

#[derive(Debug, DbEnum, Clone, Copy)]
#[DieselTypePath = "sql_types::HashType"]
#[DbValueStyle = "UPPERCASE"]
pub enum HashType {
    MD5,
    SHA1,
    SHA256,
    SHA512,
    MURMUR3A32,
    XXHASH32,
}
impl HashType {
    pub fn from_grpc(value: i32) -> HashType {
        match value {
            1 => HashType::MD5,
            2 => HashType::SHA1,
            3 => HashType::SHA256,
            4 => HashType::SHA512,
            5 => HashType::MURMUR3A32,
            6 => HashType::XXHASH32,
            _ => HashType::MD5, // Default
        }
    }
}

#[derive(Debug, DbEnum, Clone, Copy, PartialEq, Eq)]
#[DieselTypePath = "sql_types::KeyValueType"]
#[DbValueStyle = "UPPERCASE"]
pub enum KeyValueType {
    LABEL,
    HOOK,
}

#[derive(Debug, DbEnum, Clone, Copy)]
#[DieselTypePath = "sql_types::IdentityProviderType"]
#[DbValueStyle = "UPPERCASE"]
pub enum IdentityProviderType {
    OIDC,
}

#[derive(Debug, DbEnum, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[DieselTypePath = "sql_types::UserRights"]
#[DbValueStyle = "UPPERCASE"]
pub enum UserRights {
    READ,
    APPEND,
    MODIFY,
    WRITE,
    ADMIN,
}

#[derive(Debug, DbEnum, PartialEq, Eq, Clone, Copy)]
#[DieselTypePath = "sql_types::Resources"]
#[DbValueStyle = "UPPERCASE"]
pub enum Resources {
    PROJECT,
    COLLECTION,
    OBJECT,
    OBJECTGROUP,
}

#[derive(Debug, DbEnum, Clone, Copy, PartialEq, Eq, PartialOrd)]
#[DieselTypePath = "sql_types::ReferenceStatus"]
#[DbValueStyle = "UPPERCASE"]
pub enum ReferenceStatus {
    STAGING,
    HIDDEN,
    OK,
}
