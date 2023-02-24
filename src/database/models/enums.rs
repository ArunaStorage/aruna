use crate::database::schema::*;
use crate::error::{ArunaError, TypeConversionError};
use diesel_derive_enum::*;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use tonic::{Code, Status};

#[derive(Debug, DbEnum, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[ExistingTypePath = "sql_types::ObjectStatus"]
#[DbValueStyle = "UPPERCASE"]
pub enum ObjectStatus {
    INITIALIZING,
    AVAILABLE,
    UNAVAILABLE,
    ERROR,
    DELETED,
    TRASH,
}

#[derive(Clone, Copy, Debug, DbEnum, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[ExistingTypePath = "sql_types::EndpointType"]
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

#[derive(Debug, DbEnum, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[ExistingTypePath = "sql_types::Dataclass"]
#[DbValueStyle = "UPPERCASE"]
pub enum Dataclass {
    PUBLIC,
    PRIVATE,
    CONFIDENTIAL,
    PROTECTED,
}

#[derive(Debug, DbEnum, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[ExistingTypePath = "sql_types::SourceType"]
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

#[derive(Debug, DbEnum, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[ExistingTypePath = "sql_types::HashType"]
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
#[ExistingTypePath = "sql_types::KeyValueType"]
#[DbValueStyle = "UPPERCASE"]
pub enum KeyValueType {
    LABEL,
    HOOK,
}

#[derive(Debug, DbEnum, Clone, Copy)]
#[ExistingTypePath = "sql_types::IdentityProviderType"]
#[DbValueStyle = "UPPERCASE"]
pub enum IdentityProviderType {
    OIDC,
}

#[derive(Debug, DbEnum, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[ExistingTypePath = "sql_types::UserRights"]
#[DbValueStyle = "UPPERCASE"]
pub enum UserRights {
    NONE,
    READ,
    APPEND,
    MODIFY,
    WRITE,
    ADMIN,
}

#[derive(Debug, DbEnum, PartialEq, Eq, Clone, Copy)]
#[ExistingTypePath = "sql_types::Resources"]
#[DbValueStyle = "UPPERCASE"]
pub enum Resources {
    PROJECT,
    COLLECTION,
    OBJECT,
    OBJECTGROUP,
}

#[derive(Debug, DbEnum, Clone, Copy, PartialEq, Eq, PartialOrd)]
#[ExistingTypePath = "sql_types::ReferenceStatus"]
#[DbValueStyle = "UPPERCASE"]
pub enum ReferenceStatus {
    STAGING,
    HIDDEN,
    OK,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_type_from_i32_test() {
        assert!(EndpointType::from_i32(1).unwrap() == EndpointType::S3);
        assert!(EndpointType::from_i32(2).unwrap() == EndpointType::File);
        assert!(EndpointType::from_i32(0).is_err());
        assert!(EndpointType::from_i32(-1).is_err());
        assert!(EndpointType::from_i32(3).is_err())
    }

    #[test]
    fn endpoint_type_from_str_test() {
        assert!(EndpointType::from_str("S3").unwrap() == EndpointType::S3);
        assert!(EndpointType::from_str("File").unwrap() == EndpointType::File);
        assert!(EndpointType::from_str("UNKNOWN").is_err());
        assert!(EndpointType::from_str("TEST").is_err())
    }

    #[test]
    fn source_type_from_i32_test() {
        assert!(SourceType::from_i32(1).unwrap() == SourceType::URL);
        assert!(SourceType::from_i32(2).unwrap() == SourceType::DOI);
        assert!(SourceType::from_i32(0).is_err());
        assert!(SourceType::from_i32(-1).is_err());
        assert!(SourceType::from_i32(3).is_err());
    }

    #[test]
    fn hash_type_from_grpc_test() {
        assert!(HashType::from_grpc(0) == HashType::MD5);
        assert!(HashType::from_grpc(1) == HashType::MD5);
        assert!(HashType::from_grpc(2) == HashType::SHA1);
        assert!(HashType::from_grpc(3) == HashType::SHA256);
        assert!(HashType::from_grpc(4) == HashType::SHA512);
        assert!(HashType::from_grpc(5) == HashType::MURMUR3A32);
        assert!(HashType::from_grpc(6) == HashType::XXHASH32);
        assert!(HashType::from_grpc(7) == HashType::MD5);
        assert!(HashType::from_grpc(100000) == HashType::MD5); // Default -> MD5
    }
}
