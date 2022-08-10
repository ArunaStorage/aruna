use crate::database::schema::*;
use diesel_derive_enum::*;
use tonic::{Code, Status};

#[derive(Debug, DbEnum, Clone, Copy)]
#[DieselTypePath = "sql_types::ObjectStatus"]
pub enum ObjectStatus {
    INITIALIZING,
    AVAILABLE,
    UNAVAILABLE,
    ERROR,
}

#[derive(Debug, DbEnum, Clone, Copy)]
#[DieselTypePath = "sql_types::EndpointType"]
pub enum EndpointType {
    INITIALIZING,
    AVAILABLE,
    UNAVAILABLE,
    ERROR,
}

#[derive(Debug, DbEnum, Clone, Copy)]
#[DieselTypePath = "sql_types::Dataclass"]
pub enum Dataclass {
    PUBLIC,
    PRIVATE,
    CONFIDENTIAL,
    PROTECTED,
}

#[derive(Debug, DbEnum, Clone, Copy)]
#[DieselTypePath = "sql_types::SourceType"]
pub enum SourceType {
    URL,
    DOI
}
impl SourceType {
    pub fn from_i32(value: i32) -> Result<SourceType, Status> {
        match value {
            1 => Ok(SourceType::URL),
            2 => Ok(SourceType::DOI),
            _ => Err(Status::new(Code::InvalidArgument, "unknown source type"))
        }
    }
}

#[derive(Debug, DbEnum, Clone, Copy, PartialEq)]
#[DieselTypePath = "sql_types::KeyValueType"]
pub enum KeyValueType {
    LABEL,
    HOOK,
}

#[derive(Debug, DbEnum, Clone, Copy)]
#[DieselTypePath = "sql_types::IdentityProviderType"]
pub enum IdentityProviderType {
    OIDC,
}

#[derive(Debug, DbEnum, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[DieselTypePath = "sql_types::UserRights"]
pub enum UserRights {
    READ,
    APPEND,
    MODIFY,
    WRITE,
    ADMIN,
}

#[derive(Debug, DbEnum, PartialEq, Clone, Copy)]
#[DieselTypePath = "sql_types::Resources"]
pub enum Resources {
    PROJECT,
    COLLECTION,
    OBJECT,
    OBJECTGROUP,
}
