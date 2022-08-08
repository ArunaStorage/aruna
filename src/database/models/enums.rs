use crate::database::schema::*;
use diesel_derive_enum::*;

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
    S3,
    URL,
    DOI,
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
