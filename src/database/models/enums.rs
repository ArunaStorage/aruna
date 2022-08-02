use diesel_derive_enum::*;

#[derive(Debug, DbEnum)]
#[DieselTypePath = "crate::database::schema::sql_types::ObjectStatus"]
pub enum ObjectStatus {
    INITIALIZING,
    AVAILABLE,
    UNAVAILABLE,
    ERROR,
}

#[derive(Debug, DbEnum)]
#[DieselTypePath = "crate::database::schema::sql_types::EndpointType"]
pub enum EndpointType {
    INITIALIZING,
    AVAILABLE,
    UNAVAILABLE,
    ERROR,
}

#[derive(Debug, DbEnum)]
#[DieselTypePath = "crate::database::schema::sql_types::Dataclass"]
pub enum Dataclass {
    PUBLIC,
    PRIVATE,
    CONFIDENTIAL,
    PROTECTED,
}

#[derive(Debug, DbEnum)]
#[DieselTypePath = "crate::database::schema::sql_types::SourceType"]
pub enum SourceType {
    S3,
    URL,
    DOI,
}

#[derive(Debug, DbEnum)]
#[DieselTypePath = "crate::database::schema::sql_types::KeyValueType"]
pub enum KeyValueType {
    LABEL,
    URL,
}

#[derive(Debug, DbEnum)]
#[DieselTypePath = "crate::database::schema::sql_types::IdentityProviderType"]
pub enum IdentityProviderType {
    OIDC,
}

#[derive(Debug, DbEnum)]
#[DieselTypePath = "crate::database::schema::sql_types::UserRights"]
pub enum UserRights {
    READ,
    APPEND,
    MODIFY,
    WRITE,
    ADMIN,
}

#[derive(Debug, DbEnum)]
#[DieselTypePath = "crate::database::schema::sql_types::Resources"]
pub enum Resources {
    PROJECT,
    COLLECTION,
    OBJECT,
    OBJECTGROUP,
}
