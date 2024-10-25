use std::sync::Arc;

use crate::{context::Context, error::ArunaError};
use heed::byteorder::{BigEndian, ByteOrder, WriteBytesExt};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use ulid::Ulid;
use super::controller::Controller;

pub type SerializedResponse = Vec<u8>;
pub type SerializedRequest = Vec<u8>;

pub trait Request {
    type Response;
    fn get_context(&self) -> &Context;
    async fn run_request(self, requester: Option<Requester>, controller: &Controller) -> Result<Self::Response, ArunaError>;
}

pub trait WriteRequest: Serialize + DeserializeOwned + Sized {
    async fn execute(&self, controller: &Controller) -> Result<SerializedResponse, ArunaError>;

    fn into_bytes(self, expected_u16: u16) -> Result<SerializedRequest, ArunaError> {
        let mut bytes = Vec::new();
        bytes.write_u16::<BigEndian>(expected_u16)?;
        bincode::serialize_into(&mut bytes, &self)?;
        Ok(bytes)
    }
    fn from_bytes(bytes: SerializedRequest) -> Result<Self, ArunaError> {
        let self_deser: Self = bincode::deserialize(&bytes[2..])?;
        Ok(self_deser)
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Requester {
    User {
        user_id: Ulid,
        auth_method: AuthMethod,
    },
    ServiceAccount {
        service_account_id: Ulid,
        token_id: Ulid,
        group_id: Ulid,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuthMethod {
    Oidc {
        oidc_realm: String,
        oidc_subject: String,
    },
    Aruna(Ulid),
}

impl Requester {
    pub fn get_id(&self) -> Ulid {
        match self {
            Self::User {
                auth_method,
                user_id,
            } => match auth_method {
                AuthMethod::Oidc { .. } => *user_id,
                AuthMethod::Aruna(token_id) => *token_id,
            },
            Self::ServiceAccount { token_id, .. } => *token_id,
        }
    }
}