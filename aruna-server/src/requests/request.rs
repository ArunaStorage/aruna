use super::controller::Controller;
use crate::{context::Context, error::ArunaError};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

pub type SerializedResponse = Vec<u8>;
pub type SerializedRequest = Vec<u8>;

pub trait Request: Send {
    type Response: Send;
    fn get_context<'a>(&'a self) -> &'a Context;
    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError>;
}

#[typetag::serde(tag = "type")]
#[async_trait::async_trait]
pub trait WriteRequest: Send {
    async fn execute(&self, controller: &Controller) -> Result<SerializedResponse, ArunaError>;
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
