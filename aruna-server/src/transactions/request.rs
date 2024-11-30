use super::controller::Controller;
use crate::{context::Context, error::ArunaError};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use ulid::Ulid;

pub type SerializedResponse = Vec<u8>;
pub type SerializedRequest = Vec<u8>;

pub(crate) trait Request: Debug + Send {
    type Response: Send;
    fn get_context<'a>(&'a self) -> Context;
    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError>;
}

#[typetag::serde(tag = "type")]
#[async_trait::async_trait]
pub trait WriteRequest: Send {
    async fn execute(
        &self,
        associated_event_id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, ArunaError>;

    fn as_json_value(&self) -> Result<serde_json::Value, ArunaError>
    where
        Self: Sized,
    {
        Ok(serde_json::to_value(self as &dyn WriteRequest)?)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Requester {
    User {
        user_id: Ulid,
        auth_method: AuthMethod,
        impersonated_by: Option<Ulid>,
    },
    ServiceAccount {
        service_account_id: Ulid,
        token_id: u16,
        group_id: Ulid,
        impersonated_by: Option<Ulid>,
    },
    Unregistered {
        oidc_realm: String,
        oidc_subject: String,
    },
    Component {
        server_id: Ulid,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuthMethod {
    Oidc {
        oidc_realm: String,
        oidc_subject: String,
    },
    Aruna(u16),
}

impl Requester {
    pub fn get_id(&self) -> Option<Ulid> {
        match self {
            Self::User {
                auth_method,
                user_id,
                ..
            } => match auth_method {
                AuthMethod::Oidc { .. } => Some(*user_id),
                AuthMethod::Aruna(_) => Some(*user_id),
            },
            Self::ServiceAccount {
                service_account_id, ..
            } => Some(*service_account_id),
            Self::Unregistered { .. } => None,
            Self::Component { server_id } => Some(*server_id),
        }
    }

    pub fn get_token_idx(&self) -> Option<u16> {
        match self {
            Self::ServiceAccount { token_id, .. } => Some(*token_id),
            Self::User {
                auth_method: AuthMethod::Aruna(idx),
                ..
            } => Some(*idx),
            _ => None,
        }
    }

    pub fn get_impersonator(&self) -> Option<Ulid> {
        match self {
            Self::User {
                impersonated_by, ..
            } => *impersonated_by,
            Self::ServiceAccount {
                impersonated_by, ..
            } => *impersonated_by,
            _ => None,
        }
    }
}
