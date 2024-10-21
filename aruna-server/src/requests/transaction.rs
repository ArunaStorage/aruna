use serde::{Deserialize, Serialize};
use synevi::{SyneviError, Transaction};
use tracing::debug;
use ulid::Ulid;

use crate::{
    error::ArunaError,
    models::{
        AddGroupRequest, AddGroupResponse, CreateGroupRequest, CreateGroupResponse,
        CreateProjectRequest, CreateProjectResponse, CreateRealmRequest, CreateRealmResponse,
        CreateResourceRequest, CreateResourceResponse,
    },
};

use super::{
    controller::Controller, group::WriteGroupExecuteHandler, realm::WriteRealmExecuteHandler,
    resource::WriteResourceExecuteHandler,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Requests {
    CreateRealmRequest(CreateRealmRequest),
    CreateGroupRequest(CreateGroupRequest),
    CreateProjectRequest(CreateProjectRequest),
    CreateResourceRequest(CreateResourceRequest),
    AddGroupRequest(AddGroupRequest),
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub requester: Requester,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Fields {
    GroupId(Ulid),
    UserId(Ulid),
    ParentId(Ulid),
    ResourceId(Ulid),
    RealmId(Ulid),
    CreatedAt(i64),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArunaTransaction {
    pub request: Requests,
    pub metadata: Metadata,
    pub generated_fields: Option<Vec<Fields>>,
}

impl Transaction for ArunaTransaction {
    type TxErr = ArunaError;
    type TxOk = TransactionOk;

    fn as_bytes(&self) -> Vec<u8> {
        bincode::serialize(&self).unwrap_or_default()
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, SyneviError>
    where
        Self: Sized,
    {
        bincode::deserialize(&bytes)
            .map_err(|e| SyneviError::InvalidConversionFromBytes(e.to_string()))
    }
}

pub enum TransactionOk {
    CreateRealmResponse(CreateRealmResponse),
    CreateResourceResponse(CreateResourceResponse),
    CreateProjectResponse(CreateProjectResponse),
    CreateGroupResponse(CreateGroupResponse),
    AddGroupResponse(AddGroupResponse),
}

impl Controller {
    pub async fn process_transaction(
        &self,
        transaction: ArunaTransaction,
    ) -> Result<TransactionOk, ArunaError> {
        debug!("Started transaction");
        let res = match transaction.request {
            Requests::CreateRealmRequest(request) => {
                self.create_realm(request, transaction.metadata, transaction.generated_fields)
                    .await
            }
            Requests::CreateGroupRequest(request) => {
                self.create_group(request, transaction.metadata, transaction.generated_fields)
                    .await
            }
            Requests::CreateProjectRequest(request) => {
                self.create_project(request, transaction.metadata, transaction.generated_fields)
                    .await
            }
            Requests::CreateResourceRequest(request) => {
                self.create_resource(request, transaction.metadata, transaction.generated_fields)
                    .await
            }

            Requests::AddGroupRequest(_) => todo!(),
        };

        debug!("Finished transaction");
        res
    }
}
