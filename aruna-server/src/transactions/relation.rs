use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::{
    constants::relation_types,
    context::Context,
    error::ArunaError,
    models::{
        models::{RelationInfo, Resource},
        requests::{
            CreateRelationRequest, CreateRelationResponse, CreateRelationVariantRequest,
            CreateRelationVariantResponse, Direction, GetRelationInfosRequest,
            GetRelationInfosResponse, GetRelationsRequest, GetRelationsResponse,
        },
    },
    transactions::request::WriteRequest,
};

use super::{
    controller::Controller,
    request::{Request, Requester, SerializedResponse},
};

impl Request for GetRelationsRequest {
    type Response = GetRelationsResponse;
    fn get_context(&self) -> Context {
        Context::Permission {
            min_permission: crate::models::models::Permission::Read,
            source: self.node,
        }
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        let public = if let Some(requester) = requester {
            controller.authorize(&requester, &self).await?;
            false
        } else {
            true
        };

        let store = controller.get_store();
        let response = tokio::task::spawn_blocking(move || {
            let rtxn = store.read_txn()?;

            let idx = store
                .get_idx_from_ulid(&self.node, &rtxn)
                .ok_or_else(|| ArunaError::NotFound(self.node.to_string()))?;

            // Check if resource is public
            let resource = store
                .get_node::<Resource>(&rtxn, idx)
                .ok_or_else(|| ArunaError::NotFound(self.node.to_string()))?;
            if public {
                if !matches!(
                    resource.visibility,
                    crate::models::models::VisibilityClass::Public
                ) {
                    return Err(ArunaError::Unauthorized);
                }
            }

            let direction = match self.direction {
                Direction::Incoming => petgraph::Direction::Incoming,
                Direction::Outgoing => petgraph::Direction::Outgoing,
            };
            let offset = self.offset.unwrap_or_default();
            let relations = store.get_relations(idx, &self.filter, direction, &rtxn)?;
            let new_offset = if relations.len() < offset + self.page_size {
                None
            } else {
                Some(offset + self.page_size)
            };

            let relations = match relations.get(offset..offset + self.page_size) {
                Some(relations) => relations.to_vec(),
                None => match relations.get(offset..) {
                    Some(relations) => relations.to_vec(),
                    None => relations,
                },
            };

            Ok::<_, ArunaError>(GetRelationsResponse {
                relations,
                offset: new_offset,
            })
        })
        .await
        .map_err(|e| ArunaError::ServerError(e.to_string()))??;

        Ok(response)
    }
}

impl Request for GetRelationInfosRequest {
    type Response = GetRelationInfosResponse;
    fn get_context(&self) -> Context {
        Context::Public
    }

    async fn run_request(
        self,
        _requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        let store = controller.get_store();
        tokio::task::spawn_blocking(move || {
            let rtxn = store.read_txn()?;
            let relation_infos = store.get_relation_infos(&rtxn)?;
            Ok::<_, ArunaError>(GetRelationInfosResponse { relation_infos })
        })
        .await
        .map_err(|e| ArunaError::ServerError(e.to_string()))?
    }
}

impl Request for CreateRelationRequest {
    type Response = CreateRelationResponse;
    fn get_context(&self) -> Context {
        Context::PermissionFork {
            first_source: self.source,
            first_min_permission: crate::models::models::Permission::Write,
            second_min_permission: crate::models::models::Permission::Write,
            second_source: self.source,
        }
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        let request_tx = CreateRelationTx {
            req: self,
            requester: requester.ok_or_else(|| ArunaError::Unauthorized)?,
        };

        let response = controller.transaction(Ulid::new().0, &request_tx).await?;

        Ok(bincode::deserialize(&response)?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateRelationTx {
    req: CreateRelationRequest,
    requester: Requester,
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for CreateRelationTx {
    async fn execute(
        &self,
        associated_event_id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, crate::error::ArunaError> {
        controller.authorize(&self.requester, &self.req).await?;

        let source_id = self.req.source;
        let target_id = self.req.target;
        let variant = self.req.variant;

        let store = controller.get_store();
        Ok(tokio::task::spawn_blocking(move || {
            let mut wtxn = store.write_txn()?;

            let Some(source_idx) = store.get_idx_from_ulid(&source_id, wtxn.get_txn()) else {
                return Err(ArunaError::NotFound(format!("{source_id} not found")));
            };
            let Some(target_idx) = store.get_idx_from_ulid(&target_id, wtxn.get_txn()) else {
                return Err(ArunaError::NotFound(format!("{source_id} not found")));
            };
            match variant {
                relation_types::HAS_PART..=relation_types::PROJECT_PART_OF_REALM => {
                    return Err(ArunaError::Forbidden(
                        "Forbidden to set internal relations".to_string(),
                    ));
                }
                _ => {
                    if !store.get_relation_info(&variant, wtxn.get_txn())?.is_some() {
                        return Err(ArunaError::NotFound(format!(
                            "Relation variant_idx {variant} not found"
                        )));
                    }
                }
            }
            store.create_relation(&mut wtxn, source_idx, target_idx, variant)?;
            store.register_event(&mut wtxn, associated_event_id, &[source_idx, target_idx])?;

            wtxn.commit()?;

            Ok::<_, ArunaError>(bincode::serialize(&CreateRelationResponse {})?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}

impl Request for CreateRelationVariantRequest {
    type Response = CreateRelationVariantResponse;
    fn get_context(&self) -> Context {
        Context::UserOnly
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        let request_tx = CreateRelationVariantTx {
            req: self,
            requester: requester.ok_or_else(|| ArunaError::Unauthorized)?,
        };
        let response = controller.transaction(Ulid::new().0, &request_tx).await?;
        Ok(bincode::deserialize(&response)?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateRelationVariantTx {
    req: CreateRelationVariantRequest,
    requester: Requester,
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for CreateRelationVariantTx {
    async fn execute(
        &self,
        _associated_event_id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, crate::error::ArunaError> {
        controller.authorize(&self.requester, &self.req).await?;

        let store = controller.get_store();
        let forward_type = self.req.forward_type.clone();
        let backward_type = self.req.backward_type.clone();

        Ok(tokio::task::spawn_blocking(move || {
            let mut wtxn = store.write_txn()?;

            let idx = store.get_relation_infos(&wtxn.get_txn())?.len() as u32;
            let info = RelationInfo {
                idx,
                forward_type,
                backward_type,
                internal: false,
            };
            store.create_relation_variant(&mut wtxn, info)?;

            wtxn.commit()?;
            Ok::<_, ArunaError>(bincode::serialize(&CreateRelationVariantResponse {idx})?)
        })
        .await
        .map_err(|_e| {
            tracing::error!("Failed to join task");
            ArunaError::ServerError("".to_string())
        })??)
    }
}
