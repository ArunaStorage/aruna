use super::auth::Auth;
use super::controller::{Get, Transaction};
use super::transaction::{ArunaTransaction, Fields, Metadata, Requests, TransactionOk};
use super::utils::{get_created_at_field, get_resource_field};
use crate::error::ArunaError;
use crate::models::{self, HAS_PART, OWNS_PROJECT};
use crate::requests::controller::Controller;
use ulid::Ulid;

// Trait auth
// Trait <Get>
// Trait <Transaction>

pub trait ReadResourceHandler: Auth + Get {
    async fn get_resource(
        &self,
        token: Option<String>,
        request: models::GetResourceRequest,
    ) -> Result<models::GetResourceResponse, ArunaError> {
        let _ = self.authorize_token(token, &request).await?;

        let id = request.id;
        let Some(models::NodeVariantValue::Resource(resource)) = self.get(id).await? else {
            tracing::error!("Resource not found: {}", id);
            return Err(ArunaError::NotFound(id.to_string()));
        };

        Ok(models::GetResourceResponse {
            resource,
            relations: vec![], // TODO: Add get_relations to Get trait
        })
    }
}

pub trait WriteResourceRequestHandler: Transaction + Auth + Get {
    async fn create_resource(
        &self,
        token: Option<String>,
        request: models::CreateResourceRequest,
    ) -> Result<models::CreateResourceResponse, ArunaError> {
        let transaction_id = u128::from_be_bytes(Ulid::new().to_bytes());
        let resource_id = Ulid::new();
        let created_at = chrono::Utc::now().timestamp();

        // TODO: Auth

        let requester = self
            .authorize_token(token, &request)
            .await?
            .ok_or_else(|| {
                tracing::error!("Requester not found");
                ArunaError::Unauthorized
            })?;

        let TransactionOk::CreateResourceResponse(response) = self
            .transaction(
                transaction_id,
                ArunaTransaction {
                    request: Requests::CreateResourceRequest(request),
                    metadata: Metadata { requester },
                    generated_fields: Some(vec![
                        Fields::ResourceId(resource_id),
                        Fields::CreatedAt(created_at),
                    ]),
                },
            )
            .await?
        else {
            tracing::error!("Unexpected response: Not CreateResourceResponse");
            return Err(ArunaError::TransactionFailure(
                "Unexpected response: Not CreateResourceResponse".to_string(),
            ));
        };
        Ok(response)
    }

    async fn create_project(
        &self,
        token: Option<String>,
        request: models::CreateProjectRequest,
    ) -> Result<models::CreateProjectResponse, ArunaError> {
        let transaction_id = u128::from_be_bytes(Ulid::new().to_bytes());
        let resource_id = Ulid::new();
        let created_at = chrono::Utc::now().timestamp();

        // TODO: Auth

        let requester = self
            .authorize_token(token, &request)
            .await?
            .ok_or_else(|| {
                tracing::error!("Requester not found");
                ArunaError::Unauthorized
            })?;

        let TransactionOk::CreateProjectResponse(response) = self
            .transaction(
                transaction_id,
                ArunaTransaction {
                    request: Requests::CreateProjectRequest(request),
                    metadata: Metadata { requester },
                    generated_fields: Some(vec![
                        Fields::ResourceId(resource_id),
                        Fields::CreatedAt(created_at),
                    ]),
                },
            )
            .await?
        else {
            tracing::error!("Unexpected response: Not CreateProjectResponse");
            return Err(ArunaError::TransactionFailure(
                "Unexpected response: Not CreateProjectResponse".to_string(),
            ));
        };
        Ok(response)
    }
}

impl ReadResourceHandler for Controller {}

impl WriteResourceRequestHandler for Controller {}

pub trait WriteResourceExecuteHandler: Auth + Get {
    async fn create_resource(
        &self,
        request: models::CreateResourceRequest,
        metadata: Metadata,
        fields: Option<Vec<Fields>>,
    ) -> Result<TransactionOk, ArunaError>;
    async fn create_project(
        &self,
        request: models::CreateProjectRequest,
        metadata: Metadata,
        fields: Option<Vec<Fields>>,
    ) -> Result<TransactionOk, ArunaError>;
}

impl WriteResourceExecuteHandler for Controller {
    async fn create_resource(
        &self,
        request: models::CreateResourceRequest,
        metadata: Metadata,
        fields: Option<Vec<Fields>>,
    ) -> Result<TransactionOk, ArunaError> {
        self.authorize(&metadata.requester, &request).await?;

        let resource_id = get_resource_field(&fields)?;
        let created_at = get_created_at_field(&fields)?;

        let status = match request.variant {
            models::ResourceVariant::Folder => models::ResourceStatus::StatusAvailable,
            models::ResourceVariant::Object => models::ResourceStatus::StatusInitializing,
            _ => {
                tracing::error!("Unexpected resource type");
                return Err(ArunaError::TransactionFailure(
                    "Unexpected resource type".to_string(),
                ));
            }
        };
        let resource = models::Resource {
            id: resource_id,
            name: request.name,
            title: request.title,
            description: request.description,
            revision: 0,
            variant: request.variant,
            labels: request.labels,
            hook_status: Vec::new(),
            identifiers: request.identifiers,
            content_len: 0,
            count: 0,
            visibility: request.visibility,
            created_at,
            last_modified: created_at,
            authors: request.authors,
            status,
            locked: false,
            license_tag: request.license_tag,
            endpoint_status: Vec::new(),
            hashes: Vec::new(),
        };

        let mut lock = self.store.write().await;
        let env = lock.view_store.get_env();
        lock.view_store
            .add_node(models::NodeVariantValue::Resource(resource.clone()))?;
        // TODO: Create Admin group and set user as admin for this group
        lock.graph
            .add_node(models::NodeVariantId::Resource(resource_id));
        lock.graph
            .add_relation(
                models::NodeVariantId::Resource(request.parent_id),
                models::NodeVariantId::Resource(resource_id),
                HAS_PART,
                env,
            )
            .await?;

        Ok(TransactionOk::CreateResourceResponse(
            models::CreateResourceResponse { resource },
        ))
    }

    async fn create_project(
        &self,
        request: models::CreateProjectRequest,
        metadata: Metadata,
        fields: Option<Vec<Fields>>,
    ) -> Result<TransactionOk, ArunaError> {
        self.authorize(&metadata.requester, &request).await?;

        let resource_id = get_resource_field(&fields)?;
        let created_at = get_created_at_field(&fields)?;

        let resource = models::Resource {
            id: resource_id,
            name: request.name,
            title: request.title,
            description: request.description,
            revision: 0,
            variant: models::ResourceVariant::Project,
            labels: request.labels,
            hook_status: Vec::new(),
            identifiers: request.identifiers,
            content_len: 0,
            count: 0,
            visibility: request.visibility,
            created_at,
            last_modified: created_at,
            authors: request.authors,
            status: models::ResourceStatus::StatusAvailable,
            locked: false,
            license_tag: request.license_tag,
            endpoint_status: Vec::new(),
            hashes: Vec::new(),
        };

        let mut lock = self.store.write().await;
        let env = lock.view_store.get_env();
        lock.view_store
            .add_node(models::NodeVariantValue::Resource(resource.clone()))?;
        // TODO: Create Admin group and set user as admin for this group
        lock.graph
            .add_node(models::NodeVariantId::Resource(resource_id));
        lock.graph
            .add_relation(
                models::NodeVariantId::Group(request.group_id),
                models::NodeVariantId::Resource(resource_id),
                OWNS_PROJECT,
                env,
            )
            .await?;

        Ok(TransactionOk::CreateProjectResponse(
            models::CreateProjectResponse { resource },
        ))
    }
}
