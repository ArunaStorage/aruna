use crate::auth::permission_handler::PermissionHandler;
use crate::auth::structs::Context;
use crate::caching::cache::Cache;
use crate::database::enums::DbPermissionLevel;
use crate::middlelayer::create_request_types::CreateRequest;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::snapshot_request_types::SnapshotRequest;
use crate::middlelayer::update_request_types::{
    DataClassUpdate, DescriptionUpdate, KeyValueUpdate, NameUpdate,
};
use crate::search::meilisearch_client::{MeilisearchClient, ObjectDocument};
use crate::utils::conversions::get_token_from_md;
use crate::utils::grpc_utils::{self, get_id_and_ctx, query, IntoGenericInner};

use crate::database::dsls::object_dsl::ObjectWithRelations;
use crate::middlelayer::delete_request_types::DeleteRequest;
use aruna_rust_api::api::storage::models::v2::{generic_resource, Project};
use aruna_rust_api::api::storage::services::v2::project_service_server::ProjectService;
use aruna_rust_api::api::storage::services::v2::{
    ArchiveProjectRequest, ArchiveProjectResponse, CreateProjectRequest, CreateProjectResponse,
    DeleteProjectRequest, DeleteProjectResponse, GetProjectRequest, GetProjectResponse,
    GetProjectsRequest, GetProjectsResponse, UpdateProjectDataClassRequest,
    UpdateProjectDataClassResponse, UpdateProjectDescriptionRequest,
    UpdateProjectDescriptionResponse, UpdateProjectKeyValuesRequest,
    UpdateProjectKeyValuesResponse, UpdateProjectNameRequest, UpdateProjectNameResponse,
};
use diesel_ulid::DieselUlid;
use std::str::FromStr;
use std::sync::Arc;
use tonic::{Request, Response, Result};

crate::impl_grpc_server!(ProjectServiceImpl, search_client: Arc<MeilisearchClient>, default_endpoint: String);

#[tonic::async_trait]
impl ProjectService for ProjectServiceImpl {
    async fn create_project(
        &self,
        request: Request<CreateProjectRequest>,
    ) -> Result<Response<CreateProjectResponse>> {
        log_received!(&request);

        // Consume gRPC request into its parts
        let (request_metadata, _, inner_request) = request.into_parts();

        // Extract token from request and check permissions
        let token = tonic_auth!(
            get_token_from_md(&request_metadata),
            "Token authentication error"
        );

        let (user_id, _, is_dataproxy) = tonic_auth!(
            self.authorizer
                .check_permissions_verbose(&token, vec![Context::default()])
                .await,
            "Unauthorized"
        );

        // Create project in database
        let request = CreateRequest::Project(inner_request, self.default_endpoint.clone());

        let (project, user) = tonic_internal!(
            self.database_handler
                .create_resource(self.authorizer.clone(), request, user_id, is_dataproxy)
                .await,
            "Internal database error"
        );

        // Update local cache
        self.cache.add_object(project.clone());
        if let Some(user) = user {
            self.cache.update_user(&user.id.clone(), user);
        }

        // Add or update project in search index
        grpc_utils::update_search_index(
            &self.search_client,
            vec![ObjectDocument::from(project.object.clone())],
        )
        .await;

        // Create and return gRPC response
        let response = CreateProjectResponse {
            project: Some(generic_resource::Resource::from(project).into_inner()?),
        };

        return_with_log!(response);
    }

    async fn get_project(
        &self,
        request: Request<GetProjectRequest>,
    ) -> Result<Response<GetProjectResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let request = request.into_inner();

        let project_id = tonic_invalid!(
            DieselUlid::from_str(&request.project_id),
            "ULID conversion error"
        );

        let ctx = Context::res_ctx(project_id, DbPermissionLevel::READ, true);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let res = self
            .cache
            .get_object(&project_id)
            .ok_or_else(|| tonic::Status::not_found("Project not found"))?;

        let generic_project: generic_resource::Resource =
            tonic_invalid!(res.try_into(), "Invalid project");

        let response = GetProjectResponse {
            project: Some(generic_project.into_inner()?),
        };

        return_with_log!(response);
    }

    async fn get_projects(
        &self,
        request: Request<GetProjectsRequest>,
    ) -> Result<Response<GetProjectsResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let request = request.into_inner();

        let (ids, ctxs): (Vec<DieselUlid>, Vec<Context>) = get_id_and_ctx(request.project_ids)?;

        tonic_auth!(
            self.authorizer.check_permissions(&token, ctxs).await,
            "Unauthorized"
        );

        let res: Result<Vec<Project>> = ids
            .iter()
            .map(|id| -> Result<Project> {
                let proj = query(&self.cache, id)?;
                proj.into_inner()
            })
            .collect();

        let response = GetProjectsResponse { projects: res? };

        return_with_log!(response);
    }

    async fn delete_project(
        &self,
        request: Request<DeleteProjectRequest>,
    ) -> Result<Response<DeleteProjectResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = DeleteRequest::Project(request.into_inner());
        let id = tonic_invalid!(request.get_id(), "Invalid project id");

        let ctx = Context::res_ctx(id, DbPermissionLevel::ADMIN, true);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized."
        );

        let updates: Vec<ObjectWithRelations> = tonic_internal!(
            self.database_handler.delete_resource(request).await,
            "Internal database error"
        );

        let mut search_update: Vec<ObjectDocument> = vec![];
        for o in updates {
            self.cache.remove_object(&o.object.id);
            search_update.push(ObjectDocument::from(o.object))
        }

        // Add or update project in search index
        grpc_utils::update_search_index(&self.search_client, search_update).await;

        return_with_log!(DeleteProjectResponse {});
    }

    async fn update_project_name(
        &self,
        request: Request<UpdateProjectNameRequest>,
    ) -> Result<Response<UpdateProjectNameResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = NameUpdate::Project(request.into_inner());
        let project_id = tonic_invalid!(request.get_id(), "Invalid project id");
        let ctx = Context::res_ctx(project_id, DbPermissionLevel::WRITE, true);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let project = tonic_internal!(
            self.database_handler.update_name(request).await,
            "Internal database error."
        );
        self.cache
            .update_object(&project.object.id, project.clone());

        // Add or update project in search index
        grpc_utils::update_search_index(
            &self.search_client,
            vec![ObjectDocument::from(project.object.clone())],
        )
        .await;

        let project: generic_resource::Resource =
            tonic_internal!(project.try_into(), "Project conversion error");
        let response = UpdateProjectNameResponse {
            project: Some(project.into_inner()?),
        };
        return_with_log!(response);
    }

    async fn update_project_description(
        &self,
        request: Request<UpdateProjectDescriptionRequest>,
    ) -> Result<Response<UpdateProjectDescriptionResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = DescriptionUpdate::Project(request.into_inner());
        let project_id = tonic_invalid!(request.get_id(), "Invalid project id");
        let ctx = Context::res_ctx(project_id, DbPermissionLevel::WRITE, true);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let project = tonic_internal!(
            self.database_handler.update_description(request).await,
            "Internal database error."
        );
        self.cache
            .update_object(&project.object.id, project.clone());

        // Add or update project in search index
        grpc_utils::update_search_index(
            &self.search_client,
            vec![ObjectDocument::from(project.object.clone())],
        )
        .await;

        let project: generic_resource::Resource =
            tonic_internal!(project.try_into(), "Project conversion error");

        let response = UpdateProjectDescriptionResponse {
            project: Some(project.into_inner()?),
        };
        return_with_log!(response);
    }

    async fn update_project_key_values(
        &self,
        request: Request<UpdateProjectKeyValuesRequest>,
    ) -> Result<Response<UpdateProjectKeyValuesResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = KeyValueUpdate::Project(request.into_inner());
        let project_id = tonic_invalid!(request.get_id(), "Invalid project id");
        let ctx = Context::res_ctx(project_id, DbPermissionLevel::WRITE, true);

        let user_id = tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let project = tonic_internal!(
            self.database_handler
                .update_keyvals(self.authorizer.clone(), request, user_id)
                .await,
            "Internal database error."
        );
        self.cache
            .update_object(&project.object.id, project.clone());

        // Add or update project in search index
        grpc_utils::update_search_index(
            &self.search_client,
            vec![ObjectDocument::from(project.object.clone())],
        )
        .await;

        let project: generic_resource::Resource =
            tonic_internal!(project.try_into(), "Project conversion error");

        let response = UpdateProjectKeyValuesResponse {
            project: Some(project.into_inner()?),
        };
        return_with_log!(response);
    }

    async fn update_project_data_class(
        &self,
        request: Request<UpdateProjectDataClassRequest>,
    ) -> Result<Response<UpdateProjectDataClassResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = DataClassUpdate::Project(request.into_inner());
        let project_id = tonic_invalid!(request.get_id(), "Invalid project id");
        let ctx = Context::res_ctx(project_id, DbPermissionLevel::WRITE, true);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let project = tonic_internal!(
            self.database_handler.update_dataclass(request).await,
            "Internal database error."
        );
        self.cache
            .update_object(&project.object.id, project.clone());

        // Add or update project in search index
        grpc_utils::update_search_index(
            &self.search_client,
            vec![ObjectDocument::from(project.object.clone())],
        )
        .await;

        let project: generic_resource::Resource =
            tonic_internal!(project.try_into(), "Project conversion error");
        let response = UpdateProjectDataClassResponse {
            project: Some(project.into_inner()?),
        };
        return_with_log!(response);
    }

    async fn archive_project(
        &self,
        request: Request<ArchiveProjectRequest>,
    ) -> Result<Response<ArchiveProjectResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = SnapshotRequest::Project(request.into_inner());
        let project_id = tonic_invalid!(request.get_id(), "Invalid project id.");
        let ctx = Context::res_ctx(project_id, DbPermissionLevel::ADMIN, true);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let (old_id, resources) = tonic_internal!(
            self.database_handler.snapshot(request).await,
            "Internal database error."
        );

        // Update local cache and prepare search index documents
        let mut search_update: Vec<ObjectDocument> = vec![];
        for resource in resources {
            self.cache
                .update_object(&resource.object.id, resource.clone());
            search_update.push(ObjectDocument::from(resource.object))
        }

        // Add or update resources in search index
        grpc_utils::update_search_index(&self.search_client, search_update).await;

        let project: generic_resource::Resource = tonic_internal!(
            self.cache
                .get_object(&old_id)
                .ok_or_else(|| tonic::Status::not_found("Project not found"))?
                .try_into(),
            "Project conversion error"
        );
        let response = ArchiveProjectResponse {
            project: Some(project.into_inner()?),
        };
        return_with_log!(response);
    }
}
