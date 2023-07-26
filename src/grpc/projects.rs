use crate::middlelayer::create_request_types::CreateRequest;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::delete_request_types::DeleteRequest;
use crate::middlelayer::update_request_types::{
    DataClassUpdate, DescriptionUpdate, KeyValueUpdate, NameUpdate,
};
use aruna_rust_api::api::storage::models::v2::generic_resource;

use crate::utils::conversions::get_token_from_md;
use crate::utils::grpc_utils::IntoGenericInner;
use aruna_cache::notifications::NotificationCache;
use aruna_policy::ape::policy_evaluator::PolicyEvaluator;
use aruna_policy::ape::structs::{Context, PermissionLevels};
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

crate::impl_grpc_server!(ProjectServiceImpl);

#[tonic::async_trait]
impl ProjectService for ProjectServiceImpl {
    async fn create_project(
        &self,
        request: Request<CreateProjectRequest>,
    ) -> Result<Response<CreateProjectResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let request = CreateRequest::Project(request.into_inner());

        let ctx = Context::res_proj(None);

        let user_id = tonic_auth!(
            self.authorizer.check_context(&token, ctx).await,
            "Unauthorized"
        )
        .ok_or(tonic::Status::invalid_argument("Missing user id"))?;

        let (generic_project, shared_id, cache_res) = tonic_internal!(
            self.database_handler
                .create_resource(request, user_id)
                .await,
            "Internal database error"
        );

        tonic_internal!(
            self.cache.cache.process_api_resource_update(
                generic_project.clone(),
                shared_id,
                cache_res
            ),
            "Caching error"
        );

        let response = CreateProjectResponse {
            project: Some(generic_project.into_inner()?),
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

        let ctx = Context::res_proj(Some((project_id, PermissionLevels::READ, true)));

        tonic_auth!(
            self.authorizer.check_context(&token, ctx).await,
            "Unauthorized"
        );

        let res = self
            .cache
            .get_resource(&aruna_cache::structs::Resource::Project(project_id))
            .ok_or_else(|| tonic::Status::not_found("Project not found"))?;

        let response = GetProjectResponse {
            project: Some(res.into_inner()?),
        };

        return_with_log!(response);
    }

    async fn get_projects(
        &self,
        _request: Request<GetProjectsRequest>,
    ) -> Result<Response<GetProjectsResponse>> {
        todo!()
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
        let id = tonic_invalid!(request.get_id(), "Invalid collection id.");

        let ctx = Context::res_proj(Some((id, PermissionLevels::WRITE, true)));

        tonic_auth!(
            self.authorizer.check_context(&token, ctx).await,
            "Unauthorized."
        );

        let updates: Vec<(
            generic_resource::Resource,
            DieselUlid,
            aruna_cache::structs::Resource,
        )> = tonic_internal!(
            self.database_handler.delete_resource(request).await,
            "Internal database error"
        );

        for u in updates {
            tonic_internal!(
                self.cache.cache.process_api_resource_update(u.0, u.1, u.2),
                "Caching error"
            );
        }

        let response = DeleteProjectResponse {};

        return_with_log!(response);
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
        let id = tonic_invalid!(request.get_id(), "Invalid collection id.");

        let ctx = Context::res_proj(Some((id, PermissionLevels::WRITE, true)));

        tonic_auth!(
            self.authorizer.check_context(&token, ctx).await,
            "Unauthorized."
        );

        let (generic_resource, shared_id, cached_rs) = tonic_internal!(
            self.database_handler.update_name(request).await,
            "Internal database error"
        );

        tonic_internal!(
            self.cache
                .cache
                .process_api_resource_update(generic_resource, shared_id, cached_rs),
            "Caching error"
        );

        let response = UpdateProjectNameResponse {
            project: Some(generic_resource.into_inner()?),
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
        let id = tonic_invalid!(request.get_id(), "Invalid project id.");

        let ctx = Context::res_proj(Some((id, PermissionLevels::WRITE, true)));

        tonic_auth!(
            self.authorizer.check_context(&token, ctx).await,
            "Unauthorized."
        );

        let (generic_resource, shared_id, cached_rs) = tonic_internal!(
            self.database_handler.update_description(request).await,
            "Internal database error"
        );

        tonic_internal!(
            self.cache
                .cache
                .process_api_resource_update(generic_resource, shared_id, cached_rs),
            "Caching error"
        );

        let response = UpdateProjectDescriptionResponse {
            project: Some(generic_resource.into_inner()?),
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
        let id = tonic_invalid!(request.get_id(), "Invalid project id.");

        let ctx = Context::res_proj(Some((id, PermissionLevels::WRITE, true)));

        tonic_auth!(
            self.authorizer.check_context(&token, ctx).await,
            "Unauthorized."
        );

        let (generic_resource, shared_id, cached_rs) = tonic_internal!(
            self.database_handler.update_keyvals(request).await,
            "Internal database error"
        );

        tonic_internal!(
            self.cache
                .cache
                .process_api_resource_update(generic_resource, shared_id, cached_rs),
            "Caching error"
        );

        let response = UpdateProjectKeyValuesResponse {
            project: Some(generic_resource.into_inner()?),
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
        let id = tonic_invalid!(request.get_id(), "Invalid project id.");

        let ctx = Context::res_proj(Some((id, PermissionLevels::WRITE, true)));

        tonic_auth!(
            self.authorizer.check_context(&token, ctx).await,
            "Unauthorized."
        );

        let (generic_resource, shared_id, cached_rs) = tonic_internal!(
            self.database_handler.update_dataclass(request).await,
            "Internal database error"
        );

        tonic_internal!(
            self.cache
                .cache
                .process_api_resource_update(generic_resource, shared_id, cached_rs),
            "Caching error"
        );

        let response = UpdateProjectDataClassResponse {
            project: Some(generic_resource.into_inner()?),
        };

        return_with_log!(response);
    }
    async fn archive_project(
        &self,
        _request: Request<ArchiveProjectRequest>,
    ) -> Result<Response<ArchiveProjectResponse>> {
        todo!()
    }
}
