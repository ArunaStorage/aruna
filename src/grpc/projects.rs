use crate::middlelayer::create_request_types::CreateRequest;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::update_request_types::{
    DataClassUpdate, DescriptionUpdate, KeyValueUpdate, NameUpdate,
};
use aruna_cache::structs::Resource;
use aruna_rust_api::api::storage::models::v2::generic_resource;

use crate::utils::conversions::get_token_from_md;
use aruna_cache::notifications::NotificationCache;
use aruna_policy::ape::policy_evaluator::PolicyEvaluator;
use aruna_policy::ape::structs::PermissionLevels as PolicyLevels;
use aruna_policy::ape::structs::{ApeResourcePermission, Context, ResourceContext};
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
        log_received!(request);

        log_received!(request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = CreateRequest::Project(request.into_inner());
        let parent = request
            .get_parent()
            .ok_or(tonic::Status::invalid_argument("Parent missing."))?;

        let ctx = Context::ResourceContext(ResourceContext::Project(None));

        let user_id = tonic_auth!(
            &self.authorizer.check_context(&token, ctx).await,
            "Unauthorized."
        )
        .ok_or(tonic::Status::invalid_argument("User id missing."))?;

        let project = match tonic_internal!(
            self.database_handler
                .create_resource(request, user_id)
                .await,
            "Internal database error."
        ) {
            generic_resource::Resource::Project(p) => Some(p),
            _ => return Err(tonic::Status::unknown("This should not happen.")),
        };

        Ok(tonic::Response::new(CreateProjectResponse { project }))
    }

    async fn get_project(
        &self,
        request: Request<GetProjectRequest>,
    ) -> Result<Response<GetProjectResponse>> {
        log_received!(request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = request.into_inner();
        let id = tonic_invalid!(
            DieselUlid::from_str(&request.project_id),
            "Invalid dataset id."
        );
        let ctx = Context::ResourceContext(ResourceContext::Project(Some(ApeResourcePermission {
            id,
            level: PolicyLevels::READ,
            allow_sa: true,
        })));

        let project = match tonic_internal!(
            self.cache
                .cache
                .get_resource(&Resource::Project(id))
                .ok_or(tonic::Status::not_found("Collection not found.")),
            "Internal database error."
        ) {
            generic_resource::Resource::Project(p) => Some(p),
            _ => return Err(tonic::Status::unknown("This should not happen.")),
        };
        Ok(tonic::Response::new(GetProjectResponse { project }))
    }

    async fn update_project_name(
        &self,
        request: Request<UpdateProjectNameRequest>,
    ) -> Result<Response<UpdateProjectNameResponse>> {
        log_received!(request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = NameUpdate::Project(request.into_inner());
        let id = tonic_invalid!(request.get_id(), "Invalid project id.");

        let ctx = Context::ResourceContext(ResourceContext::Project(Some(ApeResourcePermission {
            id,
            level: PolicyLevels::WRITE,
            allow_sa: true,
        })));

        tonic_auth!(
            &self.authorizer.check_context(&token, ctx).await,
            "Unauthorized."
        );

        let project = match tonic_internal!(
            self.database_handler.update_name(request).await,
            "Internal database error."
        ) {
            generic_resource::Resource::Project(p) => Some(p),
            _ => return Err(tonic::Status::unknown("This should not happen.")),
        };

        Ok(tonic::Response::new(UpdateProjectNameResponse { project }))
    }
    async fn update_project_description(
        &self,
        request: Request<UpdateProjectDescriptionRequest>,
    ) -> Result<Response<UpdateProjectDescriptionResponse>> {
        log_received!(request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = DescriptionUpdate::Project(request.into_inner());
        let id = tonic_invalid!(request.get_id(), "Invalid project id.");

        let ctx = Context::ResourceContext(ResourceContext::Project(Some(ApeResourcePermission {
            id,
            level: PolicyLevels::WRITE,
            allow_sa: true,
        })));

        tonic_auth!(
            &self.authorizer.check_context(&token, ctx).await,
            "Unauthorized."
        );

        let project = match tonic_internal!(
            self.database_handler.update_description(request).await,
            "Internal database error."
        ) {
            generic_resource::Resource::Project(p) => Some(p),
            _ => return Err(tonic::Status::unknown("This should not happen.")),
        };

        Ok(tonic::Response::new(UpdateProjectDescriptionResponse {
            project,
        }))
    }
    async fn update_project_key_values(
        &self,
        request: Request<UpdateProjectKeyValuesRequest>,
    ) -> Result<Response<UpdateProjectKeyValuesResponse>> {
        log_received!(request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = KeyValueUpdate::Project(request.into_inner());
        let id = tonic_invalid!(request.get_id(), "Invalid project id.");

        let ctx = Context::ResourceContext(ResourceContext::Project(Some(ApeResourcePermission {
            id,
            level: PolicyLevels::WRITE,
            allow_sa: true,
        })));

        tonic_auth!(
            &self.authorizer.check_context(&token, ctx).await,
            "Unauthorized."
        );

        let project = match tonic_internal!(
            self.database_handler.update_keyvals(request).await,
            "Internal database error."
        ) {
            generic_resource::Resource::Project(p) => Some(p),
            _ => return Err(tonic::Status::unknown("This should not happen.")),
        };

        Ok(tonic::Response::new(UpdateProjectKeyValuesResponse {
            project,
        }))
    }
    async fn update_project_data_class(
        &self,
        request: Request<UpdateProjectDataClassRequest>,
    ) -> Result<Response<UpdateProjectDataClassResponse>> {
        log_received!(request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = DataClassUpdate::Project(request.into_inner());
        let id = tonic_invalid!(request.get_id(), "Invalid project id.");

        let ctx = Context::ResourceContext(ResourceContext::Project(Some(ApeResourcePermission {
            id,
            level: PolicyLevels::WRITE,
            allow_sa: true,
        })));

        tonic_auth!(
            &self.authorizer.check_context(&token, ctx).await,
            "Unauthorized."
        );

        let project = match tonic_internal!(
            self.database_handler.update_dataclass(request).await,
            "Internal database error."
        ) {
            generic_resource::Resource::Project(p) => Some(p),
            _ => return Err(tonic::Status::unknown("This should not happen.")),
        };

        Ok(tonic::Response::new(UpdateProjectDataClassResponse {
            project,
        }))
    }
    async fn get_projects(
        &self,
        _request: Request<GetProjectsRequest>,
    ) -> Result<Response<GetProjectsResponse>> {
        todo!()
    }
    async fn archive_project(
        &self,
        _request: Request<ArchiveProjectRequest>,
    ) -> Result<Response<ArchiveProjectResponse>> {
        todo!()
    }
    async fn delete_project(
        &self,
        _request: Request<DeleteProjectRequest>,
    ) -> Result<Response<DeleteProjectResponse>> {
        todo!()
    }
}
