use crate::auth::permission_handler::PermissionHandler;
use crate::auth::structs::Context;
use crate::caching::cache::Cache;
use crate::database::enums::DbPermissionLevel;
use crate::middlelayer::create_request_types::CreateRequest;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::update_request_types::{
    DataClassUpdate, DescriptionUpdate, KeyValueUpdate, NameUpdate,
};
use crate::utils::conversions::get_token_from_md;
use crate::utils::grpc_utils::IntoGenericInner;
use aruna_rust_api::api::storage::models::v2::generic_resource;
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

        let ctx = Context::default();

        let user_id = tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]),
            "Unauthorized"
        )
        .ok_or(tonic::Status::invalid_argument("Missing user id"))?;

        let object_with_rel = tonic_internal!(
            self.database_handler
                .create_resource(request, user_id)
                .await,
            "Internal database error"
        );

        self.cache.add_object(object_with_rel.clone());

        let generic_project: generic_resource::Resource =
            tonic_invalid!(object_with_rel.try_into(), "Invalid project");

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

        let ctx = Context::res_ctx(project_id, DbPermissionLevel::READ, true);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]),
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
        let collection_id = tonic_invalid!(request.get_id(), "Invalid project id");
        let ctx = Context::res_ctx(collection_id, DbPermissionLevel::WRITE, true);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]),
            "Unauthorized"
        );

        let project = tonic_internal!(
            self.database_handler.update_name(request).await,
            "Internal database error."
        );
        self.cache
            .update_object(&project.object.id, project.clone());
        let project: generic_resource::Resource =
            tonic_internal!(project.try_into(), "Collection conversion error");

        Ok(Response::new(UpdateProjectNameResponse {
            project: Some(project.into_inner()?),
        }))
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
        let collection_id = tonic_invalid!(request.get_id(), "Invalid project id");
        let ctx = Context::res_ctx(collection_id, DbPermissionLevel::WRITE, true);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]),
            "Unauthorized"
        );

        let project = tonic_internal!(
            self.database_handler.update_description(request).await,
            "Internal database error."
        );
        self.cache
            .update_object(&project.object.id, project.clone());
        let project: generic_resource::Resource =
            tonic_internal!(project.try_into(), "Collection conversion error");

        Ok(Response::new(UpdateProjectDescriptionResponse {
            project: Some(project.into_inner()?),
        }))
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
        let collection_id = tonic_invalid!(request.get_id(), "Invalid project id");
        let ctx = Context::res_ctx(collection_id, DbPermissionLevel::WRITE, true);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]),
            "Unauthorized"
        );

        let project = tonic_internal!(
            self.database_handler.update_keyvals(request).await,
            "Internal database error."
        );
        self.cache
            .update_object(&project.object.id, project.clone());
        let project: generic_resource::Resource =
            tonic_internal!(project.try_into(), "Collection conversion error");

        Ok(Response::new(UpdateProjectKeyValuesResponse {
            project: Some(project.into_inner()?),
        }))
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
        let collection_id = tonic_invalid!(request.get_id(), "Invalid project id");
        let ctx = Context::res_ctx(collection_id, DbPermissionLevel::WRITE, true);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]),
            "Unauthorized"
        );

        let project = tonic_internal!(
            self.database_handler.update_dataclass(request).await,
            "Internal database error."
        );
        self.cache
            .update_object(&project.object.id, project.clone());
        let project: generic_resource::Resource =
            tonic_internal!(project.try_into(), "Collection conversion error");

        Ok(Response::new(UpdateProjectDataClassResponse {
            project: Some(project.into_inner()?),
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
