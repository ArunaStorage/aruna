use crate::auth::permission_handler::PermissionHandler;
use crate::auth::structs::Context;
use crate::caching::cache::Cache;
use crate::database::enums::DbPermissionLevel;
use crate::middlelayer::create_request_types::CreateRequest;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::utils::conversions::get_token_from_md;
use crate::utils::grpc_utils::IntoGenericInner;
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
            self.authorizer.check_context(&token, vec![ctx]).await,
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

        let ctx = Context::res_ctx(project_id, DbPermissionLevel::READ, true);

        tonic_auth!(
            self.authorizer.check_context(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let res = self
            .cache
            .get_resource(&project_id)
            .ok_or_else(|| tonic::Status::not_found("Project not found"))?;

        let response = GetProjectResponse {
            project: Some(res.into_inner()?),
        };

        return_with_log!(response);
    }

    async fn update_project_name(
        &self,
        _request: Request<UpdateProjectNameRequest>,
    ) -> Result<Response<UpdateProjectNameResponse>> {
        todo!();
        // log_received!(&request);
        // let token = get_token_from_md(request.metadata()).map_err(|e| {
        //     log::debug!("{}", e);
        //     tonic::Status::unauthenticated("Token authentication error.")
        // })?;

        // let inner_request = request.into_inner();
        // let object_id = tonic_invalid!(
        //     DieselUlid::from_str(&inner_request.project_id),
        //     "ULID conversion error"
        // );
        // let ctx = Context::ResourceContext(ResourceContext::Project(Some(ApeResourcePermission {
        //     id: object_id,
        //     level: PolicyLevels::WRITE, // append?
        //     allow_sa: false,
        // })));

        // match &self.authorizer.check_permissions(&token, ctx) {
        //     Ok(b) => {
        //         if *b {
        //             // ToDo!
        //             // PLACEHOLDER!
        //             DieselUlid::generate()
        //         } else {
        //             return Err(tonic::Status::permission_denied("Not allowed."));
        //         }
        //     }
        //     Err(e) => {
        //         log::debug!("{}", e);
        //         return Err(tonic::Status::permission_denied("Not allowed."));
        //     }
        // };
        // let client = self.database.get_client().await.map_err(|e| {
        //     log::error!("{}", e);
        //     tonic::Status::unavailable("Database not avaliable.")
        // })?;
        // Object::update_name(object_id, inner_request.name, &client)
        //     .await
        //     .map_err(|e| {
        //         log::error!("{}", e);
        //         tonic::Status::aborted("Database update failed.")
        //     })?;
        // let object = Object::get_object_with_relations(&object_id, &client)
        //     .await
        //     .map_err(|e| {
        //         log::error!("{}", e);
        //         tonic::Status::aborted("Database update failed.")
        //     })?;
        // let project = Some(object.try_into().map_err(|e| {
        //     log::error!("{}", e);
        //     tonic::Status::aborted("Database request failed.")
        // })?);
        // Ok(tonic::Response::new(UpdateProjectNameResponse { project }))
    }
    async fn update_project_description(
        &self,
        _request: Request<UpdateProjectDescriptionRequest>,
    ) -> Result<Response<UpdateProjectDescriptionResponse>> {
        todo!();
        // log::info!("Recieved UpdateProjectDescriptionRequest.");
        // log::debug!("{:?}", &request);

        // let token = get_token_from_md(request.metadata()).map_err(|e| {
        //     log::debug!("{}", e);
        //     tonic::Status::unauthenticated("Token authentication error.")
        // })?;

        // let inner_request = request.into_inner();
        // let object_id = DieselUlid::from_str(&inner_request.project_id).map_err(|e| {
        //     log::error!("{}", e);
        //     tonic::Status::internal("ULID conversion error")
        // })?;
        // let ctx = Context::Project(Some(ResourcePermission {
        //     id: object_id,
        //     level: crate::database::enums::PermissionLevels::WRITE, // append?
        //     allow_sa: true,
        // }));

        // match &self.authorizer.check_permissions(&token, ctx) {
        //     Ok(b) => {
        //         if *b {
        //             // ToDo!
        //             // PLACEHOLDER!
        //             DieselUlid::generate()
        //         } else {
        //             return Err(tonic::Status::permission_denied("Not allowed."));
        //         }
        //     }
        //     Err(e) => {
        //         log::debug!("{}", e);
        //         return Err(tonic::Status::permission_denied("Not allowed."));
        //     }
        // };
        // let client = self.database.get_client().await.map_err(|e| {
        //     log::error!("{}", e);
        //     tonic::Status::unavailable("Database not avaliable.")
        // })?;
        // Object::update_description(object_id, inner_request.description, &client)
        //     .await
        //     .map_err(|e| {
        //         log::error!("{}", e);
        //         tonic::Status::aborted("Database update failed.")
        //     })?;
        // let object = Object::get_object_with_relations(&object_id, &client)
        //     .await
        //     .map_err(|e| {
        //         log::error!("{}", e);
        //         tonic::Status::aborted("Database update failed.")
        //     })?;
        // let project = Some(object.try_into().map_err(|e| {
        //     log::error!("{}", e);
        //     tonic::Status::aborted("Database request failed.")
        // })?);
        // Ok(tonic::Response::new(UpdateProjectDescriptionResponse {
        //     project,
        // }))
    }
    async fn update_project_key_values(
        &self,
        _request: Request<UpdateProjectKeyValuesRequest>,
    ) -> Result<Response<UpdateProjectKeyValuesResponse>> {
        todo!();
        // log::info!("Recieved UpdateProjectKeyValuesRequest.");
        // log::debug!("{:?}", &request);

        // let token = get_token_from_md(request.metadata()).map_err(|e| {
        //     log::debug!("{}", e);
        //     tonic::Status::unauthenticated("Token authentication error.")
        // })?;

        // let inner_request = request.into_inner();
        // let dataset_id = DieselUlid::from_str(&inner_request.project_id).map_err(|e| {
        //     log::error!("{}", e);
        //     tonic::Status::internal("ULID conversion error.")
        // })?;

        // let ctx = Context::Project(Some(ResourcePermission {
        //     id: dataset_id,
        //     level: crate::database::enums::PermissionLevels::WRITE, // append?
        //     allow_sa: true,
        // }));

        // match &self.authorizer.check_permissions(&token, ctx) {
        //     Ok(b) => {
        //         if *b {
        //             // ToDo!
        //             // PLACEHOLDER!
        //             DieselUlid::generate()
        //         } else {
        //             return Err(tonic::Status::permission_denied("Not allowed."));
        //         }
        //     }
        //     Err(e) => {
        //         log::debug!("{}", e);
        //         return Err(tonic::Status::permission_denied("Not allowed."));
        //     }
        // };
        // let mut client = self.database.get_client().await.map_err(|e| {
        //     log::error!("{}", e);
        //     tonic::Status::unavailable("Database not avaliable.")
        // })?;
        // let transaction = client.transaction().await.map_err(|e| {
        //     log::error!("{}", e);
        //     tonic::Status::unavailable("Database not avaliable.")
        // })?;

        // let client = transaction.client();

        // if !inner_request.add_key_values.is_empty() {
        //     let add_kv: KeyValues = inner_request.add_key_values.try_into().map_err(|e| {
        //         log::error!("{}", e);
        //         tonic::Status::internal("KeyValue conversion error.")
        //     })?;

        //     for kv in add_kv.0 {
        //         Object::add_key_value(&dataset_id, client, kv)
        //             .await
        //             .map_err(|e| {
        //                 log::error!("{}", e);
        //                 tonic::Status::aborted("Database transaction error.")
        //             })?;
        //     }
        // } else if !inner_request.remove_key_values.is_empty() {
        //     let rm_kv: KeyValues = inner_request.remove_key_values.try_into().map_err(|e| {
        //         log::error!("{}", e);
        //         tonic::Status::internal("KeyValue conversion error.")
        //     })?;
        //     let object = Object::get(dataset_id, client)
        //         .await
        //         .map_err(|e| {
        //             log::error!("{}", e);
        //             tonic::Status::aborted("Database transaction error.")
        //         })?
        //         .ok_or(tonic::Status::invalid_argument("Dataset does not exist."))?;
        //     for kv in rm_kv.0 {
        //         object.remove_key_value(client, kv).await.map_err(|e| {
        //             log::error!("{}", e);
        //             tonic::Status::aborted("Database transaction error.")
        //         })?;
        //     }
        // } else {
        //     return Err(tonic::Status::invalid_argument(
        //         "Both add_key_values and remove_key_values empty.",
        //     ));
        // }

        // let dataset_with_relations = Object::get_object_with_relations(&dataset_id, client)
        //     .await
        //     .map_err(|e| {
        //         log::error!("{}", e);
        //         tonic::Status::aborted("Database transaction error.")
        //     })?;
        // let project = Some(dataset_with_relations.try_into().map_err(|e| {
        //     log::error!("{}", e);
        //     tonic::Status::internal("Dataset conversion error.")
        // })?);

        // Ok(tonic::Response::new(UpdateProjectKeyValuesResponse {
        //     project,
        // }))
    }
    async fn update_project_data_class(
        &self,
        _request: Request<UpdateProjectDataClassRequest>,
    ) -> Result<Response<UpdateProjectDataClassResponse>> {
        todo!();
        // log::info!("Recieved UpdateProjectDataClassRequest.");
        // log::debug!("{:?}", &request);

        // let token = get_token_from_md(request.metadata()).map_err(|e| {
        //     log::debug!("{}", e);
        //     tonic::Status::unauthenticated("Token authentication error.")
        // })?;

        // let inner_request = request.into_inner();
        // let object_id = DieselUlid::from_str(&inner_request.project_id).map_err(|e| {
        //     log::error!("{}", e);
        //     tonic::Status::internal("ULID conversion error")
        // })?;
        // let ctx = Context::Project(Some(ResourcePermission {
        //     id: object_id,
        //     level: crate::database::enums::PermissionLevels::WRITE, // append?
        //     allow_sa: true,
        // }));

        // match &self.authorizer.check_permissions(&token, ctx) {
        //     Ok(b) => {
        //         if *b {
        //             // ToDo!
        //             // PLACEHOLDER!
        //             DieselUlid::generate()
        //         } else {
        //             return Err(tonic::Status::permission_denied("Not allowed."));
        //         }
        //     }
        //     Err(e) => {
        //         log::debug!("{}", e);
        //         return Err(tonic::Status::permission_denied("Not allowed."));
        //     }
        // };
        // let client = self.database.get_client().await.map_err(|e| {
        //     log::error!("{}", e);
        //     tonic::Status::unavailable("Database not avaliable.")
        // })?;

        // let dataclass = inner_request.data_class.try_into().map_err(|e| {
        //     log::error!("{}", e);
        //     tonic::Status::internal("DataClass conversion error.")
        // })?;
        // let old_class: i32 = Object::get(object_id, &client)
        //     .await
        //     .map_err(|e| {
        //         log::error!("{}", e);
        //         tonic::Status::internal("Database transaction failed.")
        //     })?
        //     .ok_or(tonic::Status::internal("Database transaction failed."))?
        //     .data_class
        //     .into();
        // if old_class > inner_request.data_class {
        //     return Err(tonic::Status::internal("Dataclass can only be relaxed."));
        // }
        // Object::update_dataclass(object_id, dataclass, &client)
        //     .await
        //     .map_err(|e| {
        //         log::error!("{}", e);
        //         tonic::Status::aborted("Database update failed.")
        //     })?;
        // let object = Object::get_object_with_relations(&object_id, &client)
        //     .await
        //     .map_err(|e| {
        //         log::error!("{}", e);
        //         tonic::Status::aborted("Database update failed.")
        //     })?;
        // let project = Some(object.try_into().map_err(|e| {
        //     log::error!("{}", e);
        //     tonic::Status::aborted("Database request failed.")
        // })?);
        // Ok(tonic::Response::new(UpdateProjectDataClassResponse {
        //     project,
        // }))
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
