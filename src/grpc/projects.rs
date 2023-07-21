use crate::auth::{Authorizer, Context, ResourcePermission};
use crate::caching::cache::Cache;
use crate::database::connection::Database;
use crate::database::crud::CrudDb;
use crate::database::dsls::object_dsl::{ExternalRelations, Hashes, KeyValues, Object};
use crate::utils::conversions::get_token_from_md;

use aruna_rust_api::api::storage::models::v2::{Project, Stats};
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
use postgres_types::Json;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;
use tonic::{Request, Response, Result};

crate::impl_grpc_server!(ProjectServiceImpl);

#[tonic::async_trait]
impl ProjectService for ProjectServiceImpl {
    async fn create_project(
        &self,
        request: Request<CreateProjectRequest>,
    ) -> Result<Response<CreateProjectResponse>> {
        log::info!("Recieved CreateProjectRequest.");
        log::debug!("{:?}", &request);

        let token = get_token_from_md(request.metadata()).map_err(|e| {
            log::debug!("{}", e);
            tonic::Status::unauthenticated("Token authentication error.")
        })?;

        let inner_request = request.into_inner();

        let ctx = Context::Project(None);

        let user_id = match &self.authorizer.check_permissions(&token, ctx) {
            Ok(b) => {
                if *b {
                    // ToDo!
                    // PLACEHOLDER!
                    DieselUlid::generate()
                } else {
                    return Err(tonic::Status::permission_denied("Not allowed."));
                }
            }
            Err(e) => {
                log::debug!("{}", e);
                return Err(tonic::Status::permission_denied("Not allowed."));
            }
        };

        let id = DieselUlid::generate();
        let shared_id = DieselUlid::generate();

        let key_values: KeyValues = inner_request.key_values.try_into().map_err(|e| {
            log::error!("{}", e);
            tonic::Status::internal("KeyValue conversion error.")
        })?;

        let external_relations: ExternalRelations =
            inner_request.external_relations.try_into().map_err(|e| {
                log::error!("{}", e);
                tonic::Status::internal("ExternalRelation conversion error.")
            })?;

        let create_object = Object {
            id,
            shared_id,
            revision_number: 0,
            name: inner_request.name,
            description: inner_request.description,
            created_at: None,
            content_len: 0,
            created_by: user_id,
            count: 0,
            key_values: Json(key_values.clone()),
            object_status: crate::database::enums::ObjectStatus::AVAILABLE,
            data_class: inner_request.data_class.try_into().map_err(|e| {
                log::error!("{}", e);
                tonic::Status::internal("DataClass conversion error.")
            })?,
            object_type: crate::database::enums::ObjectType::PROJECT,
            external_relations: Json(external_relations.clone()),
            hashes: Json(Hashes(Vec::new())),
        };

        let client = self.database.get_client().await.map_err(|e| {
            log::error!("{}", e);
            tonic::Status::unavailable("Database not avaliable.")
        })?;
        create_object.create(&client).await.map_err(|e| {
            log::error!("{}", e);
            tonic::Status::aborted("Database transaction failed.")
        })?;

        let stats = Some(Stats {
            count: 0,
            size: 0,
            last_updated: None, //TODO
        });
        let grpc_project = Project {
            id: create_object.id.to_string(),
            name: create_object.name,
            description: create_object.description,
            key_values: key_values.into(),
            relations: Vec::new(),
            data_class: create_object.data_class.into(),
            created_at: None, // TODO
            created_by: user_id.to_string(),
            status: create_object.object_status.into(),
            dynamic: true,
            stats,
        };
        Ok(tonic::Response::new(CreateProjectResponse {
            project: Some(grpc_project),
        }))
    }
    async fn get_project(
        &self,
        request: Request<GetProjectRequest>,
    ) -> Result<Response<GetProjectResponse>> {
        log::info!("Recieved GetCollectionRequest.");
        log::debug!("{:?}", &request);

        let token = get_token_from_md(request.metadata()).map_err(|e| {
            log::debug!("{}", e);
            tonic::Status::unauthenticated("Token authentication error.")
        })?;

        let inner_request = request.into_inner();
        let object_id = DieselUlid::from_str(&inner_request.project_id).map_err(|e| {
            log::error!("{}", e);
            tonic::Status::internal("ULID conversion error")
        })?;
        let ctx = Context::Project(Some(ResourcePermission {
            id: object_id,
            level: crate::database::enums::PermissionLevels::READ, // append?
            allow_sa: true,
        }));

        match &self.authorizer.check_permissions(&token, ctx) {
            Ok(b) => {
                if *b {
                    // ToDo!
                    // PLACEHOLDER!
                    DieselUlid::generate()
                } else {
                    return Err(tonic::Status::permission_denied("Not allowed."));
                }
            }
            Err(e) => {
                log::debug!("{}", e);
                return Err(tonic::Status::permission_denied("Not allowed."));
            }
        };
        let mut client = self.database.get_client().await.map_err(|e| {
            log::error!("{}", e);
            tonic::Status::unavailable("Database not avaliable.")
        })?;
        let transaction = client.transaction().await.map_err(|e| {
            log::error!("{}", e);
            tonic::Status::unavailable("Database not avaliable.")
        })?;

        let client = transaction.client();
        let get_object = Object::get_object_with_relations(&object_id, client)
            .await
            .map_err(|e| {
                log::error!("{}", e);
                tonic::Status::aborted("Database read error.")
            })?;

        let project = Some(get_object.try_into().map_err(|e| {
            log::error!("{}", e);
            tonic::Status::internal("ObjectFromRelations conversion failed.")
        })?);

        Ok(tonic::Response::new(GetProjectResponse { project }))
    }

    async fn update_project_name(
        &self,
        request: Request<UpdateProjectNameRequest>,
    ) -> Result<Response<UpdateProjectNameResponse>> {
        log::info!("Recieved UpdateProjectNameRequest.");
        log::debug!("{:?}", &request);

        let token = get_token_from_md(request.metadata()).map_err(|e| {
            log::debug!("{}", e);
            tonic::Status::unauthenticated("Token authentication error.")
        })?;

        let inner_request = request.into_inner();
        let object_id = DieselUlid::from_str(&inner_request.project_id).map_err(|e| {
            log::error!("{}", e);
            tonic::Status::internal("ULID conversion error")
        })?;
        let ctx = Context::Project(Some(ResourcePermission {
            id: object_id,
            level: crate::database::enums::PermissionLevels::WRITE, // append?
            allow_sa: false,
        }));

        match &self.authorizer.check_permissions(&token, ctx) {
            Ok(b) => {
                if *b {
                    // ToDo!
                    // PLACEHOLDER!
                    DieselUlid::generate()
                } else {
                    return Err(tonic::Status::permission_denied("Not allowed."));
                }
            }
            Err(e) => {
                log::debug!("{}", e);
                return Err(tonic::Status::permission_denied("Not allowed."));
            }
        };
        let client = self.database.get_client().await.map_err(|e| {
            log::error!("{}", e);
            tonic::Status::unavailable("Database not avaliable.")
        })?;
        Object::update_name(object_id, inner_request.name, &client)
            .await
            .map_err(|e| {
                log::error!("{}", e);
                tonic::Status::aborted("Database update failed.")
            })?;
        let object = Object::get_object_with_relations(&object_id, &client)
            .await
            .map_err(|e| {
                log::error!("{}", e);
                tonic::Status::aborted("Database update failed.")
            })?;
        let project = Some(object.try_into().map_err(|e| {
            log::error!("{}", e);
            tonic::Status::aborted("Database request failed.")
        })?);
        Ok(tonic::Response::new(UpdateProjectNameResponse { project }))
    }
    async fn update_project_description(
        &self,
        request: Request<UpdateProjectDescriptionRequest>,
    ) -> Result<Response<UpdateProjectDescriptionResponse>> {
        log::info!("Recieved UpdateProjectDescriptionRequest.");
        log::debug!("{:?}", &request);

        let token = get_token_from_md(request.metadata()).map_err(|e| {
            log::debug!("{}", e);
            tonic::Status::unauthenticated("Token authentication error.")
        })?;

        let inner_request = request.into_inner();
        let object_id = DieselUlid::from_str(&inner_request.project_id).map_err(|e| {
            log::error!("{}", e);
            tonic::Status::internal("ULID conversion error")
        })?;
        let ctx = Context::Project(Some(ResourcePermission {
            id: object_id,
            level: crate::database::enums::PermissionLevels::WRITE, // append?
            allow_sa: true,
        }));

        match &self.authorizer.check_permissions(&token, ctx) {
            Ok(b) => {
                if *b {
                    // ToDo!
                    // PLACEHOLDER!
                    DieselUlid::generate()
                } else {
                    return Err(tonic::Status::permission_denied("Not allowed."));
                }
            }
            Err(e) => {
                log::debug!("{}", e);
                return Err(tonic::Status::permission_denied("Not allowed."));
            }
        };
        let client = self.database.get_client().await.map_err(|e| {
            log::error!("{}", e);
            tonic::Status::unavailable("Database not avaliable.")
        })?;
        Object::update_description(object_id, inner_request.description, &client)
            .await
            .map_err(|e| {
                log::error!("{}", e);
                tonic::Status::aborted("Database update failed.")
            })?;
        let object = Object::get_object_with_relations(&object_id, &client)
            .await
            .map_err(|e| {
                log::error!("{}", e);
                tonic::Status::aborted("Database update failed.")
            })?;
        let project = Some(object.try_into().map_err(|e| {
            log::error!("{}", e);
            tonic::Status::aborted("Database request failed.")
        })?);
        Ok(tonic::Response::new(UpdateProjectDescriptionResponse {
            project,
        }))
    }
    async fn update_project_key_values(
        &self,
        request: Request<UpdateProjectKeyValuesRequest>,
    ) -> Result<Response<UpdateProjectKeyValuesResponse>> {
        log::info!("Recieved UpdateProjectKeyValuesRequest.");
        log::debug!("{:?}", &request);

        let token = get_token_from_md(request.metadata()).map_err(|e| {
            log::debug!("{}", e);
            tonic::Status::unauthenticated("Token authentication error.")
        })?;

        let inner_request = request.into_inner();
        let dataset_id = DieselUlid::from_str(&inner_request.project_id).map_err(|e| {
            log::error!("{}", e);
            tonic::Status::internal("ULID conversion error.")
        })?;

        let ctx = Context::Project(Some(ResourcePermission {
            id: dataset_id,
            level: crate::database::enums::PermissionLevels::WRITE, // append?
            allow_sa: true,
        }));

        match &self.authorizer.check_permissions(&token, ctx) {
            Ok(b) => {
                if *b {
                    // ToDo!
                    // PLACEHOLDER!
                    DieselUlid::generate()
                } else {
                    return Err(tonic::Status::permission_denied("Not allowed."));
                }
            }
            Err(e) => {
                log::debug!("{}", e);
                return Err(tonic::Status::permission_denied("Not allowed."));
            }
        };
        let mut client = self.database.get_client().await.map_err(|e| {
            log::error!("{}", e);
            tonic::Status::unavailable("Database not avaliable.")
        })?;
        let transaction = client.transaction().await.map_err(|e| {
            log::error!("{}", e);
            tonic::Status::unavailable("Database not avaliable.")
        })?;

        let client = transaction.client();

        if !inner_request.add_key_values.is_empty() {
            let add_kv: KeyValues = inner_request.add_key_values.try_into().map_err(|e| {
                log::error!("{}", e);
                tonic::Status::internal("KeyValue conversion error.")
            })?;

            for kv in add_kv.0 {
                Object::add_key_value(&dataset_id, client, kv)
                    .await
                    .map_err(|e| {
                        log::error!("{}", e);
                        tonic::Status::aborted("Database transaction error.")
                    })?;
            }
        } else if !inner_request.remove_key_values.is_empty() {
            let rm_kv: KeyValues = inner_request.remove_key_values.try_into().map_err(|e| {
                log::error!("{}", e);
                tonic::Status::internal("KeyValue conversion error.")
            })?;
            let object = Object::get(dataset_id, client)
                .await
                .map_err(|e| {
                    log::error!("{}", e);
                    tonic::Status::aborted("Database transaction error.")
                })?
                .ok_or(tonic::Status::invalid_argument("Dataset does not exist."))?;
            for kv in rm_kv.0 {
                object.remove_key_value(client, kv).await.map_err(|e| {
                    log::error!("{}", e);
                    tonic::Status::aborted("Database transaction error.")
                })?;
            }
        } else {
            return Err(tonic::Status::invalid_argument(
                "Both add_key_values and remove_key_values empty.",
            ));
        }

        let dataset_with_relations = Object::get_object_with_relations(&dataset_id, client)
            .await
            .map_err(|e| {
                log::error!("{}", e);
                tonic::Status::aborted("Database transaction error.")
            })?;
        let project = Some(dataset_with_relations.try_into().map_err(|e| {
            log::error!("{}", e);
            tonic::Status::internal("Dataset conversion error.")
        })?);

        Ok(tonic::Response::new(UpdateProjectKeyValuesResponse {
            project,
        }))
    }
    async fn update_project_data_class(
        &self,
        request: Request<UpdateProjectDataClassRequest>,
    ) -> Result<Response<UpdateProjectDataClassResponse>> {
        log::info!("Recieved UpdateProjectDataClassRequest.");
        log::debug!("{:?}", &request);

        let token = get_token_from_md(request.metadata()).map_err(|e| {
            log::debug!("{}", e);
            tonic::Status::unauthenticated("Token authentication error.")
        })?;

        let inner_request = request.into_inner();
        let object_id = DieselUlid::from_str(&inner_request.project_id).map_err(|e| {
            log::error!("{}", e);
            tonic::Status::internal("ULID conversion error")
        })?;
        let ctx = Context::Project(Some(ResourcePermission {
            id: object_id,
            level: crate::database::enums::PermissionLevels::WRITE, // append?
            allow_sa: true,
        }));

        match &self.authorizer.check_permissions(&token, ctx) {
            Ok(b) => {
                if *b {
                    // ToDo!
                    // PLACEHOLDER!
                    DieselUlid::generate()
                } else {
                    return Err(tonic::Status::permission_denied("Not allowed."));
                }
            }
            Err(e) => {
                log::debug!("{}", e);
                return Err(tonic::Status::permission_denied("Not allowed."));
            }
        };
        let client = self.database.get_client().await.map_err(|e| {
            log::error!("{}", e);
            tonic::Status::unavailable("Database not avaliable.")
        })?;

        let dataclass = inner_request.data_class.try_into().map_err(|e| {
            log::error!("{}", e);
            tonic::Status::internal("DataClass conversion error.")
        })?;
        let old_class: i32 = Object::get(object_id, &client)
            .await
            .map_err(|e| {
                log::error!("{}", e);
                tonic::Status::internal("Database transaction failed.")
            })?
            .ok_or(tonic::Status::internal("Database transaction failed."))?
            .data_class
            .into();
        if old_class > inner_request.data_class {
            return Err(tonic::Status::internal("Dataclass can only be relaxed."));
        }
        Object::update_dataclass(object_id, dataclass, &client)
            .await
            .map_err(|e| {
                log::error!("{}", e);
                tonic::Status::aborted("Database update failed.")
            })?;
        let object = Object::get_object_with_relations(&object_id, &client)
            .await
            .map_err(|e| {
                log::error!("{}", e);
                tonic::Status::aborted("Database update failed.")
            })?;
        let project = Some(object.try_into().map_err(|e| {
            log::error!("{}", e);
            tonic::Status::aborted("Database request failed.")
        })?);
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
