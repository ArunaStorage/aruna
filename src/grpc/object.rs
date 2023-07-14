use crate::auth::{Authorizer, Context, ResourcePermission};
use crate::caching::cache::Cache;
use crate::database::connection::Database;
use crate::database::crud::CrudDb;
use crate::database::internal_relation_dsl::InternalRelation;
use crate::database::object_dsl::{ExternalRelations, Hashes, KeyValues, Object};
use crate::utils::conversions::get_token_from_md;
use aruna_rust_api::api::storage::models::v2::{
    relation::Relation as RelationEnum, Object as GRPCObject, Relation,
};
use aruna_rust_api::api::storage::services::v2::create_object_request::Parent;
use aruna_rust_api::api::storage::services::v2::object_service_server::ObjectService;
use aruna_rust_api::api::storage::services::v2::{
    CloneObjectRequest, CloneObjectResponse, CreateObjectRequest, CreateObjectResponse,
    DeleteObjectRequest, DeleteObjectResponse, FinishObjectStagingRequest,
    FinishObjectStagingResponse, GetDownloadUrlRequest, GetDownloadUrlResponse, GetObjectRequest,
    GetObjectResponse, GetObjectsRequest, GetObjectsResponse, GetUploadUrlRequest,
    GetUploadUrlResponse, UpdateObjectRequest, UpdateObjectResponse,
};
use diesel_ulid::DieselUlid;
use postgres_types::Json;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;
use tonic::{Request, Response, Result};

crate::impl_grpc_server!(ObjectServiceImpl);

#[tonic::async_trait]
impl ObjectService for ObjectServiceImpl {
    async fn create_object(
        &self,
        request: Request<CreateObjectRequest>,
    ) -> Result<Response<CreateObjectResponse>> {
        log::info!("Recieved CreateObjectRequest.");
        log::debug!("{:?}", &request);

        let token = get_token_from_md(request.metadata()).map_err(|e| {
            log::debug!("{}", e);
            tonic::Status::unauthenticated("Token authentication error.")
        })?;

        let inner_request = request.into_inner();
        let parent_id = match inner_request.parent {
            Some(parent) => {
                let id = match parent {
                    Parent::ProjectId(id) => id,
                    Parent::CollectionId(id) => id,
                    Parent::DatasetId(id) => id,
                };
                DieselUlid::from_str(&id).map_err(|e| {
                    log::debug!("{}", e);
                    tonic::Status::internal("ULID parsing error")
                })?
            }
            None => return Err(tonic::Status::invalid_argument("Object has no parent")),
        };

        let ctx = Context::Object(ResourcePermission {
            id: parent_id,
            level: crate::database::enums::PermissionLevels::WRITE, // append?
            allow_sa: true,
        });

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

        let hashes: Hashes = inner_request.hashes.try_into().map_err(|e| {
            log::error!("{}", e);
            tonic::Status::internal("Hash conversion error.")
        })?;

        let create_object = Object {
            id,
            shared_id,
            revision_number: 0,
            name: inner_request.name,
            description: inner_request.description,
            created_at: None,
            content_len: 0, // gets updated when finish_object_staging gets called
            created_by: user_id,
            count: 1, // Objects always have count 1,
            key_values: Json(key_values.clone()),
            object_status: crate::database::enums::ObjectStatus::INITIALIZING,
            // ToDo!
            data_class: inner_request.data_class.try_into().map_err(|e| {
                log::error!("{}", e);
                tonic::Status::internal("DataClass conversion error.")
            })?,
            object_type: crate::database::enums::ObjectType::OBJECT,
            external_relations: Json(external_relations.clone()),
            hashes: Json(hashes.clone()),
        };

        let mut client = self.database.get_client().await.map_err(|e| {
            log::error!("{}", e);
            tonic::Status::unavailable("Database not avaliable.")
        })?;

        let transaction = client.transaction().await.map_err(|e| {
            log::error!("{}", e);
            tonic::Status::unavailable("Database not avaliable.")
        })?;

        let transaction_client = transaction.client();

        let create_relation = InternalRelation {
            id: DieselUlid::generate(),
            origin_pid: parent_id,
            is_persistent: false,
            target_pid: create_object.id,
            type_id: 1,
        };

        create_relation
            .create(&transaction_client)
            .await
            .map_err(|e| {
                log::error!("{}", e);
                tonic::Status::aborted("Database transaction failed.")
            })?;
        create_object
            .create(&transaction_client)
            .await
            .map_err(|e| {
                log::error!("{}", e);
                tonic::Status::aborted("Database transaction failed.")
            })?;

        // Needs mut for internal relation push
        let mut relations: Vec<Relation> = external_relations
            .0
            .into_iter()
            .map(|r| Relation {
                relation: Some(RelationEnum::External(r.into())),
            })
            .collect();
        // TODO!
        // relations.push(Relation {
        //     relation: Some(RelationEnum::Internal(create_relation.into())),
        // });
        let grpc_object = GRPCObject {
            id: create_object.id.to_string(),
            name: create_object.name,
            description: create_object.description,
            key_values: key_values.into(),
            relations,
            content_len: create_object.content_len,
            data_class: create_object.data_class.into(),
            created_at: None, // TODO
            created_by: user_id.to_string(),
            status: create_object.object_status.into(),
            dynamic: false,
            hashes: hashes.into(),
        };
        Ok(tonic::Response::new(CreateObjectResponse {
            object: Some(grpc_object),
        }))
    }
    async fn get_upload_url(
        &self,
        request: Request<GetUploadUrlRequest>,
    ) -> Result<Response<GetUploadUrlResponse>> {
        Err(tonic::Status::unimplemented(
            "GetUploadURL is not yet implemented.",
        ))
    }
    async fn get_download_url(
        &self,
        request: Request<GetDownloadUrlRequest>,
    ) -> Result<Response<GetDownloadUrlResponse>> {
        Err(tonic::Status::unimplemented(
            "GetDownloadURL is not yet implemented.",
        ))
    }
    async fn finish_object_staging(
        &self,
        request: Request<FinishObjectStagingRequest>,
    ) -> Result<Response<FinishObjectStagingResponse>> {
        log::info!("Recieved CreateObjectRequest.");
        log::debug!("{:?}", &request);

        let token = get_token_from_md(request.metadata()).map_err(|e| {
            log::debug!("{}", e);
            tonic::Status::unauthenticated("Token authentication error.")
        })?;

        let inner_request = request.into_inner();

        let ctx = Context::Object(ResourcePermission {
            id: DieselUlid::from_str(&inner_request.object_id).map_err(|e| {
                log::error!("{}", e);
                tonic::Status::internal("ULID conversion error")
            })?,
            level: crate::database::enums::PermissionLevels::WRITE, // append?
            allow_sa: true,
        });
        let _user_id = self
            .authorizer
            .check_permissions(&token, ctx)
            .map_err(|e| {
                log::error!("{}", e);
                tonic::Status::permission_denied("Permission denied.")
            })?;

        let client = self.database.get_client().await.map_err(|e| {
            log::error!("{}", e);
            tonic::Status::unavailable("Database not avaliable.")
        })?;
        let object_pid = DieselUlid::from_str(&inner_request.object_id).map_err(|e| {
            log::error!("{}", e);
            tonic::Status::internal("ULID conversion error.")
        })?;
        let to_update_object = match Object::get(object_pid, &client).await.map_err(|e| {
            log::error!("{}", e);
            tonic::Status::aborted("Database call failed.")
        })? {
            Some(o) => o,
            None => return Err(tonic::Status::aborted("Database call failed.")),
        };

        let hashes = if !inner_request.hashes.is_empty() {
            let req_hashes: Hashes = inner_request.hashes.try_into().map_err(|e| {
                log::error!("{}", e);
                tonic::Status::internal("Hash conversion error.")
            })?;
            if !to_update_object.hashes.0 .0.is_empty() {
                let comp_hashes = to_update_object.hashes.0;

                if comp_hashes != req_hashes {
                    return Err(tonic::Status::internal("Hashes diverge."));
                }
                None
            } else {
                Some(req_hashes)
            }
        } else {
            None
        };

        if !inner_request.completed_parts.is_empty() {
            return Err(tonic::Status::unimplemented(
                "Finish multipart objects is not yet implemented.",
            ));
        }

        Object::finish_object_staging(
            &object_pid,
            &client,
            hashes,
            inner_request.content_len,
            crate::database::enums::ObjectStatus::AVAILABLE,
        )
        .await
        .map_err(|e| {
            log::error!("{}", e);
            tonic::Status::aborted("Database update failed.")
        })?;

        todo!()
        // let grpc_object = GRPCObject {
        //     id: to_update_object.id.to_string(),
        //     name: to_update_object.name,
        //     description: to_update_object.description,
        //     key_values: to_update_object.key_values.into(),
        //     relations: to_update_object.external_relations.into(),
        //     content_len: create_object.content_len,
        //     data_class: create_object.data_class.into(),
        //     created_at: None, // TODO
        //     created_by: user_id.to_string(),
        //     status: create_object.object_status.into(),
        //     dynamic: false,
        //     hashes: hashes.into(),
        // };
        // Ok(tonic::Response::new(FinishObjectStagingResponse {
        //     object: Some(grpc_object),
        // }))
    }
    async fn update_object(
        &self,
        request: Request<UpdateObjectRequest>,
    ) -> Result<Response<UpdateObjectResponse>> {
        todo!()
    }
    async fn clone_object(
        &self,
        request: Request<CloneObjectRequest>,
    ) -> Result<Response<CloneObjectResponse>> {
        todo!()
    }
    async fn delete_object(
        &self,
        request: Request<DeleteObjectRequest>,
    ) -> Result<Response<DeleteObjectResponse>> {
        todo!()
    }
    async fn get_object(
        &self,
        request: Request<GetObjectRequest>,
    ) -> Result<Response<GetObjectResponse>> {
        todo!()
    }
    async fn get_objects(
        &self,
        request: Request<GetObjectsRequest>,
    ) -> Result<Response<GetObjectsResponse>> {
        todo!()
    }
}
