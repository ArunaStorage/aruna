use crate::auth::{Authorizer, Context, ResourcePermission};
use crate::caching::cache::Cache;
use crate::database::connection::Database;
use crate::database::crud::CrudDb;
use crate::database::enums::ObjectType;
use crate::database::internal_relation_dsl::InternalRelation;
use crate::database::object_dsl::{
    ExternalRelations, Hashes, KeyValue as DBKeyValue, KeyValues, Object,
};
use crate::utils::conversions::get_token_from_md;
use aruna_rust_api::api::storage::models::v2::{
    relation::Relation as RelationEnum, InternalRelation as APIInternalRelation,
    Object as GRPCObject, Relation,
};
use aruna_rust_api::api::storage::services::v2::create_object_request::Parent as CreateParent;
use aruna_rust_api::api::storage::services::v2::object_service_server::ObjectService;
use aruna_rust_api::api::storage::services::v2::update_object_request::Parent as UpdateParent;
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
        let (parent_id, variant) = match inner_request.parent {
            Some(parent) => {
                // TODO: Parent validation needed
                let (id, var) = match parent {
                    CreateParent::ProjectId(id) => (id, 1),
                    CreateParent::CollectionId(id) => (id, 2),
                    CreateParent::DatasetId(id) => (id, 3),
                };
                (
                    DieselUlid::from_str(&id).map_err(|e| {
                        log::debug!("{}", e);
                        tonic::Status::internal("ULID parsing error")
                    })?,
                    var,
                )
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

        let parent_relation = Some(RelationEnum::Internal(APIInternalRelation {
            resource_id: create_object.id.to_string(),
            resource_variant: variant,
            direction: 2,
            variant:
                Some(aruna_rust_api::api::storage::models::v2::internal_relation::Variant::DefinedVariant(
                    1,
                )),
        }));

        let mut relations: Vec<Relation> = external_relations
            .0
            .into_iter()
            .map(|r| Relation {
                relation: Some(RelationEnum::External(r.into())),
            })
            .collect();
        relations.push(Relation {
            relation: parent_relation,
        });
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
        let user_id = self
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
                let comp_hashes = to_update_object.hashes.0.clone();

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
            hashes.clone(),
            inner_request.content_len,
            crate::database::enums::ObjectStatus::AVAILABLE,
        )
        .await
        .map_err(|e| {
            log::error!("{}", e);
            tonic::Status::aborted("Database update failed.")
        })?;

        let grpc_object = GRPCObject {
            id: to_update_object.id.to_string(),
            name: to_update_object.name,
            description: to_update_object.description,
            key_values: to_update_object.key_values.0.into(),
            relations: to_update_object
                .external_relations
                .0
                 .0
                .into_iter()
                .map(|r| Relation {
                    relation: Some(RelationEnum::External(r.into())),
                })
                .collect(),
            content_len: inner_request.content_len,
            data_class: to_update_object.data_class.into(),
            created_at: None, // TODO
            created_by: user_id.to_string(),
            status: 3,
            dynamic: false,
            hashes: match hashes {
                Some(h) => h.into(),
                None => to_update_object.hashes.0.into(),
            },
        };
        Ok(tonic::Response::new(FinishObjectStagingResponse {
            object: Some(grpc_object),
        }))
    }
    async fn update_object(
        &self,
        request: Request<UpdateObjectRequest>,
    ) -> Result<Response<UpdateObjectResponse>> {
        log::info!("Recieved CreateObjectRequest.");
        log::debug!("{:?}", &request);

        let token = get_token_from_md(request.metadata()).map_err(|e| {
            log::debug!("{}", e);
            tonic::Status::unauthenticated("Token authentication error.")
        })?;

        let inner_request = request.into_inner();

        let ctx = Context::Object(ResourcePermission {
            // Is the parent_id relevant here?
            id: DieselUlid::from_str(&inner_request.object_id).map_err(|e| {
                log::error!("{}", e);
                tonic::Status::internal("ULID conversion error")
            })?,
            level: crate::database::enums::PermissionLevels::WRITE, // append?
            allow_sa: true,
        });

        let user_id = match &self.authorizer.check_permissions(&token, ctx) {
            Ok(b) => {
                if *b {
                    // TODO!
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

        let old_object_pid = DieselUlid::from_str(&inner_request.object_id).map_err(|e| {
            log::error!("{}", e);
            tonic::Status::internal("ULID conversion error.")
        })?;
        let old_object = match Object::get(old_object_pid, &client).await.map_err(|e| {
            log::error!("{}", e);
            tonic::Status::aborted("Database call failed.")
        })? {
            Some(o) => o,
            None => return Err(tonic::Status::aborted("Database call failed.")),
        };

        let data_class = match inner_request.data_class {
            0 => Ok(old_object.data_class),
            1..=5 => inner_request.data_class.try_into(),
            _ => return Err(tonic::Status::internal("Invalid dataclass.")),
        }
        .map_err(|e| {
            log::error!("{}", e);
            tonic::Status::internal("Invalid dataclass.")
        })?;

        let remove_kv: KeyValues = inner_request.remove_key_values.try_into().map_err(|e| {
            log::error!("{}", e);
            tonic::Status::internal("KeyValue conversion error.")
        })?;
        let mut add_kv: KeyValues = inner_request.add_key_values.try_into().map_err(|e| {
            log::error!("{}", e);
            tonic::Status::internal("KeyValue conversion error.")
        })?;
        let mut key_values: Vec<DBKeyValue> = old_object
            .key_values
            .0
             .0
            .into_iter()
            .filter(|l| !remove_kv.0.contains(l))
            .collect();
        key_values.append(&mut add_kv.0);
        let transaction = client.transaction().await.map_err(|e| {
            log::error!("{}", e);
            tonic::Status::unavailable("Database not avaliable.")
        })?;

        let new_version_object_id = DieselUlid::generate();

        let transaction_client = transaction.client();
        let parent_relation = match inner_request.parent {
            Some(p) => {
                // TODO: Parent validation needed!
                let p = match p {
                    UpdateParent::ProjectId(p) => p,
                    UpdateParent::DatasetId(p) => p,
                    UpdateParent::CollectionId(p) => p,
                };
                let parent = DieselUlid::from_str(&p).map_err(|e| {
                    log::error!("{}", e);
                    tonic::Status::internal("ULID conversion error.")
                })?;

                let ctx = Context::Object(ResourcePermission {
                    // Is the parent_id relevant here?
                    id: DieselUlid::from_str(&p).map_err(|e| {
                        log::error!("{}", e);
                        tonic::Status::internal("ULID conversion error")
                    })?,
                    level: crate::database::enums::PermissionLevels::APPEND,
                    allow_sa: true,
                });

                match &self.authorizer.check_permissions(&token, ctx) {
                    Ok(b) => {
                        if *b {
                            // TODO!
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
                let create_relation = InternalRelation {
                    id: DieselUlid::generate(),
                    origin_pid: parent,
                    is_persistent: false,
                    target_pid: new_version_object_id,
                    type_id: 1,
                };
                create_relation
                    .create(&transaction_client)
                    .await
                    .map_err(|e| {
                        log::error!("{}", e);
                        tonic::Status::aborted("Database transaction failed.")
                    })?;
                Some(create_relation)
            }
            None => None,
        };

        let new_version_object = Object {
            id: new_version_object_id,
            content_len: old_object.content_len,
            shared_id: old_object.shared_id,
            count: 1,
            created_at: None,
            created_by: user_id,
            revision_number: old_object.revision_number + 1,
            object_status: old_object.object_status,
            object_type: ObjectType::OBJECT,
            description: match inner_request.description {
                Some(d) => d,
                None => old_object.description,
            },
            name: match inner_request.name {
                Some(n) => n,
                None => old_object.name,
            },
            hashes: match inner_request.hashes.is_empty() {
                false => Json(inner_request.hashes.try_into().map_err(|e| {
                    log::error!("{}", e);
                    tonic::Status::internal("Invalid hashes.")
                })?),
                true => old_object.hashes.into(),
            },
            data_class,
            key_values: Json(KeyValues(key_values)),
            external_relations: old_object.external_relations.clone(),
        };
        new_version_object
            .create(&transaction_client)
            .await
            .map_err(|e| {
                log::error!("{}", e);
                tonic::Status::aborted("Database transaction failed.")
            })?;
        let external_relations: ExternalRelations =
            old_object.external_relations.0.try_into().map_err(|e| {
                log::error!("{}", e);
                tonic::Status::internal("ExternalRelation conversion error.")
            })?;

        let mut relations: Vec<Relation> = external_relations
            .0
            .into_iter()
            .map(|r| Relation {
                relation: Some(RelationEnum::External(r.into())),
            })
            .collect();
        match parent_relation {
            Some(r) => {
                relations.push(Relation {
                    relation: Some(RelationEnum::Internal(
                        InternalRelation::from_db_internal_relation(r, false, 1),
                    )),
                });
            }
            None => (),
        };
        let grpc_object = GRPCObject {
            id: new_version_object_id.to_string(),
            name: new_version_object.name,
            description: new_version_object.description,
            key_values: new_version_object.key_values.0.into(),
            relations,
            content_len: new_version_object.content_len,
            data_class: new_version_object.data_class.into(),
            created_at: None, // TODO
            created_by: user_id.to_string(),
            status: new_version_object.object_status.into(),
            dynamic: false,
            hashes: new_version_object.hashes.0.into(),
        };
        Ok(tonic::Response::new(UpdateObjectResponse {
            object: Some(grpc_object),
            new_revision: true,
        }))
    }

    async fn delete_object(
        &self,
        request: Request<DeleteObjectRequest>,
    ) -> Result<Response<DeleteObjectResponse>> {
        log::info!("Recieved CreateObjectRequest.");
        log::debug!("{:?}", &request);

        let token = get_token_from_md(request.metadata()).map_err(|e| {
            log::debug!("{}", e);
            tonic::Status::unauthenticated("Token authentication error.")
        })?;

        let inner_request = request.into_inner();
        let object_id = DieselUlid::from_str(&inner_request.object_id).map_err(|e| {
            log::error!("{}", e);
            tonic::Status::internal("ULID conversion error")
        })?;
        let ctx = Context::Object(ResourcePermission {
            id: object_id,
            level: crate::database::enums::PermissionLevels::WRITE, // delete?
            allow_sa: false,
        });

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

        let object_id = DieselUlid::from_str(&inner_request.object_id).map_err(|e| {
            log::error!("{}", e);
            tonic::Status::internal("ULID conversion error.")
        })?;
        let mut client = self.database.get_client().await.map_err(|e| {
            log::error!("{}", e);
            tonic::Status::unavailable("Database not avaliable.")
        })?;
        let transaction = client.transaction().await.map_err(|e| {
            log::error!("{}", e);
            tonic::Status::unavailable("Database not avaliable.")
        })?;
        let transaction_client = transaction.client();
        let object = Object::get(object_id, &transaction_client)
            .await
            .map_err(|e| {
                log::error!("{}", e);
                tonic::Status::unavailable("Database call failed.")
            })?
            .ok_or(tonic::Status::not_found("Object not found."))?;
        match inner_request.with_revisions {
            true => {
                let revisions = Object::get_all_revisions(&object.shared_id, &transaction_client)
                    .await
                    .map_err(|e| {
                        log::error!("{}", e);
                        tonic::Status::unavailable("Revisions not found")
                    })?;
                for r in revisions {
                    r.delete(r.id, &transaction_client).await.map_err(|e| {
                        log::error!("{}", e);
                        tonic::Status::aborted("Database delete transaction failed.")
                    })?;
                }
            }
            false => {
                object
                    .delete(object.id, &transaction_client)
                    .await
                    .map_err(|e| {
                        log::error!("{}", e);
                        tonic::Status::aborted("Database delete transaction failed.")
                    })?;
            }
        };
        Ok(tonic::Response::new(DeleteObjectResponse {}))
    }
    async fn get_object(
        &self,
        request: Request<GetObjectRequest>,
    ) -> Result<Response<GetObjectResponse>> {
        log::info!("Recieved CreateObjectRequest.");
        log::debug!("{:?}", &request);

        let token = get_token_from_md(request.metadata()).map_err(|e| {
            log::debug!("{}", e);
            tonic::Status::unauthenticated("Token authentication error.")
        })?;

        let inner_request = request.into_inner();
        let object_id = DieselUlid::from_str(&inner_request.object_id).map_err(|e| {
            log::error!("{}", e);
            tonic::Status::internal("ULID conversion error")
        })?;
        let ctx = Context::Object(ResourcePermission {
            id: object_id,
            level: crate::database::enums::PermissionLevels::READ, // append?
            allow_sa: true,
        });

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

        let get_object = match Object::get(object_id, &client).await.map_err(|e| {
            log::error!("{}", e);
            tonic::Status::aborted("Database read error.")
        })? {
            Some(o) => o,
            None => return Err(tonic::Status::not_found("Object not found")),
        };

        let (to_relations, from_relations) =
            InternalRelation::get_filtered_by_id(object_id, &client)
                .await
                .map_err(|e| {
                    log::error!("{}", e);
                    tonic::Status::aborted("Database read error.")
                })?;
        let mut from_relations = match from_relations {
            Some(r) => r
                .into_iter()
                .map(|r| Relation {
                    relation: Some(RelationEnum::Internal(
                        InternalRelation::from_db_internal_relation(r, true, 4),
                    )),
                })
                .collect(),
            None => Vec::new(),
        };
        let mut to_relations = to_relations
            .into_iter()
            .map(|r| Relation {
                relation: Some(RelationEnum::Internal(
                    InternalRelation::from_db_internal_relation(r, false, 4),
                )),
            })
            .collect();
        let mut relations: Vec<Relation> = get_object
            .external_relations
            .0
             .0
            .into_iter()
            .map(|r| Relation {
                relation: Some(RelationEnum::External(r.into())),
            })
            .collect();
        relations.append(&mut to_relations);
        relations.append(&mut from_relations);

        let grpc_object = GRPCObject {
            id: object_id.to_string(),
            content_len: get_object.content_len,
            name: get_object.name,
            description: get_object.description,
            created_at: None, //TODO
            created_by: get_object.created_by.to_string(),
            data_class: get_object.data_class.into(),
            dynamic: false,
            hashes: get_object.hashes.0.into(),
            key_values: get_object.key_values.0.into(),
            status: get_object.object_status.into(),
            relations,
        };

        Ok(tonic::Response::new(GetObjectResponse {
            object: Some(grpc_object),
        }))
    }
    async fn get_objects(
        &self,
        request: Request<GetObjectsRequest>,
    ) -> Result<Response<GetObjectsResponse>> {
        //TODO
        Err(tonic::Status::unimplemented(
            "GetObjects is not implemented.",
        ))
    }
    async fn get_upload_url(
        &self,
        request: Request<GetUploadUrlRequest>,
    ) -> Result<Response<GetUploadUrlResponse>> {
        //TODO
        Err(tonic::Status::unimplemented(
            "GetUploadURL is not implemented.",
        ))
    }
    async fn get_download_url(
        &self,
        request: Request<GetDownloadUrlRequest>,
    ) -> Result<Response<GetDownloadUrlResponse>> {
        //TODO
        Err(tonic::Status::unimplemented(
            "GetDownloadURL is not implemented.",
        ))
    }
    async fn clone_object(
        &self,
        request: Request<CloneObjectRequest>,
    ) -> Result<Response<CloneObjectResponse>> {
        //TODO
        Err(tonic::Status::unimplemented(
            "CloneObject is not implemented.",
        ))
    }
}
