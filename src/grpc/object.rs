use crate::auth::{Authorizer, Context, ResourcePermission};
use crate::caching::cache::Cache;
use crate::database::connection::Database;
use crate::database::crud::CrudDb;
use crate::database::enums::ObjectType;
use crate::database::internal_relation_dsl::InternalRelation;
use crate::database::internal_relation_dsl::INTERNAL_RELATION_VARIANT_BELONGS_TO;
use crate::database::object_dsl::{
    ExternalRelations, Hashes, KeyValue as DBKeyValue, KeyValues, Object,
};
use crate::utils::conversions::{from_db_internal_relation, get_token_from_md};
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
                let (id, var) = match parent {
                    CreateParent::ProjectId(id) => (id, ObjectType::PROJECT),
                    CreateParent::CollectionId(id) => (id, ObjectType::COLLECTION),
                    CreateParent::DatasetId(id) => (id, ObjectType::DATASET),
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
            origin_type: variant.clone(),
            is_persistent: false,
            target_pid: create_object.id,
            target_type: ObjectType::OBJECT,
            type_name: INTERNAL_RELATION_VARIANT_BELONGS_TO.to_string(),
        };

        create_relation
            .create(transaction_client)
            .await
            .map_err(|e| {
                log::error!("{}", e);
                tonic::Status::aborted("Database transaction failed.")
            })?;
        create_object
            .create(transaction_client)
            .await
            .map_err(|e| {
                log::error!("{}", e);
                tonic::Status::aborted("Database transaction failed.")
            })?;

        let parent_relation = Some(RelationEnum::Internal(APIInternalRelation {
            resource_id: create_object.id.to_string(),
            resource_variant: variant.into(),
            direction: 2,
            defined_variant: 1,
            custom_variant: None,
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
        log::info!("Recieved FinishObjectStagingRequest.");
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
            created_at: to_update_object.created_at.map(|t| t.into()),
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
        log::info!("Recieved UpdateObjectRequest.");
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

        let grpc_object = if inner_request.name.is_some()
            || !inner_request.remove_key_values.is_empty()
            || !inner_request.hashes.is_empty()
        {
            // Create new object

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
                // Can only add new parents
                Some(p) => {
                    let (p, v) = match p {
                        UpdateParent::ProjectId(p) => (p, ObjectType::PROJECT),
                        UpdateParent::DatasetId(p) => (p, ObjectType::COLLECTION),
                        UpdateParent::CollectionId(p) => (p, ObjectType::DATASET),
                    };
                    let parent = DieselUlid::from_str(&p).map_err(|e| {
                        log::error!("{}", e);
                        tonic::Status::internal("ULID conversion error.")
                    })?;

                    let ctx = Context::Object(ResourcePermission {
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
                        origin_type: v,
                        is_persistent: false,
                        target_pid: new_version_object_id,
                        target_type: ObjectType::OBJECT,
                        type_name: INTERNAL_RELATION_VARIANT_BELONGS_TO.to_string(),
                    };
                    create_relation
                        .create(transaction_client)
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
                created_at: None, //
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
                    true => old_object.hashes,
                },
                data_class,
                key_values: Json(KeyValues(key_values)),
                external_relations: old_object.external_relations.clone(),
            };
            new_version_object
                .create(transaction_client)
                .await
                .map_err(|e| {
                    log::error!("{}", e);
                    tonic::Status::aborted("Database transaction failed.")
                })?;
            let external_relations: ExternalRelations = old_object.external_relations.0;

            let mut relations: Vec<Relation> = external_relations
                .0
                .into_iter()
                .map(|r| Relation {
                    relation: Some(RelationEnum::External(r.into())),
                })
                .collect();
            if let Some(r) = parent_relation {
                relations.push(Relation {
                    relation: Some(RelationEnum::Internal(
                        from_db_internal_relation(r.clone(), false, r.origin_type.into()).map_err(
                            |e| {
                                log::error!("{}", e);
                                tonic::Status::internal("Internal custom type conversion error.")
                            },
                        )?,
                    )),
                });
            };

            GRPCObject {
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
            }
        } else {
            // Update object

            let data_class = match inner_request.data_class {
                0 => Ok(old_object.data_class),
                1..=5 => {
                    let old_data_class: i32 = old_object.data_class.into();
                    if old_data_class < inner_request.data_class {
                        inner_request.data_class.try_into()
                    } else {
                        return Err(tonic::Status::internal("Dataclass can only be relaxed."));
                    }
                }
                _ => return Err(tonic::Status::internal("Invalid dataclass.")),
            }
            .map_err(|e| {
                log::error!("{}", e);
                tonic::Status::internal("Invalid dataclass.")
            })?;

            let mut key_values: Vec<DBKeyValue> = old_object.key_values.0 .0;

            if !inner_request.add_key_values.is_empty() {
                let mut add_kv: KeyValues =
                    inner_request.add_key_values.try_into().map_err(|e| {
                        log::error!("{}", e);
                        tonic::Status::internal("KeyValue conversion error.")
                    })?;
                key_values.append(&mut add_kv.0);
            }

            let transaction = client.transaction().await.map_err(|e| {
                log::error!("{}", e);
                tonic::Status::unavailable("Database not avaliable.")
            })?;

            let transaction_client = transaction.client();
            let parent_relation = match inner_request.parent {
                // Can only add new parents
                Some(p) => {
                    let (p, v) = match p {
                        UpdateParent::ProjectId(p) => (p, ObjectType::PROJECT),
                        UpdateParent::DatasetId(p) => (p, ObjectType::DATASET),
                        UpdateParent::CollectionId(p) => (p, ObjectType::COLLECTION),
                    };
                    let parent = DieselUlid::from_str(&p).map_err(|e| {
                        log::error!("{}", e);
                        tonic::Status::internal("ULID conversion error.")
                    })?;

                    let ctx = Context::Object(ResourcePermission {
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
                        origin_type: v,
                        is_persistent: false,
                        target_pid: old_object.id,
                        target_type: ObjectType::OBJECT,
                        type_name: INTERNAL_RELATION_VARIANT_BELONGS_TO.to_string(),
                    };
                    let exists = InternalRelation::get_by_pids(
                        create_relation.origin_pid,
                        create_relation.target_pid,
                        transaction_client,
                    )
                    .await
                    .map_err(|e| {
                        log::error!("{}", e);
                        tonic::Status::internal("Database transaction error.")
                    })?
                    .is_some();
                    if exists {
                        return Err(tonic::Status::internal(
                            "InternalRelation to parent already exists.",
                        ));
                    } else {
                        create_relation
                            .create(transaction_client)
                            .await
                            .map_err(|e| {
                                log::error!("{}", e);
                                tonic::Status::aborted("Database transaction failed.")
                            })?;
                        Some(create_relation)
                    }
                }
                None => None,
            };

            let updated_object = Object {
                id: old_object.id,
                content_len: old_object.content_len,
                shared_id: old_object.shared_id,
                count: 1,
                created_at: old_object.created_at, //
                created_by: user_id,
                revision_number: old_object.revision_number,
                object_status: old_object.object_status,
                object_type: ObjectType::OBJECT,
                description: match inner_request.description {
                    Some(d) => d,
                    None => old_object.description,
                },
                name: old_object.name,
                hashes: old_object.hashes,
                data_class, //TODO
                key_values: Json(KeyValues(key_values)),
                external_relations: old_object.external_relations.clone(),
            };
            updated_object
                .update(transaction_client)
                .await
                .map_err(|e| {
                    log::error!("{}", e);
                    tonic::Status::aborted("Database transaction failed.")
                })?;
            let external_relations: ExternalRelations = old_object.external_relations.0;

            let mut relations: Vec<Relation> = external_relations
                .0
                .into_iter()
                .map(|r| Relation {
                    relation: Some(RelationEnum::External(r.into())),
                })
                .collect();
            if let Some(r) = parent_relation {
                relations.push(Relation {
                    relation: Some(RelationEnum::Internal(
                        from_db_internal_relation(r.clone(), false, r.origin_type.into()).map_err(
                            |e| {
                                log::error!("{}", e);
                                tonic::Status::internal("Internal custom type conversion error.")
                            },
                        )?,
                    )),
                });
            };

            GRPCObject {
                id: updated_object.id.to_string(),
                name: updated_object.name,
                description: updated_object.description,
                key_values: updated_object.key_values.0.into(),
                relations,
                content_len: updated_object.content_len,
                data_class: updated_object.data_class.into(),
                created_at: old_object.created_at.map(|t| t.into()),
                created_by: user_id.to_string(),
                status: updated_object.object_status.into(),
                dynamic: false,
                hashes: updated_object.hashes.0.into(),
            }
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
        log::info!("Recieved DeleteObjectRequest.");
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
            level: crate::database::enums::PermissionLevels::ADMIN,
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
        let object = Object::get(object_id, transaction_client)
            .await
            .map_err(|e| {
                log::error!("{}", e);
                tonic::Status::unavailable("Database call failed.")
            })?
            .ok_or(tonic::Status::not_found("Object not found."))?;
        // Should only mark as deleted
        match inner_request.with_revisions {
            true => {
                let revisions = Object::get_all_revisions(&object.shared_id, transaction_client)
                    .await
                    .map_err(|e| {
                        log::error!("{}", e);
                        tonic::Status::unavailable("Revisions not found")
                    })?;
                for r in revisions {
                    r.delete(transaction_client).await.map_err(|e| {
                        log::error!("{}", e);
                        tonic::Status::aborted("Database delete transaction failed.")
                    })?;
                }
            }
            false => {
                object.delete(transaction_client).await.map_err(|e| {
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
        log::info!("Recieved GetObjectRequest.");
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

        let get_object = Object::get_object_with_relations(&object_id, &client)
            .await
            .map_err(|e| {
                log::error!("{}", e);
                tonic::Status::aborted("Database read error.")
            })?;

        let grpc_object = get_object.try_into().map_err(|e| {
            log::error!("{}", e);
            tonic::Status::internal("Object conversion error.")
        })?;
        Ok(tonic::Response::new(GetObjectResponse {
            object: Some(grpc_object),
        }))
    }
    async fn get_objects(
        &self,
        _request: Request<GetObjectsRequest>,
    ) -> Result<Response<GetObjectsResponse>> {
        //TODO
        Err(tonic::Status::unimplemented(
            "GetObjects is not implemented.",
        ))
    }
    async fn get_upload_url(
        &self,
        _request: Request<GetUploadUrlRequest>,
    ) -> Result<Response<GetUploadUrlResponse>> {
        //TODO
        Err(tonic::Status::unimplemented(
            "GetUploadURL is not implemented.",
        ))
    }
    async fn get_download_url(
        &self,
        _request: Request<GetDownloadUrlRequest>,
    ) -> Result<Response<GetDownloadUrlResponse>> {
        //TODO
        Err(tonic::Status::unimplemented(
            "GetDownloadURL is not implemented.",
        ))
    }
    async fn clone_object(
        &self,
        _request: Request<CloneObjectRequest>,
    ) -> Result<Response<CloneObjectResponse>> {
        //TODO
        Err(tonic::Status::unimplemented(
            "CloneObject is not implemented.",
        ))
    }
}
