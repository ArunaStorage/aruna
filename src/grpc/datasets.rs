use crate::auth::{Authorizer, Context, ResourcePermission};
use crate::caching::cache::Cache;
use crate::database::connection::Database;
use crate::database::crud::CrudDb;
use crate::database::enums::ObjectType;
use crate::database::internal_relation_dsl::{
    InternalRelation, INTERNAL_RELATION_VARIANT_BELONGS_TO,
};
use crate::database::object_dsl::{ExternalRelations, Hashes, KeyValues, Object};
use crate::utils::conversions::get_token_from_md;
use aruna_rust_api::api::storage::models::v2::{
    relation::Relation as RelationEnum, Dataset as GRPCDataset,
    InternalRelation as APIInternalRelation, Relation, Stats,
};
use aruna_rust_api::api::storage::services::v2::create_dataset_request::Parent as CreateParent;
use aruna_rust_api::api::storage::services::v2::dataset_service_server::DatasetService;
use aruna_rust_api::api::storage::services::v2::{
    CreateDatasetRequest, CreateDatasetResponse, DeleteDatasetRequest, DeleteDatasetResponse,
    GetDatasetRequest, GetDatasetResponse, GetDatasetsRequest, GetDatasetsResponse,
    SnapshotDatasetRequest, SnapshotDatasetResponse, UpdateDatasetDataClassRequest,
    UpdateDatasetDataClassResponse, UpdateDatasetDescriptionRequest,
    UpdateDatasetDescriptionResponse, UpdateDatasetKeyValuesRequest,
    UpdateDatasetKeyValuesResponse, UpdateDatasetNameRequest, UpdateDatasetNameResponse,
};
use diesel_ulid::DieselUlid;
use postgres_types::Json;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;
use tonic::{Request, Response, Result};

crate::impl_grpc_server!(DatasetServiceImpl);

#[tonic::async_trait]
impl DatasetService for DatasetServiceImpl {
    async fn create_dataset(
        &self,
        request: Request<CreateDatasetRequest>,
    ) -> Result<Response<CreateDatasetResponse>> {
        log::info!("Recieved CreateDatasetRequest.");
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
            object_status: crate::database::enums::ObjectStatus::AVAILABLE,
            data_class: inner_request.data_class.try_into().map_err(|e| {
                log::error!("{}", e);
                tonic::Status::internal("DataClass conversion error.")
            })?,
            object_type: crate::database::enums::ObjectType::DATASET,
            external_relations: Json(external_relations.clone()),
            hashes: Json(Hashes(Vec::new())),
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
            target_type: ObjectType::DATASET,
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

        let stats = Some(Stats {
            count: 0,
            size: 0,
            last_updated: None, //TODO
        });
        let grpc_dataset = GRPCDataset {
            id: create_object.id.to_string(),
            name: create_object.name,
            description: create_object.description,
            key_values: key_values.into(),
            relations,
            data_class: create_object.data_class.into(),
            created_at: None, // TODO
            created_by: user_id.to_string(),
            status: create_object.object_status.into(),
            dynamic: false,
            stats,
        };
        Ok(tonic::Response::new(CreateDatasetResponse {
            dataset: Some(grpc_dataset),
        }))
    }
    async fn get_dataset(
        &self,
        request: Request<GetDatasetRequest>,
    ) -> Result<Response<GetDatasetResponse>> {
        log::info!("Recieved GetDatasetRequest.");
        log::debug!("{:?}", &request);

        let token = get_token_from_md(request.metadata()).map_err(|e| {
            log::debug!("{}", e);
            tonic::Status::unauthenticated("Token authentication error.")
        })?;

        let inner_request = request.into_inner();
        let object_id = DieselUlid::from_str(&inner_request.dataset_id).map_err(|e| {
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

        let dataset = Some(get_object.try_into().map_err(|e| {
            log::error!("{}", e);
            tonic::Status::internal("ObjectFromRelations conversion failed.")
        })?);

        Ok(tonic::Response::new(GetDatasetResponse { dataset }))
    }

    async fn update_dataset_name(
        &self,
        request: Request<UpdateDatasetNameRequest>,
    ) -> Result<Response<UpdateDatasetNameResponse>> {
        log::info!("Recieved UpdateDatasetNameRequest.");
        log::debug!("{:?}", &request);

        let token = get_token_from_md(request.metadata()).map_err(|e| {
            log::debug!("{}", e);
            tonic::Status::unauthenticated("Token authentication error.")
        })?;

        let inner_request = request.into_inner();
        let object_id = DieselUlid::from_str(&inner_request.dataset_id).map_err(|e| {
            log::error!("{}", e);
            tonic::Status::internal("ULID conversion error")
        })?;
        let ctx = Context::Object(ResourcePermission {
            id: object_id,
            level: crate::database::enums::PermissionLevels::WRITE, // append?
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
        let dataset = Some(object.try_into().map_err(|e| {
            log::error!("{}", e);
            tonic::Status::aborted("Database request failed.")
        })?);
        Ok(tonic::Response::new(UpdateDatasetNameResponse { dataset }))
    }
    async fn update_dataset_description(
        &self,
        request: Request<UpdateDatasetDescriptionRequest>,
    ) -> Result<Response<UpdateDatasetDescriptionResponse>> {
        log::info!("Recieved UpdateDatasetDescriptionRequest.");
        log::debug!("{:?}", &request);

        let token = get_token_from_md(request.metadata()).map_err(|e| {
            log::debug!("{}", e);
            tonic::Status::unauthenticated("Token authentication error.")
        })?;

        let inner_request = request.into_inner();
        let object_id = DieselUlid::from_str(&inner_request.dataset_id).map_err(|e| {
            log::error!("{}", e);
            tonic::Status::internal("ULID conversion error")
        })?;
        let ctx = Context::Object(ResourcePermission {
            id: object_id,
            level: crate::database::enums::PermissionLevels::WRITE, // append?
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
        let dataset = Some(object.try_into().map_err(|e| {
            log::error!("{}", e);
            tonic::Status::aborted("Database request failed.")
        })?);
        Ok(tonic::Response::new(UpdateDatasetDescriptionResponse {
            dataset,
        }))
    }

    async fn update_dataset_data_class(
        &self,
        request: Request<UpdateDatasetDataClassRequest>,
    ) -> Result<Response<UpdateDatasetDataClassResponse>> {
        log::info!("Recieved UpdateDatasetDataClassRequest.");
        log::debug!("{:?}", &request);

        let token = get_token_from_md(request.metadata()).map_err(|e| {
            log::debug!("{}", e);
            tonic::Status::unauthenticated("Token authentication error.")
        })?;

        let inner_request = request.into_inner();
        let object_id = DieselUlid::from_str(&inner_request.dataset_id).map_err(|e| {
            log::error!("{}", e);
            tonic::Status::internal("ULID conversion error")
        })?;
        let ctx = Context::Object(ResourcePermission {
            id: object_id,
            level: crate::database::enums::PermissionLevels::WRITE, // append?
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
        let dataset = Some(object.try_into().map_err(|e| {
            log::error!("{}", e);
            tonic::Status::aborted("Database request failed.")
        })?);
        Ok(tonic::Response::new(UpdateDatasetDataClassResponse {
            dataset,
        }))
    }
    async fn update_dataset_key_values(
        &self,
        request: Request<UpdateDatasetKeyValuesRequest>,
    ) -> Result<Response<UpdateDatasetKeyValuesResponse>> {
        log::info!("Recieved UpdateDatasetKeyValuesRequest.");
        log::debug!("{:?}", &request);

        let token = get_token_from_md(request.metadata()).map_err(|e| {
            log::debug!("{}", e);
            tonic::Status::unauthenticated("Token authentication error.")
        })?;

        let inner_request = request.into_inner();
        let dataset_id = DieselUlid::from_str(&inner_request.dataset_id).map_err(|e| {
            log::error!("{}", e);
            tonic::Status::internal("ULID conversion error.")
        })?;

        let ctx = Context::Object(ResourcePermission {
            id: dataset_id,
            level: crate::database::enums::PermissionLevels::WRITE, // append?
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
        let dataset = Some(dataset_with_relations.try_into().map_err(|e| {
            log::error!("{}", e);
            tonic::Status::internal("Dataset conversion error.")
        })?);

        Ok(tonic::Response::new(UpdateDatasetKeyValuesResponse {
            dataset,
        }))
    }

    async fn delete_dataset(
        &self,
        _request: Request<DeleteDatasetRequest>,
    ) -> Result<Response<DeleteDatasetResponse>> {
        todo!()
    }
    async fn get_datasets(
        &self,
        _request: Request<GetDatasetsRequest>,
    ) -> Result<Response<GetDatasetsResponse>> {
        todo!()
    }
    async fn snapshot_dataset(
        &self,
        _request: Request<SnapshotDatasetRequest>,
    ) -> Result<Response<SnapshotDatasetResponse>> {
        todo!()
    }
}
