use crate::database::connection::Database;
use crate::database::crud::CrudDb;
use crate::database::dsls::internal_relation_dsl::InternalRelation;
use crate::database::dsls::internal_relation_dsl::{
    INTERNAL_RELATION_VARIANT_BELONGS_TO, INTERNAL_RELATION_VARIANT_METADATA,
    INTERNAL_RELATION_VARIANT_ORIGIN, INTERNAL_RELATION_VARIANT_POLICY,
    INTERNAL_RELATION_VARIANT_VERSION,
};
use crate::database::dsls::object_dsl::{DefinedVariant, ExternalRelation, Object};
use crate::database::enums::ObjectType;
use crate::utils::conversions::get_token_from_md;
use aruna_cache::notifications::NotificationCache;
use aruna_policy::ape::policy_evaluator::PolicyEvaluator;
use aruna_policy::ape::structs::PermissionLevels as PolicyLevels;
use aruna_policy::ape::structs::{ApeResourcePermission, Context, ResourceContext};
use aruna_rust_api::api::storage::models::v2::relation;
use aruna_rust_api::api::storage::services::v2::get_hierachy_response::Graph;
use aruna_rust_api::api::storage::services::v2::relations_service_server::RelationsService;
use aruna_rust_api::api::storage::services::v2::ModifyRelationsResponse;
use aruna_rust_api::api::storage::services::v2::{CollectionRelations, GetHierachyResponse};
use aruna_rust_api::api::storage::services::v2::{DatasetRelations, GetHierachyRequest};
use aruna_rust_api::api::storage::services::v2::{ModifyRelationsRequest, ProjectRelations};
use diesel_ulid::DieselUlid;
use std::str::FromStr;
use std::sync::Arc;
use tokio_postgres::Client;
use tonic::{Request, Response, Result};

crate::impl_grpc_server!(RelationsServiceImpl);

#[tonic::async_trait]
impl RelationsService for RelationsServiceImpl {
    async fn modify_relations(
        &self,
        request: Request<ModifyRelationsRequest>,
    ) -> Result<Response<ModifyRelationsResponse>> {
        log::info!("Recieved ModifyRelationsRequest.");
        log::debug!("{:?}", &request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );
        let inner_request = request.into_inner();
        let resource_id = tonic_invalid!(
            DieselUlid::from_str(&inner_request.resource_id),
            "ULID conversion error"
        );
        let ctx = Context::ResourceContext(ResourceContext::Object(ApeResourcePermission {
            id: resource_id,
            level: PolicyLevels::WRITE,
            allow_sa: false,
        }));
        let user_id = tonic_auth!(
            self.authorizer.check_context(&token, ctx).await,
            "User not authenticated"
        );

        let mut client =
            tonic_internal!(self.database.get_client().await, "Database not avaliable.");

        let transaction = client.transaction().await.map_err(|e| {
            log::error!("{}", e);
            tonic::Status::unavailable("Database not avaliable.")
        })?;

        let client = transaction.client();

        let resource = Object::get(resource_id, client)
            .await
            .map_err(|e| {
                log::error!("{}", e);
                tonic::Status::unavailable("Database transaction failed.")
            })?
            .ok_or(tonic::Status::not_found("Resource not found"))?;
        let resource_type = resource.clone().object_type;
        for relation in inner_request.add_relations {
            if let Some(rel) = relation.relation {
                match rel {
                    relation::Relation::External(external) => {
                        let external_relation = match external.defined_variant {
                            0 => {
                                return Err(tonic::Status::internal(
                                    "Undefined external variants are forbidden.",
                                ))
                            }
                            1 => ExternalRelation {
                                identifier: external.identifier,
                                defined_variant: DefinedVariant::URL,
                                custom_variant: None,
                            },
                            2 => ExternalRelation {
                                identifier: external.identifier,
                                defined_variant: DefinedVariant::IDENTIFIER,
                                custom_variant: None,
                            },
                            3 => ExternalRelation {
                                identifier: external.identifier,
                                defined_variant: DefinedVariant::CUSTOM,
                                custom_variant: external.custom_variant,
                            },
                            _ => {
                                return Err(tonic::Status::internal(
                                    "ExternalRelation conversion error.",
                                ))
                            }
                        };
                        Object::add_external_relations(&resource_id, client, external_relation)
                            .await
                            .map_err(|e| {
                                log::error!("{}", e);
                                tonic::Status::aborted("Database transaction error.")
                            })?;
                    }
                    relation::Relation::Internal(internal) => {
                        let (origin_pid, origin_type, target_pid, target_type) = match internal
                            .direction
                        {
                            0 => {
                                return Err(tonic::Status::internal(
                                    "Undefined directions are forbidden.",
                                ))
                            }
                            1 => {
                                let origin_pid = DieselUlid::from_str(&internal.resource_id)
                                    .map_err(|e| {
                                        log::error!("{}", e);
                                        tonic::Status::internal("ULID conversion error.")
                                    })?;
                                let ctx = Context::ResourceContext(ResourceContext::Object(
                                    ApeResourcePermission {
                                        id: origin_pid,
                                        level: PolicyLevels::WRITE,
                                        allow_sa: false,
                                    },
                                ));
                                let user_id =
                                    self.authorizer.check_context(&token, ctx).await.map_err(
                                        |e| {
                                            log::error!("{}", e);
                                            tonic::Status::unauthenticated("User not authenticated")
                                        },
                                    )?;
                                (
                                    origin_pid,
                                    internal.resource_variant.try_into().map_err(|e| {
                                        log::error!("{}", e);
                                        tonic::Status::internal("ResourceVariant conversion error.")
                                    })?,
                                    resource_id,
                                    resource_type.clone(),
                                )
                            }
                            2 => {
                                let target_pid = DieselUlid::from_str(&internal.resource_id)
                                    .map_err(|e| {
                                        log::error!("{}", e);
                                        tonic::Status::internal("ULID conversion error.")
                                    })?;
                                let ctx = Context::ResourceContext(ResourceContext::Object(
                                    ApeResourcePermission {
                                        id: target_pid,
                                        level: PolicyLevels::WRITE,
                                        allow_sa: true,
                                    },
                                ));
                                self.authorizer
                                    .check_context(&token, ctx)
                                    .await
                                    .map_err(|e| {
                                        log::error!("{}", e);
                                        tonic::Status::unauthenticated("User not authenticated")
                                    })?;
                                (
                                    resource_id,
                                    resource_type.clone(),
                                    target_pid,
                                    internal.resource_variant.try_into().map_err(|e| {
                                        log::error!("{}", e);
                                        tonic::Status::internal("ResourceVariant conversion error.")
                                    })?,
                                )
                            }
                            _ => {
                                return Err(tonic::Status::internal("Direction conversion error."))
                            }
                        };
                        let internal_relation = match internal.defined_variant {
                            0 => {
                                return Err(tonic::Status::internal(
                                    "Undefined internal variants are forbidden.",
                                ))
                            }
                            i if i > 0 && i < 6 => {
                                let type_name = match i {
                                    1 => INTERNAL_RELATION_VARIANT_BELONGS_TO.to_string(),
                                    2 => INTERNAL_RELATION_VARIANT_ORIGIN.to_string(),
                                    3 => INTERNAL_RELATION_VARIANT_VERSION.to_string(),
                                    4 => INTERNAL_RELATION_VARIANT_METADATA.to_string(),
                                    5 => INTERNAL_RELATION_VARIANT_POLICY.to_string(),
                                    _ => {
                                        return Err(tonic::Status::internal(
                                            "Undefined internal variants are forbidden.",
                                        ))
                                    }
                                };
                                InternalRelation {
                                    id: DieselUlid::generate(),
                                    origin_pid,
                                    origin_type,
                                    type_name,
                                    target_pid,
                                    target_type,
                                    is_persistent: false,
                                }
                            }
                            6 => {
                                let name = internal.custom_variant.ok_or(
                                    tonic::Status::invalid_argument(
                                        "No custom variant name specified.",
                                    ),
                                )?;
                                InternalRelation {
                                    id: DieselUlid::generate(),
                                    origin_pid,
                                    origin_type,
                                    type_name: name,
                                    target_pid,
                                    target_type,
                                    is_persistent: false,
                                }
                            }
                            _ => {
                                return Err(tonic::Status::internal(
                                    "InternalRelation conversion error.",
                                ))
                            }
                        };
                        let exists = InternalRelation::get_by_pids(origin_pid, target_pid, client)
                            .await
                            .map_err(|e| {
                                log::error!("{}", e);
                                tonic::Status::aborted("Database transaction failed.")
                            })?
                            .is_some();
                        if exists {
                            return Err(tonic::Status::aborted("Relation already exists"));
                        } else {
                            internal_relation.create(client).await.map_err(|e| {
                                log::error!("{}", e);
                                tonic::Status::aborted("Database transaction failed.")
                            })?;
                        }
                    }
                }
            }
        }
        for relation in inner_request.remove_relations {
            if let Some(rel) = relation.relation {
                match rel {
                    relation::Relation::External(external) => {
                        let external_relation = match external.defined_variant {
                            0 => {
                                return Err(tonic::Status::internal(
                                    "Undefined external variants are forbidden.",
                                ))
                            }
                            1 => ExternalRelation {
                                identifier: external.identifier,
                                defined_variant: DefinedVariant::URL,
                                custom_variant: None,
                            },
                            2 => ExternalRelation {
                                identifier: external.identifier,
                                defined_variant: DefinedVariant::IDENTIFIER,
                                custom_variant: None,
                            },
                            3 => ExternalRelation {
                                identifier: external.identifier,
                                defined_variant: DefinedVariant::CUSTOM,
                                custom_variant: external.custom_variant,
                            },
                            _ => {
                                return Err(tonic::Status::internal(
                                    "ExternalRelation conversion error.",
                                ))
                            }
                        };
                        Object::remove_external_relation(&resource, client, external_relation)
                            .await
                            .map_err(|e| {
                                log::error!("{}", e);
                                tonic::Status::aborted("Database transaction error.")
                            })?;
                    }
                    relation::Relation::Internal(internal) => {
                        let (origin_pid, target_pid) = match internal.direction {
                            0 => {
                                return Err(tonic::Status::internal(
                                    "Undefined directions are forbidden.",
                                ))
                            }
                            1 => {
                                let origin_pid = DieselUlid::from_str(&internal.resource_id)
                                    .map_err(|e| {
                                        log::error!("{}", e);
                                        tonic::Status::internal("ULID conversion error.")
                                    })?;
                                let ctx = Context::ResourceContext(ResourceContext::Object(
                                    ApeResourcePermission {
                                        id: origin_pid,
                                        level: PolicyLevels::WRITE,
                                        allow_sa: true,
                                    },
                                ));
                                self.authorizer
                                    .check_context(&token, ctx)
                                    .await
                                    .map_err(|e| {
                                        log::error!("{}", e);
                                        tonic::Status::unauthenticated("User not authenticated")
                                    })?;
                                (origin_pid, resource_id)
                            }
                            2 => {
                                let target_pid = DieselUlid::from_str(&internal.resource_id)
                                    .map_err(|e| {
                                        log::error!("{}", e);
                                        tonic::Status::internal("ULID conversion error.")
                                    })?;
                                let ctx = Context::ResourceContext(ResourceContext::Object(
                                    ApeResourcePermission {
                                        id: target_pid,
                                        level: PolicyLevels::WRITE,
                                        allow_sa: true,
                                    },
                                ));
                                self.authorizer
                                    .check_context(&token, ctx)
                                    .await
                                    .map_err(|e| {
                                        log::error!("{}", e);
                                        tonic::Status::unauthenticated("User not authenticated")
                                    })?;
                                (resource_id, target_pid)
                            }
                            _ => {
                                return Err(tonic::Status::internal("Direction conversion error."))
                            }
                        };
                        if let Some(ir) =
                            InternalRelation::get_by_pids(origin_pid, target_pid, client)
                                .await
                                .map_err(|e| {
                                    log::error!("{}", e);
                                    tonic::Status::aborted("Database transaction failed.")
                                })?
                        {
                            ir.delete(client).await.map_err(|e| {
                                log::error!("{}", e);
                                tonic::Status::aborted("Database transaction failed.")
                            })?;
                        };
                    }
                }
            }
        }
        Ok(tonic::Response::new(ModifyRelationsResponse {}))
    }
    async fn get_hierachy(
        &self,
        request: Request<GetHierachyRequest>,
    ) -> Result<Response<GetHierachyResponse>> {
        log::info!("Recieved GetHierarchyRequest.");
        log::debug!("{:?}", &request);
        let token = get_token_from_md(request.metadata()).map_err(|e| {
            log::debug!("{}", e);
            tonic::Status::unauthenticated("Token authentication error.")
        })?;
        let inner_request = request.into_inner();
        let resource_id = DieselUlid::from_str(&inner_request.resource_id).map_err(|e| {
            log::error!("{}", e);
            tonic::Status::internal("ULID conversion error")
        })?;
        let ctx = Context::ResourceContext(ResourceContext::Object(ApeResourcePermission {
            id: resource_id,
            level: PolicyLevels::READ,
            allow_sa: true,
        }));
        self.authorizer
            .check_context(&token, ctx)
            .await
            .map_err(|e| {
                log::error!("{}", e);
                tonic::Status::unauthenticated("User not authenticated")
            })?;

        let mut client = self.database.get_client().await.map_err(|e| {
            log::error!("{}", e);
            tonic::Status::unavailable("Database not avaliable.")
        })?;

        let transaction = client.transaction().await.map_err(|e| {
            log::error!("{}", e);
            tonic::Status::unavailable("Database not avaliable.")
        })?;

        let client = transaction.client();

        let resource = Object::get(resource_id, client)
            .await
            .map_err(|e| {
                log::error!("{}", e);
                tonic::Status::aborted("Database transaction failed.")
            })?
            .ok_or(tonic::Status::not_found("Resource not found."))?;
        let graph = if resource.object_type == ObjectType::DATASET {
            GetHierachyResponse {
                graph: Some(Graph::Dataset(
                    get_dataset_relations(resource_id, &client).await?,
                )),
            }
        } else if resource.object_type == ObjectType::COLLECTION {
            GetHierachyResponse {
                graph: Some(Graph::Collection(
                    get_collection_relations(resource_id, &client).await?,
                )),
            }
        } else {
            GetHierachyResponse {
                graph: Some(Graph::Project(
                    get_project_relations(resource_id, &client).await?,
                )),
            }
        };
        Ok(tonic::Response::new(graph))
    }
}

pub async fn get_dataset_relations(
    dataset: DieselUlid,
    client: &Client,
) -> Result<DatasetRelations> {
    let resources = InternalRelation::get_outbound_by_id(dataset, client)
        .await
        .map_err(|e| {
            log::error!("{}", e);
            tonic::Status::aborted("Database transaction failed.")
        })?;
    Ok(DatasetRelations {
        origin: dataset.to_string(),
        object_children: resources
            .into_iter()
            .map(|o| o.target_pid.to_string())
            .collect(),
    })
}
pub async fn get_collection_relations(
    collection: DieselUlid,
    client: &Client,
) -> Result<CollectionRelations> {
    let children = InternalRelation::get_outbound_by_id(collection, client)
        .await
        .map_err(|e| {
            log::error!("{}", e);
            tonic::Status::aborted("Database transaction failed.")
        })?;
    let datasets_ulid: Vec<DieselUlid> = children
        .clone()
        .into_iter()
        .filter(|d| d.target_type == ObjectType::DATASET)
        .map(|d| d.target_pid)
        .collect();
    let mut dataset_children: Vec<DatasetRelations> = Vec::new();
    for d in datasets_ulid {
        dataset_children.push(get_dataset_relations(d, client).await.map_err(|e| {
            log::error!("{}", e);
            tonic::Status::aborted("Database transaction failed.")
        })?);
    }
    let object_children: Vec<String> = children
        .into_iter()
        .filter(|o| o.target_type == ObjectType::OBJECT)
        .map(|o| o.target_pid.to_string())
        .collect();
    Ok(CollectionRelations {
        origin: collection.to_string(),
        dataset_children,
        object_children,
    })
}
pub async fn get_project_relations(
    project: DieselUlid,
    client: &Client,
) -> Result<ProjectRelations> {
    let children = InternalRelation::get_outbound_by_id(project, client)
        .await
        .map_err(|e| {
            log::error!("{}", e);
            tonic::Status::aborted("Database transaction failed.")
        })?;
    let collections_ulid: Vec<DieselUlid> = children
        .clone()
        .into_iter()
        .filter(|d| d.target_type == ObjectType::COLLECTION)
        .map(|d| d.target_pid)
        .collect();
    let datasets_ulid: Vec<DieselUlid> = children
        .clone()
        .into_iter()
        .filter(|d| d.target_type == ObjectType::DATASET)
        .map(|d| d.target_pid)
        .collect();
    let mut collection_children: Vec<CollectionRelations> = Vec::new();
    for c in collections_ulid {
        collection_children.push(get_collection_relations(c, client).await.map_err(|e| {
            log::error!("{}", e);
            tonic::Status::aborted("Database transaction failed.")
        })?);
    }
    let mut dataset_children: Vec<DatasetRelations> = Vec::new();
    for d in datasets_ulid {
        dataset_children.push(get_dataset_relations(d, client).await.map_err(|e| {
            log::error!("{}", e);
            tonic::Status::aborted("Database transaction failed.")
        })?);
    }
    let object_children: Vec<String> = children
        .into_iter()
        .filter(|o| o.target_type == ObjectType::OBJECT)
        .map(|o| o.target_pid.to_string())
        .collect();
    Ok(ProjectRelations {
        origin: project.to_string(),
        collection_children,
        dataset_children,
        object_children,
    })
}
