use crate::auth::{Authorizer, Context, ResourcePermission};
use crate::caching::cache::Cache;
use crate::database::connection::Database;
use crate::database::crud::CrudDb;
use crate::database::internal_relation_dsl::InternalRelation;
use crate::database::object_dsl::{DefinedVariant, ExternalRelation, Object};
use crate::utils::conversions::get_token_from_md;
use aruna_rust_api::api::storage::models::v2::relation;
use aruna_rust_api::api::storage::services::v2::relations_service_server::RelationsService;
use aruna_rust_api::api::storage::services::v2::GetHierachyRequest;
use aruna_rust_api::api::storage::services::v2::GetHierachyResponse;
use aruna_rust_api::api::storage::services::v2::ModifyRelationsRequest;
use aruna_rust_api::api::storage::services::v2::ModifyRelationsResponse;
use diesel_ulid::DieselUlid;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;
use tonic::{Request, Response, Result};

crate::impl_grpc_server!(RelationsServiceImpl);

#[tonic::async_trait]
impl RelationsService for RelationsServiceImpl {
    async fn modify_relations(
        &self,
        request: Request<ModifyRelationsRequest>,
    ) -> Result<Response<ModifyRelationsResponse>> {
        log::info!("Recieved CreateObjectRequest.");
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
        let ctx = Context::Object(ResourcePermission {
            id: resource_id,
            level: crate::database::enums::PermissionLevels::WRITE,
            allow_sa: false,
        });
        match &self.authorizer.check_permissions(&token, ctx) {
            Ok(b) => {
                if *b {
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

        let resource = Object::get(resource_id, &client)
            .await
            .map_err(|e| {
                log::error!("{}", e);
                tonic::Status::unavailable("Database transaction failed.")
            })?
            .ok_or(tonic::Status::not_found("Resource not found"))?;
        let resource_type = resource.clone().object_type;
        for relation in inner_request.add_relations {
            match relation.relation {
                Some(rel) => match rel {
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
                        Object::add_external_relations(&resource_id, &client, external_relation)
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
                                let ctx = Context::Object(ResourcePermission {
                                    id: origin_pid,
                                    level: crate::database::enums::PermissionLevels::WRITE,
                                    allow_sa: false,
                                });
                                match &self.authorizer.check_permissions(&token, ctx) {
                                    Ok(b) => {
                                        if *b {
                                        } else {
                                            return Err(tonic::Status::permission_denied(
                                                "Not allowed.",
                                            ));
                                        }
                                    }
                                    Err(e) => {
                                        log::debug!("{}", e);
                                        return Err(tonic::Status::permission_denied(
                                            "Not allowed.",
                                        ));
                                    }
                                };
                                (
                                    origin_pid,
                                    // This is not optimal
                                    Object::get(origin_pid, &client)
                                        .await
                                        .map_err(|e| {
                                            log::error!("{}", e);
                                            tonic::Status::unavailable(
                                                "Database transaction failed.",
                                            )
                                        })?
                                        .ok_or(tonic::Status::not_found("Resource not found"))?
                                        .object_type,
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
                                let ctx = Context::Object(ResourcePermission {
                                    id: target_pid,
                                    level: crate::database::enums::PermissionLevels::WRITE,
                                    allow_sa: false,
                                });
                                match &self.authorizer.check_permissions(&token, ctx) {
                                    Ok(b) => {
                                        if *b {
                                        } else {
                                            return Err(tonic::Status::permission_denied(
                                                "Not allowed.",
                                            ));
                                        }
                                    }
                                    Err(e) => {
                                        log::debug!("{}", e);
                                        return Err(tonic::Status::permission_denied(
                                            "Not allowed.",
                                        ));
                                    }
                                };
                                (
                                    resource_id,
                                    resource_type.clone(),
                                    target_pid,
                                    // This is not optimal
                                    Object::get(target_pid, &client)
                                        .await
                                        .map_err(|e| {
                                            log::error!("{}", e);
                                            tonic::Status::unavailable(
                                                "Database transaction failed.",
                                            )
                                        })?
                                        .ok_or(tonic::Status::not_found("Resource not found"))?
                                        .object_type,
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
                            1 => InternalRelation {
                                id: DieselUlid::generate(),
                                origin_pid,
                                origin_type,
                                type_id: 1, // Belongs_to
                                target_pid,
                                target_type,
                                is_persistent: false,
                            },
                            2 => InternalRelation {
                                id: DieselUlid::generate(),
                                origin_pid,
                                origin_type,
                                type_id: 2, // Origin
                                target_pid,
                                target_type,
                                is_persistent: false,
                            },
                            3 => InternalRelation {
                                id: DieselUlid::generate(),
                                origin_pid,
                                origin_type,
                                type_id: 3, // Version
                                target_pid,
                                target_type,
                                is_persistent: false,
                            },
                            4 => InternalRelation {
                                id: DieselUlid::generate(),
                                origin_pid,
                                origin_type,
                                type_id: 4, // Metadata
                                target_pid,
                                target_type,
                                is_persistent: false,
                            },
                            5 => InternalRelation {
                                id: DieselUlid::generate(),
                                origin_pid,
                                origin_type,
                                type_id: 5, // Policy
                                target_pid,
                                target_type,
                                is_persistent: false,
                            },
                            6 => {
                                // Custom
                                // -> easiest, because directions must not be checked

                                InternalRelation {
                                    id: DieselUlid::generate(),
                                    origin_pid,
                                    origin_type,
                                    type_id: 6, // TODO: or custom entry in DB,
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
                        if InternalRelation::get_by_pids(origin_pid, target_pid, &client)
                            .await
                            .map_err(|e| {
                                log::error!("{}", e);
                                tonic::Status::aborted("Database transaction failed.")
                            })?
                            .is_some()
                        {
                            return Err(tonic::Status::aborted("Relation already exists"));
                        } else {
                            internal_relation.create(&client).await.map_err(|e| {
                                log::error!("{}", e);
                                tonic::Status::aborted("Database transaction failed.")
                            })?;
                        }
                    }
                },
                None => (),
            }
        }
        for relation in inner_request.remove_relations {
            match relation.relation {
                Some(rel) => match rel {
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
                        Object::remove_external_relation(&resource, &client, external_relation)
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
                                let ctx = Context::Object(ResourcePermission {
                                    id: origin_pid,
                                    level: crate::database::enums::PermissionLevels::WRITE,
                                    allow_sa: false,
                                });
                                match &self.authorizer.check_permissions(&token, ctx) {
                                    Ok(b) => {
                                        if *b {
                                        } else {
                                            return Err(tonic::Status::permission_denied(
                                                "Not allowed.",
                                            ));
                                        }
                                    }
                                    Err(e) => {
                                        log::debug!("{}", e);
                                        return Err(tonic::Status::permission_denied(
                                            "Not allowed.",
                                        ));
                                    }
                                };
                                (origin_pid, resource_id)
                            }
                            2 => {
                                let target_pid = DieselUlid::from_str(&internal.resource_id)
                                    .map_err(|e| {
                                        log::error!("{}", e);
                                        tonic::Status::internal("ULID conversion error.")
                                    })?;
                                let ctx = Context::Object(ResourcePermission {
                                    id: target_pid,
                                    level: crate::database::enums::PermissionLevels::WRITE,
                                    allow_sa: false,
                                });
                                match &self.authorizer.check_permissions(&token, ctx) {
                                    Ok(b) => {
                                        if *b {
                                        } else {
                                            return Err(tonic::Status::permission_denied(
                                                "Not allowed.",
                                            ));
                                        }
                                    }
                                    Err(e) => {
                                        log::debug!("{}", e);
                                        return Err(tonic::Status::permission_denied(
                                            "Not allowed.",
                                        ));
                                    }
                                };
                                (resource_id, target_pid)
                            }
                            _ => {
                                return Err(tonic::Status::internal("Direction conversion error."))
                            }
                        };
                        match InternalRelation::get_by_pids(origin_pid, target_pid, &client)
                            .await
                            .map_err(|e| {
                                log::error!("{}", e);
                                tonic::Status::aborted("Database transaction failed.")
                            })? {
                            Some(ir) => {
                                ir.delete(&client).await.map_err(|e| {
                                    log::error!("{}", e);
                                    tonic::Status::aborted("Database transaction failed.")
                                })?;
                            }
                            None => (),
                        };
                    }
                },
                None => (),
            }
        }
        Ok(tonic::Response::new(ModifyRelationsResponse {}))
    }
    async fn get_hierachy(
        &self,
        request: Request<GetHierachyRequest>,
    ) -> Result<Response<GetHierachyResponse>> {
        todo!()
    }
}
