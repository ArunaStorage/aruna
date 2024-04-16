use std::{str::FromStr};

use crate::{
    auth::structs::Context,
    database::{
        dsls::{
            internal_relation_dsl::{
                InternalRelation, INTERNAL_RELATION_VARIANT_BELONGS_TO,
                INTERNAL_RELATION_VARIANT_DELETED, INTERNAL_RELATION_VARIANT_METADATA,
                INTERNAL_RELATION_VARIANT_ORIGIN, INTERNAL_RELATION_VARIANT_POLICY,
                INTERNAL_RELATION_VARIANT_VERSION,
            },
            object_dsl::{
                DefinedVariant, ExternalRelation as DBExternalRelation, ExternalRelations,
            },
        },
        enums::{DbPermissionLevel, ObjectType},
    },
};
use ahash::RandomState;
use anyhow::{anyhow, bail, Result};
use aruna_rust_api::api::storage::models::v2::{
    relation::Relation as RelationEnum, ExternalRelation, InternalRelation as APIInternalRelation,
    InternalRelationVariant, Relation,
};
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use tokio_postgres::Client;
use crate::database::dsls::object_dsl::Object;

pub fn from_db_internal_relation(internal: InternalRelation, inbound: bool) -> Relation {
    let (direction, resource_variant, resource_id) = if inbound {
        (
            1,
            match internal.origin_type {
                ObjectType::PROJECT => 1,
                ObjectType::COLLECTION => 2,
                ObjectType::DATASET => 3,
                ObjectType::OBJECT => 4,
            },
            internal.origin_pid.to_string(),
        )
    } else {
        (
            2,
            match internal.target_type {
                ObjectType::PROJECT => 1,
                ObjectType::COLLECTION => 2,
                ObjectType::DATASET => 3,
                ObjectType::OBJECT => 4,
            },
            internal.target_pid.to_string(),
        )
    };
    let (defined_variant, custom_variant) = match internal.relation_name.as_str() {
        INTERNAL_RELATION_VARIANT_BELONGS_TO => (1, None),
        INTERNAL_RELATION_VARIANT_ORIGIN => (2, None),
        INTERNAL_RELATION_VARIANT_VERSION => (3, None),
        INTERNAL_RELATION_VARIANT_METADATA => (4, None),
        INTERNAL_RELATION_VARIANT_POLICY => (5, None),
        INTERNAL_RELATION_VARIANT_DELETED => (6, None),
        _ => (7, Some(internal.relation_name)),
    };

    Relation {
        relation: Some(RelationEnum::Internal(APIInternalRelation {
            resource_id,
            resource_variant,
            direction, // 1 for inbound, 2 for outbound
            defined_variant,
            custom_variant,
        })),
    }
}

impl TryFrom<&Vec<ExternalRelation>> for ExternalRelations {
    type Error = anyhow::Error;
    //noinspection ALL
    fn try_from(ex_rels: &Vec<ExternalRelation>) -> Result<Self> {
        let relations: DashMap<String, DBExternalRelation, RandomState> = DashMap::default();
        for r in ex_rels {
            let rs: DBExternalRelation = r.try_into()?;
            relations.insert(r.identifier.clone(), rs);
        }
        Ok(ExternalRelations(relations))
    }
}

impl TryFrom<&ExternalRelation> for DBExternalRelation {
    type Error = anyhow::Error;
    fn try_from(ex_rel: &ExternalRelation) -> Result<Self> {
        let (defined_variant, custom_variant) = match ex_rel.defined_variant {
            1 => (DefinedVariant::URL, None),
            2 => (DefinedVariant::IDENTIFIER, None),
            3 => (DefinedVariant::CUSTOM, ex_rel.custom_variant.clone()),
            _ => return Err(anyhow!("Relation variant not defined.")),
        };
        Ok(DBExternalRelation {
            identifier: ex_rel.identifier.to_string(),
            defined_variant,
            custom_variant,
        })
    }
}

impl From<DBExternalRelation> for ExternalRelation {
    fn from(r: DBExternalRelation) -> Self {
        let (defined_variant, custom_variant) = match r.defined_variant {
            DefinedVariant::CUSTOM => (3, r.custom_variant),
            DefinedVariant::IDENTIFIER => (2, None),
            DefinedVariant::URL => (1, None),
        };
        ExternalRelation {
            identifier: r.identifier,
            defined_variant,
            custom_variant,
        }
    }
}

impl InternalRelation {
    pub async fn from_api(
        api_rel: &APIInternalRelation,
        related: DieselUlid,
        transaction_client: &Client,
        // cache: Arc<Cache>,
    ) -> Result<Self> {
        match api_rel.direction() {
            aruna_rust_api::api::storage::models::v2::RelationDirection::Inbound => {
                let self_obj = Object::get_object_with_relations(&related, transaction_client).await?;
                    // .get_object(&related)
                    // .ok_or_else(|| anyhow!("self_obj not found"))?;
                let other_obj = Object::get_object_with_relations(&DieselUlid::from_str(&api_rel.resource_id)?, transaction_client).await?;
                 //    cache
                 //    .get_object(&DieselUlid::from_str(&api_rel.resource_id)?)
                 //    .ok_or_else(|| anyhow!("other_obj not found"))?;

                if self_obj.object.object_type == other_obj.object.object_type
                    && api_rel.defined_variant == InternalRelationVariant::BelongsTo as i32
                {
                    return Err(anyhow!(
                        "Can not assign BelongsTo relation to same-level hierarchy object"
                    ));
                }

                Ok(InternalRelation {
                    id: DieselUlid::generate(),
                    origin_pid: other_obj.object.id,
                    origin_type: other_obj.object.object_type,
                    relation_name: api_rel
                        .defined_variant
                        .as_relation_name(api_rel.custom_variant.clone())?,
                    target_pid: self_obj.object.id,
                    target_type: self_obj.object.object_type,
                    target_name: self_obj.object.name,
                })
            }
            aruna_rust_api::api::storage::models::v2::RelationDirection::Outbound => {
                let other_obj = Object::get_object_with_relations(&related, transaction_client).await?;
                //     cache
                //     .get_object(&related)
                //     .ok_or_else(|| anyhow!("self_obj not found"))?;
                let self_obj = Object::get_object_with_relations(&DieselUlid::from_str(&api_rel.resource_id)?, transaction_client).await?;
                //     cache
                //     .get_object(&DieselUlid::from_str(&api_rel.resource_id)?)
                //     .ok_or_else(|| anyhow!("other_obj not found"))?;

                if self_obj.object.object_type == other_obj.object.object_type
                    && api_rel.defined_variant == InternalRelationVariant::BelongsTo as i32
                {
                    return Err(anyhow!(
                        "Can not assign BelongsTo relation to same-level hierarchy object"
                    ));
                }

                Ok(InternalRelation {
                    id: DieselUlid::generate(),
                    origin_pid: other_obj.object.id,
                    origin_type: other_obj.object.object_type,
                    relation_name: api_rel
                        .defined_variant
                        .as_relation_name(api_rel.custom_variant.clone())?,
                    target_pid: self_obj.object.id,
                    target_type: self_obj.object.object_type,
                    target_name: self_obj.object.name,
                })
            }
            _ => bail!("Invalid direction"),
        }
    }
}

pub trait IntoRelationName {
    fn as_relation_name(&self, name: Option<String>) -> Result<String>;
}

impl IntoRelationName for i32 {
    fn as_relation_name(&self, name: Option<String>) -> Result<String> {
        match self {
            1 => Ok(INTERNAL_RELATION_VARIANT_BELONGS_TO.to_string()),
            2 => Ok(INTERNAL_RELATION_VARIANT_ORIGIN.to_string()),
            3 => Ok(INTERNAL_RELATION_VARIANT_VERSION.to_string()),
            4 => Ok(INTERNAL_RELATION_VARIANT_METADATA.to_string()),
            5 => Ok(INTERNAL_RELATION_VARIANT_POLICY.to_string()),
            6 => Ok(INTERNAL_RELATION_VARIANT_DELETED.to_string()),
            7 => Ok(name.ok_or_else(|| anyhow!("Custom relation variant not found"))?),
            _ => bail!("Invalid relation variant"),
        }
    }
}

pub struct ContextContainer(pub Vec<Context>);
impl TryFrom<Vec<Relation>> for ContextContainer {
    type Error = tonic::Status;

    fn try_from(relations: Vec<Relation>) -> Result<Self, tonic::Status> {
        let vec = relations
            .iter()
            .filter_map(|rel| match &rel.relation {
                Some(aruna_rust_api::api::storage::models::v2::relation::Relation::Internal(
                    internal_relation,
                )) => Some(internal_relation),
                _ => None,
            })
            .map(|ir| -> Result<Context, tonic::Status> {
                Ok(Context::res_ctx(
                    DieselUlid::from_str(&ir.resource_id).map_err(|e| {
                        log::error!("{e}");
                        tonic::Status::invalid_argument("Invalid relation id".to_string())
                    })?,
                    DbPermissionLevel::APPEND,
                    true,
                ))
            })
            .collect::<Result<Vec<Context>, tonic::Status>>()?;
        Ok(ContextContainer(vec))
    }
}
