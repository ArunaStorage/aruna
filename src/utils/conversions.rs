use crate::database::dsls::internal_relation_dsl::InternalRelation;
use crate::database::dsls::internal_relation_dsl::{
    INTERNAL_RELATION_VARIANT_BELONGS_TO, INTERNAL_RELATION_VARIANT_METADATA,
    INTERNAL_RELATION_VARIANT_ORIGIN, INTERNAL_RELATION_VARIANT_POLICY,
    INTERNAL_RELATION_VARIANT_VERSION,
};
use crate::database::dsls::object_dsl::Object;
use crate::database::{
    dsls::object_dsl::{
        Algorithm, DefinedVariant, ExternalRelation as DBExternalRelation, ExternalRelations,
        Hash as DBHash, Hashes, KeyValue as DBKeyValue, KeyValueVariant, KeyValues,
        ObjectWithRelations,
    },
    enums::{DataClass, ObjectStatus, ObjectType},
};
use crate::middlelayer::update_handler::GRPCResource;

use anyhow::{anyhow, Result};
use aruna_rust_api::api::storage::models::v2::{
    relation::Relation as RelationEnum, Collection as GRPCCollection, Dataset as GRPCDataset,
    ExternalRelation, Hash, InternalRelation as APIInternalRelation, KeyValue,
    Object as GRPCObject, Project as GRPCProject, Relation, Stats,
};
use tonic::metadata::MetadataMap;

pub fn type_name_of<T>(_: T) -> &'static str {
    std::any::type_name::<T>()
}

pub fn get_token_from_md(md: &MetadataMap) -> Result<String> {
    let token_string = md
        .get("Authorization")
        .ok_or(anyhow!("Metadata token not found"))?
        .to_str()?;

    let splitted = token_string.split(' ').collect::<Vec<_>>();

    if splitted.len() != 2 {
        log::debug!(
            "Could not get token from metadata: Wrong length, expected: 2, got: {:?}",
            splitted.len()
        );
        return Err(anyhow!("Auhtorization flow error"));
    }

    if splitted[0] != "Bearer" {
        log::debug!(
            "Could not get token from metadata: Invalid Tokentype, expected: Bearer, got: {:?}",
            splitted[0]
        );

        return Err(anyhow!("Auhtorization flow error"));
    }

    if splitted[1].is_empty() {
        log::debug!(
            "Could not get token from metadata: Invalid Tokenlength, expected: >0, got: {:?}",
            splitted[1].len()
        );

        return Err(anyhow!("Auhtorization flow error"));
    }

    Ok(splitted[1].to_string())
}

impl TryFrom<Vec<KeyValue>> for KeyValues {
    type Error = anyhow::Error;
    fn try_from(key_val: Vec<KeyValue>) -> Result<Self> {
        let mut key_vals: Vec<DBKeyValue> = Vec::new();
        for kv in key_val {
            let kv = kv.try_into()?;
            key_vals.push(kv);
        }
        Ok(KeyValues(key_vals))
    }
}

impl TryFrom<KeyValue> for DBKeyValue {
    type Error = anyhow::Error;
    fn try_from(key_val: KeyValue) -> Result<Self> {
        Ok(DBKeyValue {
            key: key_val.key,
            value: key_val.value,
            variant: key_val.variant.try_into()?,
        })
    }
}

impl TryFrom<i32> for KeyValueVariant {
    type Error = anyhow::Error;
    fn try_from(var: i32) -> Result<Self> {
        match var {
            1 => Ok(KeyValueVariant::LABEL),
            2 => Ok(KeyValueVariant::STATIC_LABEL),
            3 => Ok(KeyValueVariant::HOOK),
            _ => Err(anyhow!("KeyValue variant not defined.")),
        }
    }
}

impl TryFrom<Vec<ExternalRelation>> for ExternalRelations {
    type Error = anyhow::Error;
    fn try_from(ex_rels: Vec<ExternalRelation>) -> Result<Self> {
        let mut relations: Vec<DBExternalRelation> = Vec::new();
        for r in ex_rels {
            let rs = r.try_into()?;
            relations.push(rs);
        }
        Ok(ExternalRelations(relations))
    }
}

impl TryFrom<ExternalRelation> for DBExternalRelation {
    type Error = anyhow::Error;
    fn try_from(ex_rel: ExternalRelation) -> Result<Self> {
        let (defined_variant, custom_variant) = match ex_rel.defined_variant {
            1 => (DefinedVariant::URL, None),
            2 => (DefinedVariant::IDENTIFIER, None),
            3 => (DefinedVariant::CUSTOM, ex_rel.custom_variant),
            _ => return Err(anyhow!("Relation variant not defined.")),
        };
        Ok(DBExternalRelation {
            identifier: ex_rel.identifier,
            defined_variant,
            custom_variant,
        })
    }
}

impl TryFrom<i32> for DataClass {
    type Error = anyhow::Error;
    fn try_from(var: i32) -> Result<Self> {
        match var {
            1 => Ok(DataClass::PUBLIC),
            2 => Ok(DataClass::PRIVATE),
            4 => Ok(DataClass::WORKSPACE),
            5 => Ok(DataClass::CONFIDENTIAL),
            _ => Err(anyhow!("Not defined.")),
        }
    }
}
impl From<DataClass> for i32 {
    fn from(var: DataClass) -> Self {
        match var {
            DataClass::PUBLIC => 1,
            DataClass::PRIVATE => 2,
            DataClass::WORKSPACE => 4,
            DataClass::CONFIDENTIAL => 5,
        }
    }
}
impl From<ObjectStatus> for i32 {
    fn from(var: ObjectStatus) -> Self {
        match var {
            ObjectStatus::INITIALIZING => 1,
            ObjectStatus::VALIDATING => 2,
            ObjectStatus::AVAILABLE => 3,
            ObjectStatus::UNAVAILABLE => 4,
            ObjectStatus::ERROR => 5,
            ObjectStatus::DELETED => 6,
        }
    }
}
impl From<KeyValues> for Vec<KeyValue> {
    fn from(keyval: KeyValues) -> Self {
        keyval
            .0
            .into_iter()
            .map(|kv| KeyValue {
                key: kv.key,
                value: kv.value,
                variant: match kv.variant {
                    KeyValueVariant::LABEL => 1,
                    KeyValueVariant::STATIC_LABEL => 2,
                    KeyValueVariant::HOOK => 3,
                },
            })
            .collect()
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

impl TryFrom<Vec<Hash>> for Hashes {
    type Error = anyhow::Error;
    fn try_from(h: Vec<Hash>) -> Result<Self> {
        let mut hashes = Vec::new();
        for h in h {
            hashes.push(DBHash {
                alg: h.alg.try_into()?,
                hash: h.hash,
            })
        }

        Ok(Hashes(hashes))
    }
}

impl TryFrom<i32> for Algorithm {
    type Error = anyhow::Error;
    fn try_from(a: i32) -> Result<Self> {
        match a {
            1 => Ok(Algorithm::MD5),
            2 => Ok(Algorithm::SHA256),
            _ => Err(anyhow!("Hash algorithm conversion error.")),
        }
    }
}

impl From<Hashes> for Vec<Hash> {
    fn from(hashes: Hashes) -> Self {
        hashes
            .0
            .into_iter()
            .map(|h| Hash {
                alg: match h.alg {
                    Algorithm::MD5 => 1,
                    Algorithm::SHA256 => 2,
                },
                hash: h.hash,
            })
            .collect()
    }
}

impl From<ObjectType> for i32 {
    fn from(object_type: ObjectType) -> Self {
        match object_type {
            ObjectType::PROJECT => 1,
            ObjectType::COLLECTION => 2,
            ObjectType::DATASET => 3,
            ObjectType::OBJECT => 4,
        }
    }
}

impl TryFrom<ObjectWithRelations> for GRPCObject {
    type Error = anyhow::Error;
    fn try_from(get_object: ObjectWithRelations) -> Result<Self> {
        let (to_relations, from_relations) = (
            get_object.inbound.0 .0,
            match get_object.outbound.0 .0.is_empty() {
                true => None,
                false => Some(get_object.outbound.0 .0),
            },
        );

        let mut from_relations = match from_relations {
            Some(r) => {
                let mut relations: Vec<Relation> = Vec::new();
                for relation in r.into_iter() {
                    relations.push(Relation {
                        relation: Some(RelationEnum::Internal(
                            from_db_internal_relation(
                                relation.clone(),
                                true,
                                relation.target_type.into(),
                            )
                            .map_err(|e| {
                                log::error!("{}", e);
                                tonic::Status::internal("Internal custom type conversion error.")
                            })?,
                        )),
                    });
                }
                relations
            }
            None => Vec::new(),
        };

        let mut to_relations_converted: Vec<Relation> = Vec::new();
        for relation in to_relations.into_iter() {
            to_relations_converted.push(Relation {
                relation: Some(RelationEnum::Internal(
                    from_db_internal_relation(relation.clone(), false, relation.origin_type.into())
                        .map_err(|e| {
                            log::error!("{}", e);
                            tonic::Status::internal("Internal custom type conversion error.")
                        })?,
                )),
            });
        }
        let mut relations: Vec<Relation> = get_object
            .object
            .external_relations
            .0
             .0
            .into_iter()
            .map(|r| Relation {
                relation: Some(RelationEnum::External(r.into())),
            })
            .collect();
        relations.append(&mut to_relations_converted);
        relations.append(&mut from_relations);

        Ok(GRPCObject {
            id: get_object.object.id.to_string(),
            content_len: get_object.object.content_len,
            name: get_object.object.name,
            description: get_object.object.description,
            created_at: get_object.object.created_at.map(|t| t.into()),
            created_by: get_object.object.created_by.to_string(),
            data_class: get_object.object.data_class.into(),
            dynamic: false,
            hashes: get_object.object.hashes.0.into(),
            key_values: get_object.object.key_values.0.into(),
            status: get_object.object.object_status.into(),
            relations,
        })
    }
}

impl TryFrom<ObjectWithRelations> for GRPCDataset {
    type Error = anyhow::Error;
    fn try_from(object_with_relations: ObjectWithRelations) -> Result<GRPCDataset> {
        let (to_relations, from_relations) = (
            object_with_relations.inbound.0 .0,
            match object_with_relations.outbound.0 .0.is_empty() {
                true => None,
                false => Some(object_with_relations.outbound.0 .0),
            },
        );

        let mut from_relations = match from_relations {
            Some(r) => {
                let mut relations: Vec<Relation> = Vec::new();
                for relation in r.into_iter() {
                    relations.push(Relation {
                        relation: Some(RelationEnum::Internal(
                            from_db_internal_relation(
                                relation.clone(),
                                true,
                                relation.target_type.into(),
                            )
                            .map_err(|e| {
                                log::error!("{}", e);
                                tonic::Status::internal("Internal custom type conversion error.")
                            })?,
                        )),
                    });
                }
                relations
            }
            None => Vec::new(),
        };

        let mut to_relations_converted: Vec<Relation> = Vec::new();
        for relation in to_relations.into_iter() {
            to_relations_converted.push(Relation {
                relation: Some(RelationEnum::Internal(
                    from_db_internal_relation(relation.clone(), false, relation.origin_type.into())
                        .map_err(|e| {
                            log::error!("{}", e);
                            tonic::Status::internal("Internal custom type conversion error.")
                        })?,
                )),
            });
        }
        let mut relations: Vec<Relation> = object_with_relations
            .object
            .external_relations
            .0
             .0
            .into_iter()
            .map(|r| Relation {
                relation: Some(RelationEnum::External(r.into())),
            })
            .collect();
        relations.append(&mut to_relations_converted);
        relations.append(&mut from_relations);
        let stats = Some(Stats {
            count: object_with_relations.object.count as i64,
            size: 0, // TODO
            last_updated: object_with_relations.object.created_at.map(|t| t.into()),
        });

        Ok(GRPCDataset {
            id: object_with_relations.object.id.to_string(),
            name: object_with_relations.object.name,
            description: object_with_relations.object.description,
            created_at: object_with_relations.object.created_at.map(|t| t.into()),
            stats,
            created_by: object_with_relations.object.created_by.to_string(),
            data_class: object_with_relations.object.data_class.into(),
            dynamic: false,
            key_values: object_with_relations.object.key_values.0.into(),
            status: object_with_relations.object.object_status.into(),
            relations,
        })
    }
}

impl TryFrom<ObjectWithRelations> for GRPCCollection {
    type Error = anyhow::Error;
    fn try_from(object_with_relations: ObjectWithRelations) -> Result<GRPCCollection> {
        let (to_relations, from_relations) = (
            object_with_relations.inbound.0 .0,
            match object_with_relations.outbound.0 .0.is_empty() {
                true => None,
                false => Some(object_with_relations.outbound.0 .0),
            },
        );

        let mut from_relations = match from_relations {
            Some(r) => {
                let mut relations: Vec<Relation> = Vec::new();
                for relation in r.into_iter() {
                    relations.push(Relation {
                        relation: Some(RelationEnum::Internal(
                            from_db_internal_relation(
                                relation.clone(),
                                true,
                                relation.target_type.into(),
                            )
                            .map_err(|e| {
                                log::error!("{}", e);
                                tonic::Status::internal("Internal custom type conversion error.")
                            })?,
                        )),
                    });
                }
                relations
            }
            None => Vec::new(),
        };

        let mut to_relations_converted: Vec<Relation> = Vec::new();
        for relation in to_relations.into_iter() {
            to_relations_converted.push(Relation {
                relation: Some(RelationEnum::Internal(
                    from_db_internal_relation(relation.clone(), false, relation.origin_type.into())
                        .map_err(|e| {
                            log::error!("{}", e);
                            tonic::Status::internal("Internal custom type conversion error.")
                        })?,
                )),
            });
        }
        let mut relations: Vec<Relation> = object_with_relations
            .object
            .external_relations
            .0
             .0
            .into_iter()
            .map(|r| Relation {
                relation: Some(RelationEnum::External(r.into())),
            })
            .collect();
        relations.append(&mut to_relations_converted);
        relations.append(&mut from_relations);
        let stats = Some(Stats {
            count: object_with_relations.object.count as i64,
            size: 0, // TODO
            last_updated: object_with_relations.object.created_at.map(|t| t.into()),
        });

        Ok(GRPCCollection {
            id: object_with_relations.object.id.to_string(),
            name: object_with_relations.object.name,
            description: object_with_relations.object.description,
            created_at: object_with_relations.object.created_at.map(|t| t.into()),
            stats,
            created_by: object_with_relations.object.created_by.to_string(),
            data_class: object_with_relations.object.data_class.into(),
            dynamic: false,
            key_values: object_with_relations.object.key_values.0.into(),
            status: object_with_relations.object.object_status.into(),
            relations,
        })
    }
}

impl TryFrom<ObjectWithRelations> for GRPCProject {
    type Error = anyhow::Error;
    fn try_from(object_with_relations: ObjectWithRelations) -> Result<GRPCProject> {
        let (to_relations, from_relations) = (
            object_with_relations.inbound.0 .0,
            match object_with_relations.outbound.0 .0.is_empty() {
                true => None,
                false => Some(object_with_relations.outbound.0 .0),
            },
        );

        let mut from_relations = match from_relations {
            Some(r) => {
                let mut relations: Vec<Relation> = Vec::new();
                for relation in r.into_iter() {
                    relations.push(Relation {
                        relation: Some(RelationEnum::Internal(
                            from_db_internal_relation(
                                relation.clone(),
                                true,
                                relation.target_type.into(),
                            )
                            .map_err(|e| {
                                log::error!("{}", e);
                                tonic::Status::internal("Internal custom type conversion error.")
                            })?,
                        )),
                    });
                }
                relations
            }
            None => Vec::new(),
        };

        let mut to_relations_converted: Vec<Relation> = Vec::new();
        for relation in to_relations.into_iter() {
            to_relations_converted.push(Relation {
                relation: Some(RelationEnum::Internal(
                    from_db_internal_relation(relation.clone(), false, relation.origin_type.into())
                        .map_err(|e| {
                            log::error!("{}", e);
                            tonic::Status::internal("Internal custom type conversion error.")
                        })?,
                )),
            });
        }
        let mut relations: Vec<Relation> = object_with_relations
            .object
            .external_relations
            .0
             .0
            .into_iter()
            .map(|r| Relation {
                relation: Some(RelationEnum::External(r.into())),
            })
            .collect();
        relations.append(&mut to_relations_converted);
        relations.append(&mut from_relations);
        let stats = Some(Stats {
            count: object_with_relations.object.count as i64,
            size: 0, // TODO
            last_updated: object_with_relations.object.created_at.map(|t| t.into()),
        });

        Ok(GRPCProject {
            id: object_with_relations.object.id.to_string(),
            name: object_with_relations.object.name,
            description: object_with_relations.object.description,
            created_at: object_with_relations.object.created_at.map(|t| t.into()),
            stats,
            created_by: object_with_relations.object.created_by.to_string(),
            data_class: object_with_relations.object.data_class.into(),
            dynamic: false,
            key_values: object_with_relations.object.key_values.0.into(),
            status: object_with_relations.object.object_status.into(),
            relations,
        })
    }
}
pub fn from_db_internal_relation(
    internal: InternalRelation,
    resource_is_origin: bool,
    resource_variant: i32,
) -> Result<APIInternalRelation> {
    let direction = if resource_is_origin { 1 } else { 2 };
    let (defined_variant, custom_variant) = match internal.type_name.as_str() {
        INTERNAL_RELATION_VARIANT_BELONGS_TO => (1, None),
        INTERNAL_RELATION_VARIANT_ORIGIN => (2, None),
        INTERNAL_RELATION_VARIANT_VERSION => (3, None),
        INTERNAL_RELATION_VARIANT_METADATA => (4, None),
        INTERNAL_RELATION_VARIANT_POLICY => (5, None),
        _ => (6, Some(internal.type_name)),
    };
    Ok(APIInternalRelation {
        resource_id: internal.origin_pid.to_string(),
        resource_variant,
        direction, // 1 for inbound, 2 for outbound
        defined_variant,
        custom_variant,
    })
}

pub fn from_db_object(internal: Option<InternalRelation>, object: Object) -> Result<GRPCResource> {
    let mut relations: Vec<Relation> = object
        .external_relations
        .0
         .0
        .into_iter()
        .map(|r| Relation {
            relation: Some(RelationEnum::External(r.into())),
        })
        .collect();
    let parent_relation = match internal {
        Some(i) => relations.push(Relation {
            relation: Some(RelationEnum::Internal(from_db_internal_relation(
                i,
                false,
                i.origin_type.try_into()?,
            )?)),
        }),
        None => (),
    };

    match object.object_type {
        ObjectType::PROJECT => Ok(GRPCResource::Project(GRPCProject {
            id: object.id.to_string(),
            name: object.name,
            description: object.description,
            key_values: object.key_values.0.into(),
            stats: None,
            relations,
            data_class: object.data_class.into(),
            created_at: None, // TODO
            created_by: object.created_by.to_string(),
            status: object.object_status.into(),
            dynamic: false,
        })),
        ObjectType::COLLECTION => Ok(GRPCResource::Collection(GRPCCollection {
            id: object.id.to_string(),
            name: object.name,
            description: object.description,
            key_values: object.key_values.0.into(),
            stats: None,
            relations,
            data_class: object.data_class.into(),
            created_at: None, // TODO
            created_by: object.created_by.to_string(),
            status: object.object_status.into(),
            dynamic: false,
        })),
        ObjectType::DATASET => Ok(GRPCResource::Dataset(GRPCDataset {
            id: object.id.to_string(),
            name: object.name,
            description: object.description,
            key_values: object.key_values.0.into(),
            stats: None,
            relations,
            data_class: object.data_class.into(),
            created_at: None, // TODO
            created_by: object.created_by.to_string(),
            status: object.object_status.into(),
            dynamic: false,
        })),
        ObjectType::OBJECT => Ok(GRPCResource::Object(GRPCObject {
            id: object.id.to_string(),
            name: object.name,
            description: object.description,
            key_values: object.key_values.0.into(),
            relations,
            content_len: object.content_len,
            data_class: object.data_class.into(),
            created_at: None, // TODO
            created_by: object.created_by.to_string(),
            status: object.object_status.into(),
            dynamic: false,
            hashes: object.hashes.0.into(),
        })),
    }
}
