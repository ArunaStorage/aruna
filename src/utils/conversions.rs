use crate::database::dsls::internal_relation_dsl::InternalRelation;
use crate::database::dsls::internal_relation_dsl::{
    INTERNAL_RELATION_VARIANT_BELONGS_TO, INTERNAL_RELATION_VARIANT_METADATA,
    INTERNAL_RELATION_VARIANT_ORIGIN, INTERNAL_RELATION_VARIANT_POLICY,
    INTERNAL_RELATION_VARIANT_VERSION,
};
use crate::database::dsls::object_dsl::Object;
use crate::database::dsls::user_dsl::{APIToken, User};
use crate::database::enums::{DbPermissionLevel, ObjectMapping};
use crate::database::{
    dsls::object_dsl::{
        Algorithm, DefinedVariant, ExternalRelation as DBExternalRelation, ExternalRelations,
        Hash as DBHash, Hashes, KeyValue as DBKeyValue, KeyValueVariant, KeyValues,
        ObjectWithRelations,
    },
    enums::{DataClass, ObjectStatus, ObjectType},
};
use crate::middlelayer::create_request_types::Parent;
use anyhow::{anyhow, Result};
use aruna_rust_api::api::storage::models::v2::permission::ResourceId;
use aruna_rust_api::api::storage::models::v2::{
    generic_resource, CustomAttributes, Permission, PermissionLevel, ResourceVariant, Status,
    Token, User as ApiUser, UserAttributes,
};
use aruna_rust_api::api::storage::models::v2::{
    relation::Relation as RelationEnum, Collection as GRPCCollection, Dataset as GRPCDataset,
    ExternalRelation, Hash, InternalRelation as APIInternalRelation, KeyValue,
    Object as GRPCObject, Project as GRPCProject, Relation, Stats,
};
use aruna_rust_api::api::storage::services::v2::{
    create_collection_request, create_dataset_request, create_object_request,
};
use diesel_ulid::DieselUlid;
use std::str::FromStr;
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
        return Err(anyhow!("Authorization flow error"));
    }

    if splitted[0] != "Bearer" {
        log::debug!(
            "Could not get token from metadata: Invalid Tokentype, expected: Bearer, got: {:?}",
            splitted[0]
        );

        return Err(anyhow!("Authorization flow error"));
    }

    if splitted[1].is_empty() {
        log::debug!(
            "Could not get token from metadata: Invalid Tokenlength, expected: >0, got: {:?}",
            splitted[1].len()
        );

        return Err(anyhow!("Authorization flow error"));
    }

    Ok(splitted[1].to_string())
}

impl TryFrom<&Vec<KeyValue>> for KeyValues {
    type Error = anyhow::Error;
    fn try_from(key_val: &Vec<KeyValue>) -> Result<Self> {
        let mut key_vals: Vec<DBKeyValue> = Vec::new();
        for kv in key_val {
            let kv = kv.try_into()?;
            key_vals.push(kv);
        }
        Ok(KeyValues(key_vals))
    }
}

impl TryFrom<&KeyValue> for DBKeyValue {
    type Error = anyhow::Error;
    fn try_from(key_val: &KeyValue) -> Result<Self> {
        Ok(DBKeyValue {
            key: key_val.key.clone(),
            value: key_val.value.clone(),
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

impl TryFrom<&Vec<ExternalRelation>> for ExternalRelations {
    type Error = anyhow::Error;
    fn try_from(ex_rels: &Vec<ExternalRelation>) -> Result<Self> {
        let mut relations: Vec<DBExternalRelation> = Vec::new();
        for r in ex_rels {
            let rs = r.try_into()?;
            relations.push(rs);
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
impl TryFrom<i32> for ObjectStatus {
    type Error = anyhow::Error;

    fn try_from(value: i32) -> std::result::Result<Self, Self::Error> {
        match value {
            1 => Ok(ObjectStatus::INITIALIZING),
            2 => Ok(ObjectStatus::VALIDATING),
            3 => Ok(ObjectStatus::AVAILABLE),
            4 => Ok(ObjectStatus::UNAVAILABLE),
            5 => Ok(ObjectStatus::ERROR),
            6 => Ok(ObjectStatus::DELETED),
            _ => Err(anyhow!("Object status not defined")),
        }
    }
}
impl From<ObjectStatus> for Status {
    fn from(val: ObjectStatus) -> Self {
        match val {
            ObjectStatus::INITIALIZING => Status::Initializing,
            ObjectStatus::VALIDATING => Status::Validating,
            ObjectStatus::AVAILABLE => Status::Available,
            ObjectStatus::UNAVAILABLE => Status::Unavailable,
            ObjectStatus::ERROR => Status::Error,
            ObjectStatus::DELETED => Status::Error,
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

impl From<ObjectType> for ResourceVariant {
    fn from(object_type: ObjectType) -> Self {
        match object_type {
            ObjectType::PROJECT => ResourceVariant::Project,
            ObjectType::COLLECTION => ResourceVariant::Collection,
            ObjectType::DATASET => ResourceVariant::Dataset,
            ObjectType::OBJECT => ResourceVariant::Object,
        }
    }
}

impl From<ObjectMapping<DieselUlid>> for ResourceId {
    fn from(value: ObjectMapping<DieselUlid>) -> Self {
        match value {
            ObjectMapping::PROJECT(id) => ResourceId::ProjectId(id.to_string()),
            ObjectMapping::COLLECTION(id) => ResourceId::CollectionId(id.to_string()),
            ObjectMapping::DATASET(id) => ResourceId::DatasetId(id.to_string()),
            ObjectMapping::OBJECT(id) => ResourceId::ObjectId(id.to_string()),
        }
    }
}

// Conversion from database model user token to proto user
impl From<User> for ApiUser {
    fn from(db_user: User) -> Self {
        // Convert and collect tokens
        let api_tokens = db_user
            .attributes
            .0
            .tokens
            .into_iter()
            .map(|(token_id, token)| convert_token_to_proto(&token_id, token))
            .collect::<Vec<_>>();

        // Collect custom attributes
        let api_custom_attributes = db_user
            .attributes
            .0
            .custom_attributes
            .into_iter()
            .map(|ca| CustomAttributes {
                attribute_name: ca.attribute_name,
                attribute_value: ca.attribute_value,
            })
            .collect::<Vec<_>>();

        // Collect personal permissions
        let api_permissions = db_user
            .attributes
            .0
            .permissions
            .into_iter()
            .map(|(resource_id, resource_mapping)| {
                convert_permission_to_proto(resource_id, resource_mapping)
            })
            .collect::<Vec<_>>();

        // Return proto user
        ApiUser {
            id: db_user.id.to_string(),
            external_id: db_user.external_id.unwrap_or_default(),
            display_name: db_user.display_name,
            active: db_user.active,
            email: db_user.email,
            attributes: Some(UserAttributes {
                global_admin: db_user.attributes.0.global_admin,
                service_account: db_user.attributes.0.service_account,
                tokens: api_tokens,
                custom_attributes: api_custom_attributes,
                personal_permissions: api_permissions,
            }),
        }
    }
}

// Conversion from database permission to proto permission
pub fn convert_permission_to_proto(
    resource_id: DieselUlid,
    resource_mapping: ObjectMapping<DbPermissionLevel>,
) -> Permission {
    match resource_mapping {
        ObjectMapping::PROJECT(perm) => Permission {
            permission_level: PermissionLevel::from(perm) as i32,
            resource_id: Some(ResourceId::ProjectId(resource_id.to_string())),
        },
        ObjectMapping::COLLECTION(perm) => Permission {
            permission_level: PermissionLevel::from(perm) as i32,
            resource_id: Some(ResourceId::CollectionId(resource_id.to_string())),
        },
        ObjectMapping::DATASET(perm) => Permission {
            permission_level: PermissionLevel::from(perm) as i32,
            resource_id: Some(ResourceId::DatasetId(resource_id.to_string())),
        },
        ObjectMapping::OBJECT(perm) => Permission {
            permission_level: PermissionLevel::from(perm) as i32,
            resource_id: Some(ResourceId::ObjectId(resource_id.to_string())),
        },
    }
}

// Conversion from database model token to proto token
pub fn convert_token_to_proto(token_id: &DieselUlid, db_token: APIToken) -> Token {
    Token {
        id: token_id.to_string(),
        name: db_token.name,
        created_at: Some(db_token.created_at.into()),
        expires_at: Some(db_token.expires_at.into()),
        permission: Some(Permission {
            permission_level: Into::<PermissionLevel>::into(db_token.user_rights) as i32,
            resource_id: db_token.object_id.map(ResourceId::from),
        }),
    }
}

// Conversion from database model permission level to proto permission level
impl From<DbPermissionLevel> for PermissionLevel {
    fn from(db_perm: DbPermissionLevel) -> Self {
        match db_perm {
            DbPermissionLevel::DENY => PermissionLevel::Unspecified, // Should not exist on db side
            DbPermissionLevel::NONE => PermissionLevel::None,
            DbPermissionLevel::READ => PermissionLevel::Read,
            DbPermissionLevel::APPEND => PermissionLevel::Append,
            DbPermissionLevel::WRITE => PermissionLevel::Write,
            DbPermissionLevel::ADMIN => PermissionLevel::Admin,
        }
    }
}

impl From<ObjectWithRelations> for generic_resource::Resource {
    fn from(object_with_relations: ObjectWithRelations) -> generic_resource::Resource {
        let (inbound, outbound) = (
            object_with_relations
                .inbound
                .0
                .iter()
                .chain(object_with_relations.inbound_belongs_to.0.iter())
                .map(|r| r.clone())
                .collect::<Vec<InternalRelation>>(),
            object_with_relations
                .outbound
                .0
                .iter()
                .chain(object_with_relations.outbound_belongs_to.0.iter())
                .map(|r| r.clone())
                .collect::<Vec<InternalRelation>>(),
        );

        let mut inbound = inbound
            .into_iter()
            .map(|r| from_db_internal_relation(r, true))
            .collect::<Vec<_>>();

        let mut outbound = outbound
            .into_iter()
            .map(|r| from_db_internal_relation(r, false))
            .collect::<Vec<_>>();
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
        relations.append(&mut inbound);
        relations.append(&mut outbound);
        let stats = Some(Stats {
            count: object_with_relations.object.count as i64,
            size: 0, // TODO
            last_updated: object_with_relations.object.created_at.map(|t| t.into()),
        });

        match object_with_relations.object.object_type {
            ObjectType::PROJECT => generic_resource::Resource::Project(GRPCProject {
                id: object_with_relations.object.id.to_string(),
                name: object_with_relations.object.name,
                description: object_with_relations.object.description,
                created_at: object_with_relations.object.created_at.map(|t| t.into()),
                stats,
                created_by: object_with_relations.object.created_by.to_string(),
                data_class: object_with_relations.object.data_class.into(),
                dynamic: object_with_relations.object.dynamic,
                key_values: object_with_relations.object.key_values.0.into(),
                status: object_with_relations.object.object_status.into(),
                relations,
            }),
            ObjectType::COLLECTION => generic_resource::Resource::Collection(GRPCCollection {
                id: object_with_relations.object.id.to_string(),
                name: object_with_relations.object.name,
                description: object_with_relations.object.description,
                created_at: object_with_relations.object.created_at.map(|t| t.into()),
                stats,
                created_by: object_with_relations.object.created_by.to_string(),
                data_class: object_with_relations.object.data_class.into(),
                dynamic: object_with_relations.object.dynamic,
                key_values: object_with_relations.object.key_values.0.into(),
                status: object_with_relations.object.object_status.into(),
                relations,
            }),
            ObjectType::DATASET => generic_resource::Resource::Dataset(GRPCDataset {
                id: object_with_relations.object.id.to_string(),
                name: object_with_relations.object.name,
                description: object_with_relations.object.description,
                created_at: object_with_relations.object.created_at.map(|t| t.into()),
                stats,
                created_by: object_with_relations.object.created_by.to_string(),
                data_class: object_with_relations.object.data_class.into(),
                dynamic: object_with_relations.object.dynamic,
                key_values: object_with_relations.object.key_values.0.into(),
                status: object_with_relations.object.object_status.into(),
                relations,
            }),
            ObjectType::OBJECT => generic_resource::Resource::Object(GRPCObject {
                id: object_with_relations.object.id.to_string(),
                content_len: object_with_relations.object.content_len,
                name: object_with_relations.object.name,
                description: object_with_relations.object.description,
                created_at: object_with_relations.object.created_at.map(|t| t.into()),
                created_by: object_with_relations.object.created_by.to_string(),
                data_class: object_with_relations.object.data_class.into(),
                dynamic: object_with_relations.object.dynamic,
                hashes: object_with_relations.object.hashes.0.into(),
                key_values: object_with_relations.object.key_values.0.into(),
                status: object_with_relations.object.object_status.into(),
                relations,
            }),
        }
    }
}

pub fn from_db_internal_relation(internal: InternalRelation, inbound: bool) -> Relation {
    let (direction, resource_variant) = if inbound {
        (
            1,
            match internal.origin_type {
                ObjectType::PROJECT => 1,
                ObjectType::COLLECTION => 2,
                ObjectType::DATASET => 3,
                ObjectType::OBJECT => 4,
            },
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
        )
    };
    let (defined_variant, custom_variant) = match internal.relation_name.as_str() {
        INTERNAL_RELATION_VARIANT_BELONGS_TO => (1, None),
        INTERNAL_RELATION_VARIANT_ORIGIN => (2, None),
        INTERNAL_RELATION_VARIANT_VERSION => (3, None),
        INTERNAL_RELATION_VARIANT_METADATA => (4, None),
        INTERNAL_RELATION_VARIANT_POLICY => (5, None),
        _ => (6, Some(internal.relation_name)),
    };

    Relation {
        relation: Some(RelationEnum::Internal(APIInternalRelation {
            resource_id: internal.origin_pid.to_string(),
            resource_variant,
            direction, // 1 for inbound, 2 for outbound
            defined_variant,
            custom_variant,
        })),
    }
}

pub fn from_db_object(
    internal: Option<InternalRelation>,
    object: Object,
) -> Result<generic_resource::Resource> {
    let mut relations: Vec<Relation> = object
        .external_relations
        .0
         .0
        .into_iter()
        .map(|r| Relation {
            relation: Some(RelationEnum::External(r.into())),
        })
        .collect();
    if let Some(i) = internal {
        relations.push(from_db_internal_relation(i.clone(), false))
    };

    match object.object_type {
        ObjectType::PROJECT => Ok(generic_resource::Resource::Project(GRPCProject {
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
            dynamic: object.dynamic,
        })),
        ObjectType::COLLECTION => Ok(generic_resource::Resource::Collection(GRPCCollection {
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
            dynamic: object.dynamic,
        })),
        ObjectType::DATASET => Ok(generic_resource::Resource::Dataset(GRPCDataset {
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
            dynamic: object.dynamic,
        })),
        ObjectType::OBJECT => Ok(generic_resource::Resource::Object(GRPCObject {
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
            dynamic: object.dynamic,
            hashes: object.hashes.0.into(),
        })),
    }
}

impl From<create_collection_request::Parent> for Parent {
    fn from(value: create_collection_request::Parent) -> Self {
        match value {
            create_collection_request::Parent::ProjectId(pid) => Parent::Project(pid),
        }
    }
}

impl From<create_dataset_request::Parent> for Parent {
    fn from(value: create_dataset_request::Parent) -> Self {
        match value {
            create_dataset_request::Parent::ProjectId(pid) => Parent::Project(pid),
            create_dataset_request::Parent::CollectionId(cid) => Parent::Collection(cid),
        }
    }
}

impl From<create_object_request::Parent> for Parent {
    fn from(value: create_object_request::Parent) -> Self {
        match value {
            create_object_request::Parent::ProjectId(pid) => Parent::Project(pid),
            create_object_request::Parent::CollectionId(cid) => Parent::Collection(cid),
            create_object_request::Parent::DatasetId(did) => Parent::Dataset(did),
        }
    }
}
// This looks stupid, but actually is really helpful when converting relations
impl TryFrom<(&APIInternalRelation, (DieselUlid, ObjectType))> for InternalRelation {
    type Error = anyhow::Error;
    fn try_from(
        internal: (&APIInternalRelation, (DieselUlid, ObjectType)),
    ) -> Result<InternalRelation> {
        let (internal, (object_id, object_type)) = internal;
        let (origin_pid, origin_type, target_pid, target_type) = match internal.direction {
            0 => return Err(anyhow!("Undefined direction")),
            1 => (
                DieselUlid::from_str(&internal.resource_id)?,
                internal.resource_variant.try_into()?,
                object_id,
                object_type,
            ),
            2 => (
                object_id,
                object_type,
                DieselUlid::from_str(&internal.resource_id)?,
                internal.resource_variant.try_into()?,
            ),

            _ => return Err(anyhow!("Internal relation direction conversion error")),
        };
        match internal.defined_variant {
            0 => Err(anyhow!("Undefined internal relation variant")),
            i if i > 0 && i < 6 => {
                let relation_name = match i {
                    1 => INTERNAL_RELATION_VARIANT_BELONGS_TO.to_string(),
                    2 => INTERNAL_RELATION_VARIANT_ORIGIN.to_string(),
                    3 => INTERNAL_RELATION_VARIANT_VERSION.to_string(),
                    4 => INTERNAL_RELATION_VARIANT_METADATA.to_string(),
                    5 => INTERNAL_RELATION_VARIANT_POLICY.to_string(),
                    _ => return Err(anyhow!("Undefined internal relation variant")),
                };
                Ok(InternalRelation {
                    id: DieselUlid::generate(),
                    origin_pid,
                    origin_type,
                    target_pid,
                    target_type,
                    is_persistent: false,
                    relation_name,
                })
            }
            6 => {
                let relation_name = internal
                    .clone()
                    .custom_variant
                    .ok_or_else(|| anyhow!("Custom relation variant not found"))?;
                Ok(InternalRelation {
                    id: DieselUlid::generate(),
                    origin_pid,
                    origin_type,
                    relation_name,
                    target_pid,
                    target_type,
                    is_persistent: false,
                })
            }
            _ => Err(anyhow!("Relation type not found")),
        }
    }
}
