use crate::caching::cache::Cache;
use crate::database::dsls::hook_dsl::{Hook, Method};
use crate::database::dsls::internal_relation_dsl::InternalRelation;
use crate::database::dsls::internal_relation_dsl::{
    INTERNAL_RELATION_VARIANT_BELONGS_TO, INTERNAL_RELATION_VARIANT_METADATA,
    INTERNAL_RELATION_VARIANT_ORIGIN, INTERNAL_RELATION_VARIANT_POLICY,
    INTERNAL_RELATION_VARIANT_VERSION,
};
use crate::database::dsls::license_dsl::License;
use crate::database::dsls::object_dsl::Object;
use crate::database::dsls::persistent_notification_dsl::{
    NotificationReference, PersistentNotification,
};
use crate::database::dsls::user_dsl::{
    APIToken, CustomAttributes as DBCustomAttributes, User as DBUser,
    UserAttributes as DBUserAttributes,
};
use crate::database::dsls::workspaces_dsl::WorkspaceTemplate;
use crate::database::enums::{
    DbPermissionLevel, EndpointVariant, NotificationReferenceType, ObjectMapping,
    PersistentNotificationVariant,
};
use crate::database::{
    dsls::endpoint_dsl::{Endpoint as DBEndpoint, HostConfig, HostConfigs},
    dsls::object_dsl::{
        Algorithm, DefinedVariant, ExternalRelation as DBExternalRelation, ExternalRelations,
        Hash as DBHash, Hashes, KeyValue as DBKeyValue, KeyValueVariant, KeyValues,
        ObjectWithRelations,
    },
    enums::{DataClass, DataProxyFeature, EndpointStatus, ObjectStatus, ObjectType},
};
use crate::middlelayer::create_request_types::Parent;
use ahash::RandomState;
use anyhow::{anyhow, bail, Result};
use aruna_rust_api::api::hooks::services::v2::hook::HookType;
use aruna_rust_api::api::hooks::services::v2::internal_hook::InternalAction;
use aruna_rust_api::api::hooks::services::v2::{
    AddHook, AddLabel, Credentials, ExternalHook, Hook as APIHook, HookInfo, InternalHook, Trigger,
};
use aruna_rust_api::api::storage::models::v2::{
    generic_resource, CustomAttributes, DataEndpoint, License as APILicense, Permission,
    PermissionLevel, ResourceVariant, Status, Token, User as ApiUser, UserAttributes,
};
use aruna_rust_api::api::storage::models::v2::{
    permission::ResourceId, relation::Relation as RelationEnum, Collection as GRPCCollection,
    Dataset as GRPCDataset, Endpoint, EndpointHostConfig, ExternalRelation, Hash,
    InternalRelation as APIInternalRelation, KeyValue, Object as GRPCObject,
    Project as GRPCProject, Relation, Stats, User,
};
use aruna_rust_api::api::storage::services::v2::{
    create_collection_request, create_dataset_request, create_object_request, CreateLicenseRequest,
    PersonalNotification, PersonalNotificationVariant, Reference, ReferenceType, WorkspaceInfo,
};
use dashmap::DashMap;
use diesel_ulid::DieselUlid;

use std::str::FromStr;
use std::sync::Arc;
use tonic::metadata::MetadataMap;

pub fn type_name_of<T>(_: T) -> &'static str {
    std::any::type_name::<T>()
}

pub fn get_token_from_md(md: &MetadataMap) -> Result<String> {
    let token_string = md
        .get("Authorization")
        .ok_or(anyhow!("Metadata token not found"))?
        .to_str()?;

    let split = token_string.split(' ').collect::<Vec<_>>();

    if split.len() != 2 {
        log::debug!(
            "Could not get token from metadata: Wrong length, expected: 2, got: {:?}",
            split.len()
        );
        return Err(anyhow!("Authorization flow error"));
    }

    if split[0] != "Bearer" {
        log::debug!(
            "Could not get token from metadata: Invalid token type, expected: Bearer, got: {:?}",
            split[0]
        );

        return Err(anyhow!("Authorization flow error"));
    }

    if split[1].is_empty() {
        log::debug!(
            "Could not get token from metadata: Invalid token length, expected: >0, got: {:?}",
            split[1].len()
        );

        return Err(anyhow!("Authorization flow error"));
    }

    Ok(split[1].to_string())
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
            4 => Err(anyhow!(
                "Can't create HookStatus without outside of hook callbacks"
            )),
            _ => Err(anyhow!("KeyValue variant not defined.")),
        }
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

impl TryFrom<i32> for DataClass {
    type Error = anyhow::Error;
    fn try_from(var: i32) -> Result<Self> {
        match var {
            1 => Ok(DataClass::PUBLIC),
            2 => Ok(DataClass::PRIVATE),
            4 => Ok(DataClass::WORKSPACE),
            5 => Ok(DataClass::CONFIDENTIAL),
            _ => Err(anyhow!("Dataclass not defined")),
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
    //noinspection ALL
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
                    KeyValueVariant::HOOK_STATUS => 4,
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

impl TryFrom<ResourceVariant> for ObjectType {
    type Error = anyhow::Error;

    fn try_from(value: ResourceVariant) -> std::result::Result<Self, Self::Error> {
        Ok(match value {
            ResourceVariant::Unspecified => bail!("Unspecified resource variant not allowed"),
            ResourceVariant::Project => ObjectType::PROJECT,
            ResourceVariant::Collection => ObjectType::COLLECTION,
            ResourceVariant::Dataset => ObjectType::DATASET,
            ResourceVariant::Object => ObjectType::OBJECT,
        })
    }
}

impl TryFrom<ResourceId> for ObjectMapping<DieselUlid> {
    type Error = anyhow::Error;

    fn try_from(value: ResourceId) -> Result<Self> {
        Ok(match value {
            ResourceId::ProjectId(id) => ObjectMapping::PROJECT(DieselUlid::from_str(&id)?),
            ResourceId::CollectionId(id) => ObjectMapping::COLLECTION(DieselUlid::from_str(&id)?),
            ResourceId::DatasetId(id) => ObjectMapping::DATASET(DieselUlid::from_str(&id)?),
            ResourceId::ObjectId(id) => ObjectMapping::OBJECT(DieselUlid::from_str(&id)?),
        })
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
impl From<DBUser> for ApiUser {
    fn from(db_user: DBUser) -> Self {
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
                trusted_endpoints: db_user
                    .attributes
                    .0
                    .trusted_endpoints
                    .iter()
                    .map(|e| e.key().to_string())
                    .collect(),
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
impl TryFrom<PermissionLevel> for DbPermissionLevel {
    type Error = anyhow::Error;

    fn try_from(value: PermissionLevel) -> std::result::Result<Self, Self::Error> {
        Ok(match value {
            PermissionLevel::Unspecified => {
                return Err(anyhow::anyhow!("Unspecified permission level"))
            }
            PermissionLevel::None => DbPermissionLevel::NONE,
            PermissionLevel::Read => DbPermissionLevel::READ,
            PermissionLevel::Append => DbPermissionLevel::APPEND,
            PermissionLevel::Write => DbPermissionLevel::WRITE,
            PermissionLevel::Admin => DbPermissionLevel::ADMIN,
        })
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
                relation: Some(RelationEnum::External(r.1.into())),
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
                endpoints: object_with_relations
                    .object
                    .endpoints
                    .0
                    .iter()
                    .map(|e| DataEndpoint {
                        id: e.key().to_string(),
                        full_synced: *e.value(),
                    })
                    .collect(),
                metadata_license_tag: object_with_relations.object.metadata_license,
                default_data_license_tag: object_with_relations.object.data_license,
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
                endpoints: object_with_relations
                    .object
                    .endpoints
                    .0
                    .iter()
                    .map(|e| DataEndpoint {
                        id: e.key().to_string(),
                        full_synced: *e.value(),
                    })
                    .collect(),
                metadata_license_tag: object_with_relations.object.metadata_license,
                default_data_license_tag: object_with_relations.object.data_license,
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
                endpoints: object_with_relations
                    .object
                    .endpoints
                    .0
                    .iter()
                    .map(|e| DataEndpoint {
                        id: e.key().to_string(),
                        full_synced: *e.value(),
                    })
                    .collect(),
                metadata_license_tag: object_with_relations.object.metadata_license,
                default_data_license_tag: object_with_relations.object.data_license,
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
                endpoints: object_with_relations
                    .object
                    .endpoints
                    .0
                    .iter()
                    .map(|e| DataEndpoint {
                        id: e.key().to_string(),
                        full_synced: *e.value(),
                    })
                    .collect(),
                metadata_license_tag: object_with_relations.object.metadata_license,
                data_license_tag: object_with_relations.object.data_license,
            }),
        }
    }
}

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
        _ => (6, Some(internal.relation_name)),
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
            relation: Some(RelationEnum::External(r.1.into())),
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
            created_at: object.created_at.map(|t| t.into()),
            created_by: object.created_by.to_string(),
            status: object.object_status.into(),
            dynamic: object.dynamic,
            endpoints: object
                .endpoints
                .0
                .iter()
                .map(|e| DataEndpoint {
                    id: e.key().to_string(),
                    full_synced: *e.value(),
                })
                .collect(),
            metadata_license_tag: object.metadata_license,
            default_data_license_tag: object.data_license,
        })),
        ObjectType::COLLECTION => Ok(generic_resource::Resource::Collection(GRPCCollection {
            id: object.id.to_string(),
            name: object.name,
            description: object.description,
            key_values: object.key_values.0.into(),
            stats: None,
            relations,
            data_class: object.data_class.into(),
            created_at: object.created_at.map(|t| t.into()),
            created_by: object.created_by.to_string(),
            status: object.object_status.into(),
            dynamic: object.dynamic,
            endpoints: object
                .endpoints
                .0
                .iter()
                .map(|e| DataEndpoint {
                    id: e.key().to_string(),
                    full_synced: *e.value(),
                })
                .collect(),
            metadata_license_tag: object.metadata_license,
            default_data_license_tag: object.data_license,
        })),
        ObjectType::DATASET => Ok(generic_resource::Resource::Dataset(GRPCDataset {
            id: object.id.to_string(),
            name: object.name,
            description: object.description,
            key_values: object.key_values.0.into(),
            stats: None,
            relations,
            data_class: object.data_class.into(),
            created_at: object.created_at.map(|t| t.into()),
            created_by: object.created_by.to_string(),
            status: object.object_status.into(),
            dynamic: object.dynamic,
            endpoints: object
                .endpoints
                .0
                .iter()
                .map(|e| DataEndpoint {
                    id: e.key().to_string(),
                    full_synced: *e.value(),
                })
                .collect(),
            metadata_license_tag: object.metadata_license,
            default_data_license_tag: object.data_license,
        })),
        ObjectType::OBJECT => Ok(generic_resource::Resource::Object(GRPCObject {
            id: object.id.to_string(),
            name: object.name,
            description: object.description,
            key_values: object.key_values.0.into(),
            relations,
            content_len: object.content_len,
            data_class: object.data_class.into(),
            created_at: object.created_at.map(|t| t.into()),
            created_by: object.created_by.to_string(),
            status: object.object_status.into(),
            dynamic: object.dynamic,
            hashes: object.hashes.0.into(),
            endpoints: object
                .endpoints
                .0
                .iter()
                .map(|e| DataEndpoint {
                    id: e.key().to_string(),
                    full_synced: *e.value(),
                })
                .collect(),
            metadata_license_tag: object.metadata_license,
            data_license_tag: object.data_license,
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

impl InternalRelation {
    pub fn from_api(
        api_rel: &APIInternalRelation,
        related: DieselUlid,
        cache: Arc<Cache>,
    ) -> Result<Self> {
        match api_rel.direction() {
            aruna_rust_api::api::storage::models::v2::RelationDirection::Inbound => {
                let self_obj = cache
                    .get_object(&related)
                    .ok_or_else(|| anyhow!("self_obj not found"))?;
                let other_obj = cache
                    .get_object(&DieselUlid::from_str(&api_rel.resource_id)?)
                    .ok_or_else(|| anyhow!("other_obj not found"))?;

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
                let other_obj = cache
                    .get_object(&related)
                    .ok_or_else(|| anyhow!("self_obj not found"))?;
                let self_obj = cache
                    .get_object(&DieselUlid::from_str(&api_rel.resource_id)?)
                    .ok_or_else(|| anyhow!("other_obj not found"))?;

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

// // This looks stupid, but is actually really helpful when converting relations
// impl TryFrom<(&APIInternalRelation, (DieselUlid, ObjectType, String))> for InternalRelation {
//     type Error = anyhow::Error;
//     fn try_from(
//         internal: (&APIInternalRelation, (DieselUlid, ObjectType)),
//     ) -> Result<InternalRelation> {
//         let (internal, (object_id, object_type)) = internal;
//         let (origin_pid, origin_type, target_pid, target_type) = match internal.direction {
//             0 => return Err(anyhow!("Undefined direction")),
//             1 => (
//                 DieselUlid::from_str(&internal.resource_id)?,
//                 internal.resource_variant.try_into()?,
//                 object_id,
//                 object_type,
//             ),
//             2 => (
//                 object_id,
//                 object_type,
//                 DieselUlid::from_str(&internal.resource_id)?,
//                 internal.resource_variant.try_into()?,
//             ),

//             _ => return Err(anyhow!("Internal relation direction conversion error")),
//         };
//         match internal.defined_variant {
//             0 => Err(anyhow!("Undefined internal relation variant")),
//             i if i > 0 && i < 6 => {
//                 let relation_name = match i {
//                     1 => INTERNAL_RELATION_VARIANT_BELONGS_TO.to_string(),
//                     2 => INTERNAL_RELATION_VARIANT_ORIGIN.to_string(),
//                     3 => INTERNAL_RELATION_VARIANT_VERSION.to_string(),
//                     4 => INTERNAL_RELATION_VARIANT_METADATA.to_string(),
//                     5 => INTERNAL_RELATION_VARIANT_POLICY.to_string(),
//                     _ => return Err(anyhow!("Undefined internal relation variant")),
//                 };
//                 Ok(InternalRelation {
//                     id: DieselUlid::generate(),
//                     origin_pid,
//                     origin_type,
//                     target_pid,
//                     target_type,
//                     relation_name,
//                     target_name: "".to_string(),
//                 })
//             }
//             6 => {
//                 let relation_name = internal
//                     .clone()
//                     .custom_variant
//                     .ok_or_else(|| anyhow!("Custom relation variant not found"))?;
//                 Ok(InternalRelation {
//                     id: DieselUlid::generate(),
//                     origin_pid,
//                     origin_type,
//                     relation_name,
//                     target_pid,
//                     target_type,
//                     target_name: "".to_string(),
//                 })
//             }
//             _ => Err(anyhow!("Relation type not found")),
//         }
//     }
// }

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
            6 => Ok(name.ok_or_else(|| anyhow!("Custom relation variant not found"))?),
            _ => bail!("Invalid relation variant"),
        }
    }
}

/*
impl From<DBUser> for DBUser {
    fn from(user: DBUser) -> Self {
        ApiUser {
            id: user.id.to_string(),
            external_id: match user.external_id {
                Some(id) => id,
                None => String::new(),
            },
            display_name: user.display_name,
            active: user.active,
            email: user.email,
            attributes: Some(user.attributes.0.into()),
        }
    }
}
*/

impl From<DBUserAttributes> for UserAttributes {
    fn from(attr: DBUserAttributes) -> Self {
        let (tokens, personal_permissions): (Vec<Token>, Vec<Permission>) = attr
            .tokens
            .into_iter()
            .map(|t| {
                (
                    Token {
                        id: t.0.to_string(),
                        name: t.1.name,
                        created_at: Some(t.1.created_at.into()),
                        expires_at: Some(t.1.expires_at.into()),
                        permission: Some(Permission {
                            permission_level: t.1.user_rights.into(),
                            resource_id: t.1.object_id.map(|resource| match resource {
                                ObjectMapping::PROJECT(id) => ResourceId::ProjectId(id.to_string()),
                                ObjectMapping::COLLECTION(id) => {
                                    ResourceId::CollectionId(id.to_string())
                                }
                                ObjectMapping::DATASET(id) => ResourceId::DatasetId(id.to_string()),
                                ObjectMapping::OBJECT(id) => ResourceId::ObjectId(id.to_string()),
                            }),
                        }),
                    },
                    Permission {
                        permission_level: t.1.user_rights.into(),
                        resource_id: t.1.object_id.map(|resource| match resource {
                            ObjectMapping::PROJECT(id) => ResourceId::ProjectId(id.to_string()),
                            ObjectMapping::COLLECTION(id) => {
                                ResourceId::CollectionId(id.to_string())
                            }
                            ObjectMapping::DATASET(id) => ResourceId::DatasetId(id.to_string()),
                            ObjectMapping::OBJECT(id) => ResourceId::ObjectId(id.to_string()),
                        }),
                    },
                )
            })
            .unzip();
        UserAttributes {
            global_admin: attr.global_admin,
            service_account: attr.service_account,
            tokens,
            custom_attributes: attr
                .custom_attributes
                .into_iter()
                .map(|c| c.into())
                .collect(),
            personal_permissions,
            trusted_endpoints: attr
                .trusted_endpoints
                .iter()
                .map(|e| e.key().to_string())
                .collect(),
        }
    }
}

impl From<DBCustomAttributes> for CustomAttributes {
    fn from(attr: DBCustomAttributes) -> Self {
        CustomAttributes {
            attribute_name: attr.attribute_name,
            attribute_value: attr.attribute_value,
        }
    }
}

impl From<DbPermissionLevel> for i32 {
    fn from(lvl: DbPermissionLevel) -> Self {
        match lvl {
            DbPermissionLevel::DENY => 1, //TODO: Currently reserved and not used
            DbPermissionLevel::NONE => 2,
            DbPermissionLevel::READ => 3,
            DbPermissionLevel::APPEND => 4,
            DbPermissionLevel::WRITE => 5,
            DbPermissionLevel::ADMIN => 6,
        }
    }
}

impl DBUser {
    pub fn into_redacted(self) -> User {
        let mut user: User = self.into();
        user.email = String::new();
        user.display_name = String::new();
        user.external_id = String::new();
        user
    }
}

pub fn as_api_token(id: DieselUlid, token: APIToken) -> Token {
    Token {
        id: id.to_string(),
        name: token.name,
        created_at: Some(token.created_at.into()),
        expires_at: Some(token.expires_at.into()),
        permission: Some(Permission {
            permission_level: token.user_rights.into(),
            resource_id: token.object_id.map(|resource| match resource {
                ObjectMapping::PROJECT(id) => ResourceId::ProjectId(id.to_string()),
                ObjectMapping::COLLECTION(id) => ResourceId::CollectionId(id.to_string()),
                ObjectMapping::DATASET(id) => ResourceId::DatasetId(id.to_string()),
                ObjectMapping::OBJECT(id) => ResourceId::ObjectId(id.to_string()),
            }),
        }),
    }
}

impl TryFrom<i32> for EndpointStatus {
    type Error = anyhow::Error;
    fn try_from(value: i32) -> Result<Self> {
        let res = match value {
            1 => EndpointStatus::INITIALIZING,
            2 => EndpointStatus::AVAILABLE,
            3 => EndpointStatus::DEGRADED,
            4 => EndpointStatus::UNAVAILABLE,
            5 => EndpointStatus::MAINTENANCE,
            _ => return Err(anyhow!("Undefined component status")),
        };
        Ok(res)
    }
}

impl TryFrom<Vec<EndpointHostConfig>> for HostConfigs {
    type Error = anyhow::Error;
    fn try_from(config: Vec<EndpointHostConfig>) -> Result<Self> {
        let res: Result<Vec<HostConfig>> = config
            .into_iter()
            .map(|c| -> Result<HostConfig> {
                Ok(HostConfig {
                    url: c.url,
                    is_primary: c.is_primary,
                    ssl: c.ssl,
                    public: c.public,
                    feature: c.host_variant.try_into()?,
                })
            })
            .collect();
        Ok(HostConfigs(res?))
    }
}

impl TryFrom<i32> for DataProxyFeature {
    type Error = anyhow::Error;
    fn try_from(var: i32) -> Result<DataProxyFeature> {
        let res = match var {
            1 => DataProxyFeature::GRPC,
            2 => DataProxyFeature::S3,
            _ => return Err(anyhow!("Undefined dataproxy feature")),
        };
        Ok(res)
    }
}

impl From<DBEndpoint> for Endpoint {
    fn from(ep: DBEndpoint) -> Self {
        Endpoint {
            id: ep.id.to_string(),
            ep_variant: ep.endpoint_variant.into(),
            name: ep.name,
            is_public: ep.is_public,
            status: ep.status.into(),
            host_configs: ep.host_config.0.into(),
        }
    }
}

impl From<EndpointVariant> for i32 {
    fn from(var: EndpointVariant) -> Self {
        match var {
            EndpointVariant::PERSISTENT => 1,
            EndpointVariant::VOLATILE => 2,
        }
    }
}

impl From<EndpointStatus> for i32 {
    fn from(status: EndpointStatus) -> Self {
        match status {
            EndpointStatus::INITIALIZING => 1,
            EndpointStatus::AVAILABLE => 2,
            EndpointStatus::DEGRADED => 3,
            EndpointStatus::UNAVAILABLE => 4,
            EndpointStatus::MAINTENANCE => 5,
        }
    }
}

impl From<HostConfigs> for Vec<EndpointHostConfig> {
    fn from(config: HostConfigs) -> Self {
        config
            .0
            .into_iter()
            .map(|c| EndpointHostConfig {
                url: c.url,
                is_primary: c.is_primary,
                ssl: c.ssl,
                public: c.public,
                host_variant: c.feature.into(),
            })
            .collect()
    }
}

impl From<DataProxyFeature> for i32 {
    fn from(feat: DataProxyFeature) -> Self {
        match feat {
            DataProxyFeature::GRPC => 1,
            DataProxyFeature::S3 => 2,
        }
    }
}

impl TryFrom<i32> for EndpointVariant {
    type Error = anyhow::Error;
    fn try_from(value: i32) -> Result<Self> {
        Ok(match value {
            1 => EndpointVariant::PERSISTENT,
            2 => EndpointVariant::VOLATILE,
            _ => return Err(anyhow!("Undefined endpoint variant")),
        })
    }
}

impl From<Hook> for HookInfo {
    fn from(hook: Hook) -> HookInfo {
        let trigger = Some(hook.as_trigger());
        HookInfo {
            name: hook.name.clone(),
            description: hook.description.clone(),
            hook_id: hook.id.to_string(),
            hook: Some(hook.as_api_hook()),
            trigger,
            timeout: hook.timeout.timestamp_millis() as u64,
            project_ids: hook.project_ids.iter().map(|id| id.to_string()).collect(),
        }
    }
}

impl From<&Method> for i32 {
    fn from(method: &Method) -> Self {
        match method {
            Method::PUT => 1,
            Method::POST => 2,
        }
    }
}
impl Hook {
    fn as_trigger(&self) -> Trigger {
        Trigger {
            trigger_type: match self.trigger_type {
                crate::database::dsls::hook_dsl::TriggerType::HOOK_ADDED => 1,
                crate::database::dsls::hook_dsl::TriggerType::OBJECT_CREATED => 2,
            },
            key: self.trigger_key.clone(),
            value: self.trigger_value.clone(),
        }
    }
    fn as_api_hook(&self) -> APIHook {
        match &self.hook.0 {
            crate::database::dsls::hook_dsl::HookVariant::Internal(internal_hook) => {
                let internal_action = match internal_hook {
                    crate::database::dsls::hook_dsl::InternalHook::AddLabel { key, value } => {
                        InternalAction::AddLabel(AddLabel {
                            key: key.clone(),
                            value: value.clone(),
                        })
                    }
                    crate::database::dsls::hook_dsl::InternalHook::AddHook { key, value } => {
                        InternalAction::AddHook(AddHook {
                            key: key.clone(),
                            value: value.clone(),
                        })
                    }

                    crate::database::dsls::hook_dsl::InternalHook::CreateRelation { relation } => {
                        InternalAction::AddRelation(Relation {
                            relation: Some(relation.clone()),
                        })
                    }
                };
                APIHook {
                    hook_type: Some(HookType::InternalHook(InternalHook {
                        internal_action: Some(internal_action),
                    })),
                }
            }
            crate::database::dsls::hook_dsl::HookVariant::External(external_hook) => {
                let custom_template = match &external_hook.template {
                    crate::database::dsls::hook_dsl::TemplateVariant::Basic => None,
                    crate::database::dsls::hook_dsl::TemplateVariant::Custom(string) => {
                        Some(string.clone())
                    }
                };
                APIHook {
                    hook_type: Some(HookType::ExternalHook(ExternalHook {
                        url: external_hook.url.clone(),
                        credentials: external_hook
                            .credentials
                            .clone()
                            .map(|c| Credentials { token: c.token }),
                        custom_template,
                        method: (&external_hook.method).into(),
                    })),
                }
            }
        }
    }
}
impl From<WorkspaceTemplate> for WorkspaceInfo {
    fn from(ws: WorkspaceTemplate) -> Self {
        WorkspaceInfo {
            workspace_id: ws.id.to_string(),
            name: ws.name,
            description: ws.description,
            owner: ws.owner.to_string(),
            prefix: ws.prefix,
            hook_ids: ws.hook_ids.0.iter().map(DieselUlid::to_string).collect(),
            endpoint_ids: ws
                .endpoint_ids
                .0
                .iter()
                .map(DieselUlid::to_string)
                .collect(),
        }
    }
}

// -------------------------------------------------- //
// ----- Personal/Persistant Notifications ---------- //
// -------------------------------------------------- //
impl From<PersistentNotification> for PersonalNotification {
    fn from(value: PersistentNotification) -> Self {
        let variant: PersonalNotificationVariant = value.notification_variant.into();
        let refs = value
            .refs
            .0
             .0
            .into_iter()
            .map(|r| r.into())
            .collect::<Vec<_>>();

        PersonalNotification {
            id: value.id.to_string(),
            variant: variant as i32,
            message: value.message,
            refs,
        }
    }
}

impl From<PersistentNotificationVariant> for PersonalNotificationVariant {
    fn from(value: PersistentNotificationVariant) -> Self {
        match value {
            PersistentNotificationVariant::ACCESS_REQUESTED => {
                PersonalNotificationVariant::AccessRequested
            }
            PersistentNotificationVariant::PERMISSION_REVOKED => {
                PersonalNotificationVariant::PermissionRevoked
            }
            PersistentNotificationVariant::PERMISSION_GRANTED => {
                PersonalNotificationVariant::PermissionGranted
            }
            PersistentNotificationVariant::PERMISSION_UPDATED => {
                PersonalNotificationVariant::PermissionUpdated
            }
            PersistentNotificationVariant::ANNOUNCEMENT => {
                PersonalNotificationVariant::Announcement
            }
        }
    }
}

impl From<NotificationReference> for Reference {
    fn from(value: NotificationReference) -> Self {
        Reference {
            ref_type: match value.reference_type {
                NotificationReferenceType::User => ReferenceType::User,
                NotificationReferenceType::Resource => ReferenceType::Resource,
            } as i32,
            ref_name: value.reference_name,
            ref_value: value.reference_value,
        }
    }
}

impl TryFrom<PersonalNotificationVariant> for PersistentNotificationVariant {
    type Error = anyhow::Error;

    fn try_from(value: PersonalNotificationVariant) -> std::result::Result<Self, Self::Error> {
        match value {
            PersonalNotificationVariant::Unspecified => Err(anyhow!(
                "Unspecified personal notification variant not allowed"
            )),
            PersonalNotificationVariant::AccessRequested => {
                Ok(PersistentNotificationVariant::ACCESS_REQUESTED)
            }
            PersonalNotificationVariant::PermissionGranted => {
                Ok(PersistentNotificationVariant::PERMISSION_GRANTED)
            }
            PersonalNotificationVariant::PermissionRevoked => {
                Ok(PersistentNotificationVariant::PERMISSION_REVOKED)
            }
            PersonalNotificationVariant::PermissionUpdated => {
                Ok(PersistentNotificationVariant::PERMISSION_UPDATED)
            }
            PersonalNotificationVariant::Announcement => {
                Ok(PersistentNotificationVariant::ANNOUNCEMENT)
            }
        }
    }
}

impl TryFrom<i32> for PersistentNotificationVariant {
    type Error = anyhow::Error;

    fn try_from(value: i32) -> std::result::Result<Self, Self::Error> {
        match value {
            1 => Ok(PersistentNotificationVariant::ACCESS_REQUESTED),
            2 => Ok(PersistentNotificationVariant::PERMISSION_GRANTED),
            3 => Ok(PersistentNotificationVariant::PERMISSION_REVOKED),
            4 => Ok(PersistentNotificationVariant::PERMISSION_UPDATED),
            5 => Ok(PersistentNotificationVariant::ANNOUNCEMENT),
            _ => Err(anyhow!(
                "Unspecified personal notification variant not allowed"
            )),
        }
    }
}

impl From<License> for APILicense {
    fn from(lic: License) -> Self {
        APILicense {
            tag: lic.tag,
            name: lic.name,
            text: lic.description,
            url: lic.url,
        }
    }
}
impl From<CreateLicenseRequest> for License {
    fn from(req: CreateLicenseRequest) -> Self {
        License {
            tag: req.tag,
            name: req.name,
            description: req.text,

            url: req.url,
        }
    }
}
