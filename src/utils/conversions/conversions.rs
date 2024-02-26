use crate::auth::structs::Context;
use crate::caching::cache::Cache;
use crate::database::dsls::hook_dsl::{Filter, Hook, Method};
use crate::database::dsls::internal_relation_dsl::{
    InternalRelation, INTERNAL_RELATION_VARIANT_DELETED,
};
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
    PersistentNotificationVariant, ReplicationStatus, ReplicationType,
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
use aruna_rust_api::api::hooks::services::v2::filter::FilterVariant;
use aruna_rust_api::api::hooks::services::v2::hook::HookType;
use aruna_rust_api::api::hooks::services::v2::internal_hook::InternalAction;
use aruna_rust_api::api::hooks::services::v2::Filter as APIFilter;
use aruna_rust_api::api::hooks::services::v2::{
    AddHook, AddLabel, Credentials, ExternalHook, Hook as APIHook, HookInfo, InternalHook, Trigger,
};
use aruna_rust_api::api::storage::models::v2::data_endpoint::Variant;
use aruna_rust_api::api::storage::models::v2::{
    generic_resource, CustomAttributes, DataClass as APIDataClass, DataEndpoint,
    InternalRelationVariant, License as APILicense, OidcMapping, Permission, PermissionLevel,
    ReplicationStatus as APIReplicationStatus, ResourceVariant, Status, Token, User as ApiUser,
    UserAttributes,
};
use aruna_rust_api::api::storage::models::v2::{
    permission::ResourceId, relation::Relation as RelationEnum, Collection as GRPCCollection,
    Dataset as GRPCDataset, Endpoint, EndpointHostConfig, ExternalRelation, Hash,
    InternalRelation as APIInternalRelation, KeyValue, Object as GRPCObject,
    Project as GRPCProject, Relation, Stats, User,
};
use aruna_rust_api::api::storage::services::v2::{
    create_collection_request, create_dataset_request, create_object_request, CreateLicenseRequest,
    PersonalNotification, PersonalNotificationVariant, Reference, ReferenceType, ServiceAccount,
    WorkspaceInfo,
};
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use postgres_types::Json;
use serde::ser::SerializeStruct;
use serde::Serialize;

use std::str::FromStr;
use std::sync::Arc;
use tonic::metadata::MetadataMap;

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
