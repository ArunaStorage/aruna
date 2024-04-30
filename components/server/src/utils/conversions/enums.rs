use crate::database::{
    dsls::{
        hook_dsl::{Filter, Method},
        object_dsl::{Algorithm, Hashes, KeyValueVariant},
    },
    enums::{
        DataClass, DataProxyFeature, DbPermissionLevel, EndpointStatus, EndpointVariant,
        ObjectMapping, ObjectStatus, ObjectType, PersistentNotificationVariant, ReplicationStatus,
        ReplicationType,
    },
};
use anyhow::{anyhow, bail, Result};
use aruna_rust_api::api::hooks::services::v2::{filter::FilterVariant, Filter as APIFilter};
use aruna_rust_api::api::storage::{
    models::v2::{
        data_endpoint::Variant, permission::ResourceId, DataClass as APIDataClass, Hash,
        PermissionLevel, ReplicationStatus as APIReplicationStatus, ResourceVariant, Status,
    },
    services::v2::PersonalNotificationVariant,
};
use diesel_ulid::DieselUlid;
use std::str::FromStr;

impl TryFrom<i32> for KeyValueVariant {
    type Error = anyhow::Error;
    fn try_from(var: i32) -> Result<Self> {
        match var {
            1 => Ok(KeyValueVariant::LABEL),
            2 => Ok(KeyValueVariant::STATIC_LABEL),
            3 => Ok(KeyValueVariant::HOOK),
            4 => Err(anyhow!("Can't create HookStatus outside of hook callbacks")),
            _ => Err(anyhow!("KeyValue variant not defined.")),
        }
    }
}

impl TryFrom<APIDataClass> for DataClass {
    type Error = anyhow::Error;

    fn try_from(value: APIDataClass) -> std::result::Result<Self, Self::Error> {
        match value {
            APIDataClass::Unspecified => bail!("Unsepcified data class is invalid"),
            APIDataClass::Public => Ok(DataClass::PUBLIC),
            APIDataClass::Private => Ok(DataClass::PRIVATE),
            APIDataClass::Workspace => Ok(DataClass::WORKSPACE),
            APIDataClass::Confidential => Ok(DataClass::CONFIDENTIAL),
        }
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

impl From<DataClass> for APIDataClass {
    fn from(value: DataClass) -> Self {
        match value {
            DataClass::PUBLIC => APIDataClass::Public,
            DataClass::PRIVATE => APIDataClass::Private,
            DataClass::WORKSPACE => APIDataClass::Workspace,
            DataClass::CONFIDENTIAL => APIDataClass::Confidential,
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

impl From<&Method> for i32 {
    fn from(method: &Method) -> Self {
        match method {
            Method::PUT => 1,
            Method::POST => 2,
        }
    }
}

impl From<ReplicationStatus> for APIReplicationStatus {
    fn from(input: ReplicationStatus) -> Self {
        match input {
            ReplicationStatus::Waiting => APIReplicationStatus::Waiting,
            ReplicationStatus::Running => APIReplicationStatus::Running,
            ReplicationStatus::Finished => APIReplicationStatus::Finished,
            ReplicationStatus::Error => APIReplicationStatus::Error,
        }
    }
}

impl From<ReplicationType> for Variant {
    fn from(input: ReplicationType) -> Self {
        match input {
            ReplicationType::FullSync => {
                Variant::FullSync(aruna_rust_api::api::storage::models::v2::FullSync {})
            }
            ReplicationType::PartialSync(inheritance) => Variant::PartialSync(inheritance),
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
impl TryFrom<APIFilter> for Filter {
    type Error = anyhow::Error;
    fn try_from(filter: APIFilter) -> Result<Self> {
        match filter.filter_variant {
            Some(var) => match var {
                FilterVariant::Name(name) => Ok(Filter::Name(name)),
                FilterVariant::KeyValue(kv) => Ok(Filter::KeyValue(crate::database::dsls::object_dsl::KeyValue {
                    key: kv.key.clone(),
                    value: kv.value.clone(),
                    variant: match kv.variant() {
                        aruna_rust_api::api::storage::models::v2::KeyValueVariant::Unspecified => {
                            return Err(anyhow!("Invalid key value variant"));
                        }
                        aruna_rust_api::api::storage::models::v2::KeyValueVariant::Label => {
                            KeyValueVariant::LABEL
                        }
                        aruna_rust_api::api::storage::models::v2::KeyValueVariant::StaticLabel => {
                            KeyValueVariant::STATIC_LABEL
                        }
                        aruna_rust_api::api::storage::models::v2::KeyValueVariant::Hook => {
                            KeyValueVariant::HOOK
                        }
                        aruna_rust_api::api::storage::models::v2::KeyValueVariant::HookStatus => {
                            KeyValueVariant::HOOK_STATUS
                        }
                    },
                })),
            },
            None => Err(anyhow!("No filter provided")),
        }
    }
}

// Conversion tests
#[cfg(test)]
mod tests {
    use crate::database::enums::DataClass;
    use aruna_rust_api::api::storage::models::v2::DataClass as APIDataClass;

    #[test]
    fn data_class_conversion_tests() {
        // Direct enum conversion in both directions
        assert_eq!(APIDataClass::Public, DataClass::PUBLIC.into());
        assert_eq!(APIDataClass::Private, DataClass::PRIVATE.into());
        assert_eq!(APIDataClass::Workspace, DataClass::WORKSPACE.into());
        assert_eq!(APIDataClass::Confidential, DataClass::CONFIDENTIAL.into());

        assert_eq!(DataClass::PUBLIC, APIDataClass::Public.try_into().unwrap());
        assert_eq!(
            DataClass::PRIVATE,
            APIDataClass::Private.try_into().unwrap()
        );
        assert_eq!(
            DataClass::WORKSPACE,
            APIDataClass::Workspace.try_into().unwrap()
        );
        assert_eq!(
            DataClass::CONFIDENTIAL,
            APIDataClass::Confidential.try_into().unwrap()
        );

        // i32 conversion in both directions
        assert_eq!(APIDataClass::Public as i32, DataClass::PUBLIC as i32);
        assert_eq!(APIDataClass::Private as i32, DataClass::PRIVATE as i32);
        assert_eq!(APIDataClass::Workspace as i32, DataClass::WORKSPACE as i32);
        assert_eq!(
            APIDataClass::Confidential as i32,
            DataClass::CONFIDENTIAL as i32
        );
    }
}
