use std::fmt::Display;

use thiserror::Error;
use tonic::Status;
use ulid::Ulid;

use aruna_rust_api::v3::aruna::api::v3 as grpc;
use crate::{error::ArunaError, models};

#[derive(Debug, Error)]
pub struct InvalidFieldError(&'static str);

impl Display for InvalidFieldError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Invalid field: {}", self.0)
    }
}

impl From<InvalidFieldError> for Status {
    fn from(e: InvalidFieldError) -> Self {
        Status::invalid_argument(e.to_string())
    }
}

impl TryFrom<i32> for models::ResourceVariant {
    type Error = InvalidFieldError;
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        Ok(match value {
            1 => models::ResourceVariant::Project,
            2 => models::ResourceVariant::Folder,
            3 => models::ResourceVariant::Object,
            _ => Err(InvalidFieldError("ResourceVariant"))?,
        })
    }
}

impl From<models::ResourceVariant> for i32 {
    fn from(variant: models::ResourceVariant) -> Self {
        match variant {
            models::ResourceVariant::Project => 1,
            models::ResourceVariant::Folder => 2,
            models::ResourceVariant::Object => 3,
        }
    }
}

impl From<grpc::KeyValue> for models::KeyValue {
    fn from(kv: grpc::KeyValue) -> Self {
        Self {
            key: kv.key,
            value: kv.value,
            locked: kv.locked,
        }
    }
}

impl From<models::KeyValue> for grpc::KeyValue {
    fn from(kv: models::KeyValue) -> Self {
        Self {
            key: kv.key,
            value: kv.value,
            locked: kv.locked,
        }
    }
}

impl TryFrom<i32> for models::VisibilityClass {
    type Error = InvalidFieldError;
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        Ok(match value {
            1 => models::VisibilityClass::Public,
            2 => models::VisibilityClass::PublicMetadata,
            3 => models::VisibilityClass::Private,
            _ => Err(InvalidFieldError("VisibilityClass"))?,
        })
    }
}

impl From<models::VisibilityClass> for i32 {
    fn from(variant: models::VisibilityClass) -> Self {
        match variant {
            models::VisibilityClass::Public => 1,
            models::VisibilityClass::PublicMetadata => 2,
            models::VisibilityClass::Private => 3,
        }
    }
}

impl TryFrom<grpc::Author> for models::Author {
    type Error = InvalidFieldError;
    fn try_from(value: grpc::Author) -> Result<Self, Self::Error> {
        Ok(models::Author {
            id: Ulid::from_string(&value.id).map_err(|_| InvalidFieldError("AuthorId"))?,
            first_name: value.first_name,
            last_name: value.last_name,
            email: value.email,
            identifier: value.orcid,
        })
    }
}

impl From<models::Author> for grpc::Author {
    fn from(value: models::Author) -> Self {
        Self {
            id: value.id.to_string(),
            first_name: value.first_name,
            last_name: value.last_name,
            email: value.email,
            orcid: value.identifier,
        }
    }
}

impl From<models::Group> for grpc::Group {
    fn from(value: models::Group) -> Self {
        Self {
            id: value.id.to_string(),
            name: value.name,
            description: value.description,
        }
    }
}

impl From<models::ResourceStatus> for i32 {
    fn from(value: models::ResourceStatus) -> Self {
        match value {
            models::ResourceStatus::StatusInitializing => 1,
            models::ResourceStatus::StatusValidating => 2,
            models::ResourceStatus::StatusAvailable => 3,
            models::ResourceStatus::StatusUnavailable => 4,
            models::ResourceStatus::StatusError => 5,
            models::ResourceStatus::StatusDeleted => 6,
        }
    }
}

impl From<models::Resource> for grpc::Resource {
    fn from(value: models::Resource) -> Self {
        Self {
            id: value.id.to_string(),
            name: value.name.to_string(),
            title: value.title.to_string(),
            description: value.description.to_string(),
            revision: value.revision,
            variant: value.variant.into(),
            labels: value
                .labels
                .into_iter()
                .map(models::KeyValue::into)
                .collect(),
            hook_status: value
                .hook_status
                .into_iter()
                .map(models::KeyValue::into)
                .collect(),
            identifiers: value.identifiers,
            content_len: value.content_len,
            count: value.count,
            visibility: value.visibility.into(),
            created_at: Some(value.created_at.into()),
            last_modified: Some(value.last_modified.into()),
            authors: value
                .authors
                .into_iter()
                .map(models::Author::into)
                .collect(),
            status: value.status.into(),
            locked: value.locked,
            endpoint_status: value
                .endpoint_status
                .into_iter()
                .map(models::EndpointStatus::into)
                .collect(),
            hashes: value.hashes.into_iter().map(models::Hash::into).collect(),
            license_tag: value.license_tag,
        }
    }
}

impl TryFrom<grpc::CreateResourceRequest> for models::CreateResourceRequest {
    type Error = InvalidFieldError;
    fn try_from(value: grpc::CreateResourceRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.name,
            title: value.title,
            description: value.description,
            variant: models::ResourceVariant::try_from(value.variant)?,
            labels: value
                .labels
                .into_iter()
                .map(models::KeyValue::from)
                .collect(),
            identifiers: value.identifiers,
            visibility: models::VisibilityClass::try_from(value.visibility)?,
            authors: value
                .authors
                .into_iter()
                .map(models::Author::try_from)
                .collect::<Result<Vec<_>, _>>()?,
            license_tag: value.license_tag,
            parent_id: Ulid::from_string(&value.parent_id)
                .map_err(|_| InvalidFieldError("parent_id"))?,
        })
    }
}

impl From<grpc::CreateRealmRequest> for models::CreateRealmRequest {
    fn from(value: grpc::CreateRealmRequest) -> Self {
        Self {
            tag: value.tag,
            name: value.name,
            description: value.description,
        }
    }
}
impl From<models::CreateRealmResponse> for grpc::CreateRealmResponse {
    fn from(value: models::CreateRealmResponse) -> Self {
        Self {
            realm: Some(value.realm.into()),
            admin_group_id: value.admin_group_id.to_string(),
        }
    }
}

impl From<grpc::CreateGroupRequest> for models::CreateGroupRequest {
    fn from(value: grpc::CreateGroupRequest) -> Self {
        Self {
            name: value.name,
            description: value.description,
        }
    }
}

impl TryFrom<grpc::CreateProjectRequest> for models::CreateProjectRequest {
    type Error = InvalidFieldError;
    fn try_from(value: grpc::CreateProjectRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.name,
            title: value.title,
            description: value.description,
            labels: value
                .labels
                .into_iter()
                .map(models::KeyValue::from)
                .collect(),
            identifiers: value.identifiers,
            visibility: models::VisibilityClass::try_from(value.visibility)?,
            authors: value
                .authors
                .into_iter()
                .map(models::Author::try_from)
                .collect::<Result<Vec<_>, _>>()?,
            license_tag: value.license_tag,
            group_id: Ulid::from_string(&value.group_id)
                .map_err(|_| InvalidFieldError("group_id"))?,
            realm_id: Ulid::from_string(&value.realm_id)
                .map_err(|_| InvalidFieldError("realm_id"))?,
        })
    }
}

impl From<models::CreateResourceResponse> for grpc::CreateResourceResponse {
    fn from(value: models::CreateResourceResponse) -> Self {
        grpc::CreateResourceResponse {
            resource: Some(value.resource.into()),
        }
    }
}

impl From<models::GetResourceResponse> for grpc::GetResourceResponse {
    fn from(value: models::GetResourceResponse) -> Self {
        grpc::GetResourceResponse {
            resource: Some(value.resource.into()),
            relations: value
                .relations
                .into_iter()
                .map(grpc::Relation::from)
                .collect::<Vec<_>>(),
        }
    }
}

impl From<models::Relation> for grpc::Relation {
    fn from(value: models::Relation) -> Self {
        Self {
            from_id: value.from_id.to_string(),
            to_id: value.to_id.to_string(),
            relation_type: value.relation_type,
        }
    }
}

impl From<models::EndpointStatus> for grpc::EndpointStatus {
    fn from(value: models::EndpointStatus) -> Self {
        Self {
            endpoint_id: value.endpoint_id.to_string(),
            status: value.status,
            variant: value.variant,
        }
    }
}

impl From<models::Hash> for grpc::Hash {
    fn from(value: models::Hash) -> Self {
        Self {
            algorithm: value.algorithm.to_string(),
            value: value.value,
        }
    }
}

impl TryFrom<grpc::GetResourceRequest> for models::GetResourceRequest {
    type Error = InvalidFieldError;

    fn try_from(value: grpc::GetResourceRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            id: Ulid::from_string(&value.id).map_err(|_| InvalidFieldError("id"))?,
        })
    }
}

impl From<models::Realm> for grpc::Realm {
    fn from(value: models::Realm) -> Self {
        Self {
            id: value.id.to_string(),
            tag: value.tag,
            name: value.name,
            description: value.description,
        }
    }
}

impl TryFrom<grpc::AddGroupRequest> for models::AddGroupRequest {
    type Error = InvalidFieldError;

    fn try_from(value: grpc::AddGroupRequest) -> Result<Self, Self::Error> {
        Ok(models::AddGroupRequest {
            realm_id: Ulid::from_string(&value.realm_id)
                .map_err(|_| InvalidFieldError("realm_id"))?,
            group_id: Ulid::from_string(&value.group_id)
                .map_err(|_| InvalidFieldError("group_id"))?,
        })
    }
}

impl From<models::AddGroupResponse> for grpc::AddGroupResponse {
    fn from(_value: models::AddGroupResponse) -> Self {
        grpc::AddGroupResponse {}
    }
}

impl From<models::CreateGroupResponse> for grpc::CreateGroupResponse {
    fn from(value: models::CreateGroupResponse) -> Self {
        Self {
            group: Some(value.group.into()),
        }
    }
}

impl From<models::CreateProjectResponse> for grpc::CreateProjectResponse {
    fn from(value: models::CreateProjectResponse) -> Self {
        Self {
            resource: Some(value.resource.into()),
        }
    }
}

impl TryFrom<grpc::GetRealmRequest> for models::GetRealmRequest {
    type Error = InvalidFieldError;

    fn try_from(value: grpc::GetRealmRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            id: Ulid::from_string(&value.id).map_err(|_| InvalidFieldError("id"))?,
        })
    }
}

impl From<models::GetRealmResponse> for grpc::GetRealmResponse {
    fn from(value: models::GetRealmResponse) -> Self {
        Self {
            realm: Some(value.realm.into()),
            group_ids: value.groups.into_iter().map(|id| id.to_string()).collect(),
        }
    }
}

impl TryFrom<grpc::GetGroupRequest> for models::GetGroupRequest {
    type Error = InvalidFieldError;

    fn try_from(value: grpc::GetGroupRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            id: Ulid::from_string(&value.id).map_err(|_| InvalidFieldError("id"))?,
        })
    }
}

impl From<models::GetGroupResponse> for grpc::GetGroupResponse {
    fn from(value: models::GetGroupResponse) -> Self {
        Self {
            group: Some(value.group.into()),
            members: value.members.into_iter().map(|id| id.to_string()).collect(),
        }
    }
}

impl From<ArunaError> for tonic::Status {
    fn from(value: ArunaError) -> Self {
        match value {
            msg @ ArunaError::InvalidParameter { .. }
            | msg @ ArunaError::ParameterNotSpecified { .. }
            | msg @ ArunaError::ConflictParameter { .. } => {
                tonic::Status::invalid_argument(msg.to_string())
            }
            msg @ ArunaError::Forbidden(..) => tonic::Status::permission_denied(msg.to_string()),
            ArunaError::Unauthorized => {
                tonic::Status::unauthenticated("No valid credentials provided")
            }
            msg @ ArunaError::NotFound(..) => tonic::Status::not_found(msg.to_string()),
            msg => tonic::Status::internal(msg.to_string()),
        }
    }
}
