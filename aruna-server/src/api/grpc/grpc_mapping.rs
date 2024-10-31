use std::fmt::Display;

use thiserror::Error;
use tonic::Status;
use ulid::Ulid;

use crate::{
    error::ArunaError,
    models::{models, requests},
};
use aruna_rust_api::v3::aruna::api::v3::{self as grpc, ResourceStatus};

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
            hook_status: vec![],
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
            status: ResourceStatus::StatusAvailable as i32,
            locked: value.locked,
            endpoint_status: vec![],
            hashes: value.hashes.into_iter().map(models::Hash::into).collect(),
            license_tag: value.license_tag,
        }
    }
}

impl TryFrom<grpc::CreateResourceRequest> for requests::CreateResourceRequest {
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

impl From<grpc::CreateRealmRequest> for requests::CreateRealmRequest {
    fn from(value: grpc::CreateRealmRequest) -> Self {
        Self {
            tag: value.tag,
            name: value.name,
            description: value.description,
        }
    }
}
impl From<requests::CreateRealmResponse> for grpc::CreateRealmResponse {
    fn from(value: requests::CreateRealmResponse) -> Self {
        Self {
            realm: Some(value.realm.into()),
            admin_group_id: value.admin_group_id.to_string(),
        }
    }
}

impl From<grpc::CreateGroupRequest> for requests::CreateGroupRequest {
    fn from(value: grpc::CreateGroupRequest) -> Self {
        Self {
            name: value.name,
            description: value.description,
        }
    }
}

impl TryFrom<grpc::CreateProjectRequest> for requests::CreateProjectRequest {
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

impl From<requests::CreateResourceResponse> for grpc::CreateResourceResponse {
    fn from(value: requests::CreateResourceResponse) -> Self {
        grpc::CreateResourceResponse {
            resource: Some(value.resource.into()),
        }
    }
}

impl From<requests::GetResourceResponse> for grpc::GetResourceResponse {
    fn from(value: requests::GetResourceResponse) -> Self {
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

impl From<models::Hash> for grpc::Hash {
    fn from(value: models::Hash) -> Self {
        Self {
            algorithm: value.algorithm.to_string(),
            value: value.value,
        }
    }
}

impl TryFrom<grpc::GetResourceRequest> for requests::GetResourceRequest {
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

impl TryFrom<grpc::AddGroupRequest> for requests::AddGroupRequest {
    type Error = InvalidFieldError;

    fn try_from(value: grpc::AddGroupRequest) -> Result<Self, Self::Error> {
        Ok(requests::AddGroupRequest {
            realm_id: Ulid::from_string(&value.realm_id)
                .map_err(|_| InvalidFieldError("realm_id"))?,
            group_id: Ulid::from_string(&value.group_id)
                .map_err(|_| InvalidFieldError("group_id"))?,
        })
    }
}

impl From<requests::AddGroupResponse> for grpc::AddGroupResponse {
    fn from(_value: requests::AddGroupResponse) -> Self {
        grpc::AddGroupResponse {}
    }
}

impl From<requests::CreateGroupResponse> for grpc::CreateGroupResponse {
    fn from(value: requests::CreateGroupResponse) -> Self {
        Self {
            group: Some(value.group.into()),
        }
    }
}

impl From<requests::CreateProjectResponse> for grpc::CreateProjectResponse {
    fn from(value: requests::CreateProjectResponse) -> Self {
        Self {
            resource: Some(value.resource.into()),
        }
    }
}

impl TryFrom<grpc::GetRealmRequest> for requests::GetRealmRequest {
    type Error = InvalidFieldError;

    fn try_from(value: grpc::GetRealmRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            id: Ulid::from_string(&value.id).map_err(|_| InvalidFieldError("id"))?,
        })
    }
}

impl From<requests::GetRealmResponse> for grpc::GetRealmResponse {
    fn from(value: requests::GetRealmResponse) -> Self {
        Self {
            realm: Some(value.realm.into()),
            group_ids: value.groups.into_iter().map(|id| id.to_string()).collect(),
        }
    }
}

impl TryFrom<grpc::GetGroupRequest> for requests::GetGroupRequest {
    type Error = InvalidFieldError;

    fn try_from(value: grpc::GetGroupRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            id: Ulid::from_string(&value.id).map_err(|_| InvalidFieldError("id"))?,
        })
    }
}

impl From<requests::GetGroupResponse> for grpc::GetGroupResponse {
    fn from(value: requests::GetGroupResponse) -> Self {
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