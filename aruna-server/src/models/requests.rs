use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{IntoParams, ToSchema};

use super::models::{Author, Group, KeyValue, Realm, Relation, Resource, ResourceVariant, VisibilityClass};

fn default_license_tag() -> String {
    "CC-BY-SA-4.0".to_string()
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateResourceRequest {
    pub name: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub description: String,
    pub variant: ResourceVariant,
    #[serde(default)]
    pub labels: Vec<KeyValue>,
    #[serde(default)]
    pub identifiers: Vec<String>,
    #[serde(default)]
    pub visibility: VisibilityClass,
    #[serde(default)]
    pub authors: Vec<Author>,
    #[serde(default = "default_license_tag")]
    pub license_tag: String,
    pub parent_id: Ulid,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateProjectRequest {
    pub name: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub labels: Vec<KeyValue>,
    #[serde(default)]
    pub identifiers: Vec<String>,
    #[serde(default)]
    pub visibility: VisibilityClass,
    #[serde(default)]
    pub authors: Vec<Author>,
    #[serde(default)]
    pub license_tag: String,
    pub group_id: Ulid,
    pub realm_id: Ulid,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateRealmRequest {
    pub tag: String,
    pub name: String,
    pub description: String,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateGroupRequest {
    pub name: String,
    pub description: String,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateGroupResponse {
    pub group: Group,
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, IntoParams,
)]
pub struct GetResourceRequest {
    pub id: Ulid,
}

// Read responses
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetResourceResponse {
    pub resource: Resource,
    pub relations: Vec<Relation>,
}

// Write responses
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateProjectResponse {
    pub resource: Resource,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateResourceResponse {
    pub resource: Resource,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateRealmResponse {
    pub realm: Realm,
    pub admin_group_id: Ulid,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct AddGroupRequest {
    pub realm_id: Ulid,
    pub group_id: Ulid,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct AddGroupResponse {}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, IntoParams,
)]
pub struct GetRealmRequest {
    pub id: Ulid,
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, IntoParams,
)]
pub struct GetRealmResponse {
    pub realm: Realm,
    pub groups: Vec<Ulid>,
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, IntoParams,
)]
pub struct GetGroupRequest {
    pub id: Ulid,
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, IntoParams,
)]
pub struct GetGroupResponse {
    pub group: Group,
    pub members: Vec<Ulid>,
}