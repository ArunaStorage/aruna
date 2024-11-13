use chrono::Utc;
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{IntoParams, ToSchema};

use super::models::{
    Author, GenericNode, Group, KeyValue, Permission, Realm, Relation, RelationInfo, Resource, ResourceVariant, Token, User, VisibilityClass
};

fn default_license_tag() -> String {
    "CC-BY-SA-4.0".to_string()
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
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

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
pub struct BatchResource {
    pub name: String,
    pub parent: Parent,
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
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Hash)]
pub enum Parent {
    ID(Ulid),
    Idx(u32),
}

impl Default for Parent {
    fn default() -> Self {
        Parent::Idx(0)
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default)]
pub struct CreateResourceBatchRequest {
    pub resources: Vec<BatchResource>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default)]
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
    // TODO: Default endpoints?
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
    // pub relations: Vec<Relation>, // TODO: remove and move into its own request
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
pub struct CreateResourceBatchResponse {
    pub resources: Vec<Resource>,
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
    // pub groups: Vec<Ulid>, // TODO: remove and move into its own request
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
    // pub members: Vec<Ulid>, // TODO: remove and move into its own request?
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct RegisterUserRequest {
    pub email: String,
    pub first_name: String,
    pub last_name: String,
    pub identifier: String,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct RegisterUserResponse {
    pub user: User,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateTokenRequest {
    pub user_id: Ulid, // TODO: REMOVE
    pub name: String,
    #[serde(default)]
    pub expires_at: Option<chrono::DateTime<Utc>>,
    //pub constraints: Vec<Constraint>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateTokenResponse {
    pub token: Token,
    pub secret: String,
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, IntoParams,
)]
pub struct SearchRequest {
    #[serde(default)]
    pub query: String,
    #[serde(default)]
    pub filter: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub offset: Option<usize>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct SearchResponse {
    pub expected_hits: usize,
    pub resources: Vec<GenericNode>,
}


#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct AddUserRequest {
    pub group_id: Ulid,
    pub user_id: Ulid,
    pub permission: Permission,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct AddUserResponse {}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub enum Direction {
    Incoming,
    Outgoing,
}

// TODO: Impl relation requests
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetRelationsRequest {
    pub node: Ulid,
    pub direction: Direction, // wrapper type for petgraph::Direction enum
    pub filter: Vec<u32>, // Filter with Strings for directions or idx for rel idx?
    pub offset: Option<usize>,
    pub page_size: usize, // Max value 1000? Default 100
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetRelationsResponse {
    pub relations: Vec<Relation>,
    pub offset: Option<usize>,
}


#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetRelationInfoRequest {
    pub relation_idx: u32,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetRelationInfoResponse {
    pub relation_info: RelationInfo,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetUsersFromGroupRequest {
    pub group_id: Ulid,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetUsersFromGroupResponse {
    pub users: Vec<User>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetGroupsFromRealmRequest {
    pub realm_id: Ulid,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetGroupsFromRealmResponse {
    pub groups: Vec<Group>,
}
