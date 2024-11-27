use chrono::Utc;
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{IntoParams, ToSchema};

use crate::transactions::request::WriteRequest;

use super::models::{
    Author, Component, GenericNode, Group, KeyValue, Permission, Realm, Relation, RelationInfo,
    Resource, ResourceVariant, Token, User, VisibilityClass,
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

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
pub struct CreateResourceBatchRequest {
    pub resources: Vec<BatchResource>,
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
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
pub struct GetResourcesRequest {
    pub ids: Vec<Ulid>,
}

// Read responses
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetResourcesResponse {
    pub resources: Vec<Resource>,
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

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, IntoParams,
)]
pub struct GetRelationsRequest {
    pub node: Ulid,
    pub direction: Direction, // wrapper type for petgraph::Direction enum
    pub filter: Vec<u32>,     // Filter with Strings for directions or idx for rel idx?
    pub offset: Option<usize>,
    pub page_size: usize, // Max value 1000? Default 100
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetRelationsResponse {
    pub relations: Vec<Relation>,
    pub offset: Option<usize>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetRelationInfosRequest {}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetRelationInfosResponse {
    pub relation_infos: Vec<RelationInfo>,
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, IntoParams,
)]
pub struct GetUsersFromGroupRequest {
    pub group_id: Ulid,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetUsersFromGroupResponse {
    pub users: Vec<User>,
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, IntoParams,
)]
pub struct GetGroupsFromRealmRequest {
    pub realm_id: Ulid,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetGroupsFromRealmResponse {
    pub groups: Vec<Group>,
}

// Issuer and auth requests
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct AddOidcProviderRequest {
    pub issuer_name: String,
    pub issuer_endpoint: String,
    pub audiences: Vec<String>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct AddOidcProviderResponse {}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetRealmsFromUserRequest {}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetRealmsFromUserResponse {
    pub realms: Vec<Realm>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetGroupsFromUserRequest {}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetGroupsFromUserResponse {
    pub groups: Vec<(Group, Permission)>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetStatsRequest {}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetStatsResponse {
    pub resources: usize,
    pub projects: usize,
    pub users: usize,
    pub storage: usize, // in bytes
    pub realms: usize,
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, IntoParams,
)]
pub struct GetRealmComponentsRequest {
    pub realm_id: Ulid,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetRealmComponentsResponse {
    pub components: Vec<Component>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetUserRequest {}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetUserResponse {
    pub user: User,
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, IntoParams,
)]
pub struct GetEventsRequest {
    pub subscriber_id: Ulid,
    #[serde(default)]
    pub acknowledge_from: Option<Ulid>,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetEventsResponse {
    // EventId, Event
    pub events: Vec<(Ulid, serde_json::Value)>,
}

// User ask to access a group
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct UserAccessGroupRequest {
    pub group_id: Ulid,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct UserAccessGroupResponse {}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct GroupAccessRealmRequest {
    pub group_id: Ulid,
    pub realm_id: Ulid,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct GroupAccessRealmResponse {}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateRelationRequest {
    pub source: Ulid,
    pub target: Ulid,
    pub variant: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateRelationResponse {}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateRelationVariantRequest {
    pub forward_type: String,
    pub backward_type: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateRelationVariantResponse {
    pub idx: u32,
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
pub struct UpdateResourceRequest {
    pub id: Ulid,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub visibility: VisibilityClass,
    #[serde(default = "default_license_tag")]
    pub license_tag: String,

    // TODO:
    // #[serde(default)]
    // pub labels: Vec<KeyValue>,
    // #[serde(default)]
    // pub identifiers: Vec<String>,
    // #[serde(default)]
    // pub authors: Vec<Author>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdateResourceResponse {
    pub resource: Resource,
}
