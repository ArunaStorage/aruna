use std::fmt::Display;

use chrono::{DateTime, NaiveDateTime, Utc};
use jsonwebtoken::DecodingKey;
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{IntoParams, ToSchema};

use crate::error::ArunaError;

pub type EdgeType = u32;

// Constants for the models
pub const HAS_PART: u32 = 0u32;
pub const OWNS_PROJECT: u32 = 1u32;
pub const PERMISSION_NONE: u32 = 2u32;
pub const PERMISSION_READ: u32 = 3u32;
pub const PERMISSION_APPEND: u32 = 4u32;
pub const PERMISSION_WRITE: u32 = 5u32;
pub const PERMISSION_ADMIN: u32 = 6u32;
pub const SHARES_PERMISSION: u32 = 7u32;
pub const OWNED_BY_USER: u32 = 8u32;
pub const GROUP_PART_OF_REALM: u32 = 9u32;
pub const GROUP_ADMINISTRATES_REALM: u32 = 10u32;
pub const REALM_USES_ENDPOINT: u32 = 11u32;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub enum ResourceVariant {
    Project,
    Folder,
    Object,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub enum NodeVariantValue {
    Resource(Resource),
    User(User),
    Token(Token),
    ServiceAccount(ServiceAccount),
    Group(Group),
    Realm(Realm),
}

impl NodeVariantValue {
    pub fn get_id(&self) -> &Ulid {
        match self {
            NodeVariantValue::Resource(res) => &res.id,
            NodeVariantValue::Token(token) => &token.id,
            NodeVariantValue::User(user) => &user.id,
            NodeVariantValue::ServiceAccount(service_account) => &service_account.id,
            NodeVariantValue::Group(group) => &group.id,
            NodeVariantValue::Realm(realm) => &realm.id,
        }
    }
}

impl NodeVariantId {
    pub fn get_ref(&self) -> &Ulid {
        match self {
            NodeVariantId::Resource(id) => id,
            NodeVariantId::User(id) => id,
            NodeVariantId::Token(id) => id,
            NodeVariantId::Group(id) => id,
            NodeVariantId::Realm(id) => id,
            NodeVariantId::ServiceAccount(id) => id,
        }
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub enum NodeVariantId {
    Resource(Ulid),
    User(Ulid),
    ServiceAccount(Ulid),
    Token(Ulid),
    Group(Ulid),
    Realm(Ulid),
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
    pub locked: bool,
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, Default, ToSchema,
)]
pub enum VisibilityClass {
    Public,
    PublicMetadata,
    #[default]
    Private,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct Author {
    pub id: Ulid,
    pub first_name: String,
    pub last_name: String,
    pub email: String,
    pub identifier: String,
}

// ArunaGraph Nodes
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct Realm {
    pub id: Ulid,
    pub tag: String, // -> Region
    pub name: String,
    pub description: String,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct Group {
    pub id: Ulid,
    pub name: String,
    pub description: String,
    // TODO: OIDC mapping ?
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct Token {
    pub id: Ulid,
    pub name: String,
    pub expires_at: DateTime<Utc>,
}
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct TokenWithPermission {
    pub id: Ulid,
    pub name: String,
    pub expires_at: DateTime<Utc>,
    pub permission: Permission,
    pub resource_id: Ulid,
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
pub struct User {
    pub id: Ulid,
    pub first_name: String,
    pub last_name: String,
    pub email: String,
    pub identifiers: String, // TODO: Vec<String>?
    /// TODO: OIDC mapping ?
    pub global_admin: bool,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct ServiceAccount {
    pub id: Ulid,
    pub name: String,
    // TODO: More fields?
}

pub struct InternalResource {
    resource: Resource,
    events: Events,
}

pub enum Events {
    ProjectEvents(Vec<u128>),
    ResourceEvents(Vec<u32>),
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct Resource {
    pub id: Ulid,
    pub name: String,
    pub title: String,
    pub description: String,
    pub revision: u64,
    pub variant: ResourceVariant,
    pub labels: Vec<KeyValue>,
    pub hook_status: Vec<KeyValue>,
    pub identifiers: Vec<String>,
    pub content_len: u64,
    pub count: u64,
    pub visibility: VisibilityClass,
    pub created_at: DateTime<Utc>,
    pub last_modified: DateTime<Utc>,
    pub authors: Vec<Author>,
    pub status: ResourceStatus,
    pub locked: bool,
    pub license_tag: String,
    // TODO:
    pub endpoint_status: Vec<EndpointStatus>,
    pub hashes: Vec<Hash>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct RelationInfo {
    pub idx: EdgeType,
    pub forward_type: String,  // A --- HasPart---> B
    pub backward_type: String, // A <---PartOf--- B
    pub internal: bool,        // only for internal use
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct Relation {
    pub from_id: Ulid,
    pub to_id: Ulid,
    pub relation_type: String,
}

#[derive(Deserialize, Serialize)]
pub struct RawRelation {
    pub source: NodeVariantId,
    pub target: NodeVariantId,
    pub edge_type: EdgeType,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct ServerInfo {
    pub node_id: Ulid,
    pub node_serial: u32,
    pub url: String,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct ServerState {
    pub node_id: Ulid,
    pub status: String,
}

pub struct PubKey {
    pub key_serial: u32,
    pub node_id: Ulid,
    pub key: String,
    pub decoding_key: DecodingKey,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct Hash {
    pub algorithm: HashAlgorithm,
    pub value: String,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct Endpoint {
    pub id: Ulid,
    pub name: String,
    /// TODO: Add more fields
    pub description: String,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct EndpointStatus {
    pub endpoint_id: String,
    pub status: i32,
    pub variant: i32,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub enum ResourceStatus {
    StatusInitializing,
    StatusValidating,
    StatusAvailable,
    StatusUnavailable,
    StatusError,
    StatusDeleted,
}

pub enum ResourceEndpointStatus {
    Pending,
    Running,
    Finished,
    Error,
}

pub enum ResourceEndpointVariant {
    Dataproxy,
    Compute,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub enum HashAlgorithm {
    Sha256,
    MD5,
}

impl Display for HashAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            HashAlgorithm::Sha256 => "Sha256",
            HashAlgorithm::MD5 => "MD5",
        };
        write!(f, "{}", name)
    }
}

#[repr(u32)]
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub enum Permission {
    None = 2,
    Read = 3,
    Append = 4,
    Write = 5,
    Admin = 6,
}

impl TryFrom<u32> for Permission {
    type Error = ArunaError;
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        Ok(match value {
            PERMISSION_NONE => Permission::None,
            PERMISSION_READ => Permission::Read,
            PERMISSION_APPEND => Permission::Append,
            PERMISSION_WRITE => Permission::Write,
            PERMISSION_ADMIN => Permission::Admin,
            _ => {
                return Err(ArunaError::ConversionError {
                    from: format!("{}u32", value),
                    to: "models::Permission".to_string(),
                })
            }
        })
    }
}

// Write requests

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

// Read requests

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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum IssuerType {
    ARUNA,
    OIDC,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Issuer {
    pub issuer_name: String,
    pub pubkey_endpoint: Option<String>,
    pub audiences: Option<Vec<String>>,
    pub issuer_type: IssuerType,
}

impl Issuer {
    pub async fn new_with_endpoint(
        issuer_name: String,
        pubkey_endpoint: String,
        audiences: Option<Vec<String>>,
    ) -> Self {
        Self {
            issuer_name,
            pubkey_endpoint: Some(pubkey_endpoint),
            audiences,
            issuer_type: IssuerType::OIDC,
        }
    }

    pub async fn new_with_keys(
        issuer_name: String,
        audiences: Option<Vec<String>>,
        issuer_type: IssuerType,
    ) -> Self {
        Self {
            issuer_name,
            pubkey_endpoint: None,
            audiences,
            issuer_type,
        }
    }

    pub async fn fetch_jwks(
        endpoint: &str,
    ) -> Result<(Vec<(String, DecodingKey)>, NaiveDateTime), ArunaError> {
        let client = reqwest::Client::new();
        let res = client.get(endpoint).send().await.map_err(|e| {
            tracing::error!(?e, "Error fetching JWK from endpoint");
            ArunaError::Unauthorized
        })?;
        let jwks: jsonwebtoken::jwk::JwkSet = res.json().await.map_err(|e| {
            tracing::error!(?e, "Error serializing JWK from endpoint");
            ArunaError::Unauthorized
        })?;

        Ok((
            jwks.keys
                .iter()
                .filter_map(|jwk| {
                    let key = DecodingKey::from_jwk(jwk).ok()?;
                    Some((jwk.common.clone().key_id?, key))
                })
                .collect::<Vec<_>>(),
            Utc::now().naive_utc(),
        ))
    }
}

/// This contains claims for ArunaTokens
/// containing 3 mandatory and 2 optional fields.
///
/// - iss: Token issuer
/// - sub: User_ID or subject
/// - exp: When this token expires (by default very large number)
/// - tid: UUID from the specific token
#[derive(Debug, Serialize, Deserialize)]
pub struct ArunaTokenClaims {
    pub iss: String, // 'aruna' or oidc issuer
    pub sub: String, // Token id / OIDC Subject
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aud: Option<Audience>, // Audience;
    pub exp: u64,    // Expiration timestamp
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
#[serde(untagged)]
pub enum Audience {
    String(String),
    Vec(Vec<String>),
}
