///  A key value pair for hooks and labels
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct KeyValue {
    #[prost(string, tag = "1")]
    pub key: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub value: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct LabelOntology {
    ///  These are the keys for labels that are required for the collection
    ///  Adding an Object without these keys will result in an error
    ///  Defaults to empty string if not specified
    #[prost(string, repeated, tag = "1")]
    pub required_label_keys: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
///  Stats for a set of objects
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct Stats {
    #[prost(int64, tag = "1")]
    pub count: i64,
    #[prost(int64, tag = "2")]
    pub acc_size: i64,
}
///  Stats for a collection
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct CollectionStats {
    #[prost(message, optional, tag = "1")]
    pub object_stats: ::core::option::Option<Stats>,
    #[prost(int64, tag = "2")]
    pub object_group_count: i64,
    #[prost(message, optional, tag = "3")]
    pub last_updated: ::core::option::Option<::prost_types::Timestamp>,
}
///  Stats for an object group
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct ObjectGroupStats {
    #[prost(message, optional, tag = "1")]
    pub object_stats: ::core::option::Option<Stats>,
    #[prost(message, optional, tag = "2")]
    pub last_updated: ::core::option::Option<::prost_types::Timestamp>,
}
///  Semver version -> Alpha Beta release are not supported -> Use "latest" for
///  mutable collections that are in development
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct Version {
    #[prost(int32, tag = "1")]
    pub major: i32,
    #[prost(int32, tag = "2")]
    pub minor: i32,
    #[prost(int32, tag = "3")]
    pub patch: i32,
}
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct Hash {
    #[prost(enumeration = "Hashalgorithm", tag = "1")]
    pub alg: i32,
    #[prost(string, tag = "2")]
    pub hash: ::prost::alloc::string::String,
}
///  Origin of the object -> To be GDPA compliant
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct Origin {
    #[prost(enumeration = "OriginType", tag = "1")]
    pub r#type: i32,
    #[prost(string, tag = "2")]
    pub id: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct Source {
    ///  This is a URL / DOI
    #[prost(string, tag = "1")]
    pub identifier: ::prost::alloc::string::String,
    ///  Either URL oder DOI
    #[prost(enumeration = "SourceType", tag = "2")]
    pub source_type: i32,
}
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct Endpoint {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    #[prost(enumeration = "EndpointType", tag = "2")]
    pub ep_type: i32,
    #[prost(string, tag = "3")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub proxy_hostname: ::prost::alloc::string::String,
    #[prost(string, tag = "5")]
    pub internal_hostname: ::prost::alloc::string::String,
    #[prost(string, tag = "6")]
    pub documentation_path: ::prost::alloc::string::String,
    #[prost(bool, tag = "7")]
    pub is_public: bool,
    #[prost(bool, tag = "8")]
    pub is_default: bool,
}
//  RULES for Objects:
//  1.  Each object is "owned" by one/or more collections
//  2.  Objects can be "borrowed" to multiple other collections
//  3.  Objects are immutable, updating an object will create a new object with
//  increased revision number
//      only people with modify permissions in the owner collection can update an
//      object
//  3.1 Special cases:
//      Hooks: Can be added/removed and modified without changing the object
//      revision number Labels: Can be added without changing the object revision
//      number, removing or modifying labels WILL change the object revision
//      number (append only) auto_update: Can be added/removed without changing
//      the object revision number and is collection specific
//  4.  Objects can only be permanently deleted by a person with admin rights on
//  the owner collection

#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct Object {
    ///  ObjectID
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    ///  Filename: Name of the original file e.g.: mydata.json
    #[prost(string, tag = "2")]
    pub filename: ::prost::alloc::string::String,
    ///  Labels to additionally describe the object
    #[prost(message, repeated, tag = "4")]
    pub labels: ::prost::alloc::vec::Vec<KeyValue>,
    ///  Hooks to be executed on the object
    #[prost(message, repeated, tag = "5")]
    pub hooks: ::prost::alloc::vec::Vec<KeyValue>,
    #[prost(message, optional, tag = "6")]
    pub created: ::core::option::Option<::prost_types::Timestamp>,
    ///  Lenght of the stored dataset
    #[prost(int64, tag = "7")]
    pub content_len: i64,
    #[prost(enumeration = "Status", tag = "8")]
    pub status: i32,
    ///  Origin of the object
    #[prost(message, optional, tag = "9")]
    pub origin: ::core::option::Option<Origin>,
    ///  Confidentiality of the object
    #[prost(enumeration = "DataClass", tag = "10")]
    pub data_class: i32,
    ///  MD5 hash of the data
    #[prost(message, optional, tag = "11")]
    pub hash: ::core::option::Option<Hash>,
    ///  Increasing revion number for each update
    #[prost(int64, tag = "12")]
    pub rev_number: i64,
    ///  External source where this data originates from
    #[prost(message, optional, tag = "13")]
    pub source: ::core::option::Option<Source>,
    ///  Is this the latest version of the object?
    #[prost(bool, tag = "14")]
    pub latest: bool,
    ///  This is a collection specific attribute
    ///  Must be false if collection is immutable
    ///
    ///  If true, the object will be updated automatically
    #[prost(bool, tag = "15")]
    pub auto_update: bool,
}
///  Multiple Objects
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct Objects {
    #[prost(message, repeated, tag = "1")]
    pub objects: ::prost::alloc::vec::Vec<Object>,
}
///  ObjectGroups are optional and can be used to group objects in a collection
///  together They need to refer to objects in the same collection Objectgroups
///  can be changed if the collection is mutable
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct ObjectGroup {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub description: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "6")]
    pub labels: ::prost::alloc::vec::Vec<KeyValue>,
    #[prost(message, repeated, tag = "7")]
    pub hooks: ::prost::alloc::vec::Vec<KeyValue>,
    ///  Must be in collection objects
    #[prost(message, repeated, tag = "8")]
    pub objects: ::prost::alloc::vec::Vec<Object>,
    ///  Must be in collection objects
    #[prost(message, repeated, tag = "9")]
    pub meta_objects: ::prost::alloc::vec::Vec<Object>,
    #[prost(message, optional, tag = "10")]
    pub stats: ::core::option::Option<ObjectGroupStats>,
    #[prost(int64, tag = "11")]
    pub rev_number: i64,
}
///  Multiple ObjectGroups
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct ObjectGroups {
    #[prost(message, repeated, tag = "1")]
    pub object_groups: ::prost::alloc::vec::Vec<ObjectGroup>,
}
///  This is a representation of the ObjectGroup without the recursive nature of
///  object references
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct ObjectGroupOverview {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub description: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "6")]
    pub labels: ::prost::alloc::vec::Vec<KeyValue>,
    #[prost(message, repeated, tag = "7")]
    pub hooks: ::prost::alloc::vec::Vec<KeyValue>,
    #[prost(message, optional, tag = "8")]
    pub stats: ::core::option::Option<ObjectGroupStats>,
    #[prost(int64, tag = "9")]
    pub rev_number: i64,
}
///  Multiple ObjectGroupOverviews
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct ObjectGroupOverviews {
    #[prost(message, repeated, tag = "1")]
    pub object_group_overviews: ::prost::alloc::vec::Vec<ObjectGroupOverview>,
}
///  This is a representation of the ObjectGroup with only ObjectIDs instead of
///  full objects
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct ObjectGroupWithId {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub description: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "6")]
    pub labels: ::prost::alloc::vec::Vec<KeyValue>,
    #[prost(message, repeated, tag = "7")]
    pub hooks: ::prost::alloc::vec::Vec<KeyValue>,
    ///  Must be in collection objects
    #[prost(string, repeated, tag = "8")]
    pub object_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    ///  Must be in collection objects
    #[prost(string, repeated, tag = "9")]
    pub meta_object_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(message, optional, tag = "10")]
    pub stats: ::core::option::Option<ObjectGroupStats>,
    #[prost(int64, tag = "11")]
    pub rev_number: i64,
}
///  Multiple ObjectGroupWithIDs
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct ObjectGroupWithIDs {
    #[prost(message, repeated, tag = "1")]
    pub object_group_with_ids: ::prost::alloc::vec::Vec<ObjectGroupWithId>,
}
//  RULES for Collections:
//  1. Each object is "owned" by one/or more collections
//  2. Objects can be in multiple collections and must be in the owner collection
//  3. Collections are either mutable with Version.latest == true or immutable
//  with a fixed version number 3.1 If a collection gets a fixed version a copy
//  is created with all "latest" objects dereferenced to their respective
//  revisions 3.2 Modifying an immutable collection will create a new copy of the
//  collection with a new version number
//  4. Collections can be created by any user, but only the owner can modify or
//  delete them

#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct Collection {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    ///  Should be unique in authgroup
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub description: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "4")]
    pub labels: ::prost::alloc::vec::Vec<KeyValue>,
    #[prost(message, repeated, tag = "5")]
    pub hooks: ::prost::alloc::vec::Vec<KeyValue>,
    ///  Ontology for labels
    #[prost(message, optional, tag = "6")]
    pub label_ontology: ::core::option::Option<LabelOntology>,
    #[prost(message, optional, tag = "7")]
    pub created: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(message, repeated, tag = "8")]
    pub objects: ::prost::alloc::vec::Vec<Object>,
    #[prost(message, repeated, tag = "9")]
    pub specifications: ::prost::alloc::vec::Vec<Object>,
    #[prost(message, repeated, tag = "10")]
    pub object_groups: ::prost::alloc::vec::Vec<ObjectGroup>,
    #[prost(message, optional, tag = "14")]
    pub stats: ::core::option::Option<CollectionStats>,
    #[prost(bool, tag = "15")]
    pub is_public: bool,
    #[prost(oneof = "collection::Version", tags = "12, 13")]
    pub version: ::core::option::Option<collection::Version>,
}
/// Nested message and enum types in `Collection`.
pub mod collection {
    #[derive(Clone, PartialEq, Eq, ::prost::Oneof)]
    pub enum Version {
        #[prost(message, tag = "12")]
        SemanticVersion(super::Version),
        #[prost(bool, tag = "13")]
        Latest(bool),
    }
}
///  Multiple Collections
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct Collections {
    #[prost(message, repeated, tag = "1")]
    pub collections: ::prost::alloc::vec::Vec<Collection>,
}
///  This is a representation of the Collection without the recursive nature of
///  objectreferences
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct CollectionOverview {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub description: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "4")]
    pub labels: ::prost::alloc::vec::Vec<KeyValue>,
    #[prost(message, repeated, tag = "5")]
    pub hooks: ::prost::alloc::vec::Vec<KeyValue>,
    ///  Ontology for labels
    #[prost(message, optional, tag = "6")]
    pub label_ontology: ::core::option::Option<LabelOntology>,
    #[prost(message, optional, tag = "7")]
    pub created: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(message, optional, tag = "14")]
    pub stats: ::core::option::Option<CollectionStats>,
    #[prost(bool, tag = "15")]
    pub is_public: bool,
    #[prost(oneof = "collection_overview::Version", tags = "12, 13")]
    pub version: ::core::option::Option<collection_overview::Version>,
}
/// Nested message and enum types in `CollectionOverview`.
pub mod collection_overview {
    #[derive(Clone, PartialEq, Eq, ::prost::Oneof)]
    pub enum Version {
        #[prost(message, tag = "12")]
        SemanticVersion(super::Version),
        #[prost(bool, tag = "13")]
        Latest(bool),
    }
}
///  Multiple CollectionOverviews
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct CollectionOverviews {
    #[prost(message, repeated, tag = "1")]
    pub collection_overviews: ::prost::alloc::vec::Vec<CollectionOverview>,
}
///  This is a representation of the Collection with only Resource RevisionIDs
///  instead of full objects
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct CollectionWithId {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub description: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "4")]
    pub labels: ::prost::alloc::vec::Vec<KeyValue>,
    #[prost(message, repeated, tag = "5")]
    pub hooks: ::prost::alloc::vec::Vec<KeyValue>,
    ///  Ontology for labels
    #[prost(message, optional, tag = "6")]
    pub label_ontology: ::core::option::Option<LabelOntology>,
    #[prost(message, optional, tag = "7")]
    pub created: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(string, repeated, tag = "8")]
    pub objects: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag = "9")]
    pub specifications: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag = "10")]
    pub object_groups: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(message, optional, tag = "14")]
    pub stats: ::core::option::Option<CollectionStats>,
    #[prost(bool, tag = "15")]
    pub is_public: bool,
    #[prost(oneof = "collection_with_id::Version", tags = "12, 13")]
    pub version: ::core::option::Option<collection_with_id::Version>,
}
/// Nested message and enum types in `CollectionWithID`.
pub mod collection_with_id {
    #[derive(Clone, PartialEq, Eq, ::prost::Oneof)]
    pub enum Version {
        #[prost(message, tag = "12")]
        SemanticVersion(super::Version),
        #[prost(bool, tag = "13")]
        Latest(bool),
    }
}
///  Multiple CollectionWithIDs
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct CollectionWithIDs {
    #[prost(message, repeated, tag = "1")]
    pub collection_with_ids: ::prost::alloc::vec::Vec<CollectionWithId>,
}
///  An arbitrary status for Objects
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum Status {
    Unspecified = 0,
    Initializing = 1,
    Available = 2,
    Unavailable = 3,
    Error = 4,
    Trash = 5,
}
impl Status {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Status::Unspecified => "STATUS_UNSPECIFIED",
            Status::Initializing => "STATUS_INITIALIZING",
            Status::Available => "STATUS_AVAILABLE",
            Status::Unavailable => "STATUS_UNAVAILABLE",
            Status::Error => "STATUS_ERROR",
            Status::Trash => "STATUS_TRASH",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum Hashalgorithm {
    Unspecified = 0,
    Md5 = 1,
    Sha1 = 2,
    Sha256 = 3,
    Sha512 = 4,
    Murmur3a32 = 5,
    Xxhash32 = 6,
}
impl Hashalgorithm {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Hashalgorithm::Unspecified => "HASHALGORITHM_UNSPECIFIED",
            Hashalgorithm::Md5 => "HASHALGORITHM_MD5",
            Hashalgorithm::Sha1 => "HASHALGORITHM_SHA1",
            Hashalgorithm::Sha256 => "HASHALGORITHM_SHA256",
            Hashalgorithm::Sha512 => "HASHALGORITHM_SHA512",
            Hashalgorithm::Murmur3a32 => "HASHALGORITHM_MURMUR3A32",
            Hashalgorithm::Xxhash32 => "HASHALGORITHM_XXHASH32",
        }
    }
}
///  Specifies the Origin of the object
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum OriginType {
    Unspecified = 0,
    ///  User uploaded the object
    User = 1,
    ///  Object was cloned from another object
    Objclone = 2,
}
impl OriginType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            OriginType::Unspecified => "ORIGIN_TYPE_UNSPECIFIED",
            OriginType::User => "ORIGIN_TYPE_USER",
            OriginType::Objclone => "ORIGIN_TYPE_OBJCLONE",
        }
    }
}
///  Dataclass defines the confidentiality of the object
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum DataClass {
    Unspecified = 0,
    Public = 1,
    Private = 2,
    Confidential = 3,
    Protected = 4,
}
impl DataClass {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            DataClass::Unspecified => "DATA_CLASS_UNSPECIFIED",
            DataClass::Public => "DATA_CLASS_PUBLIC",
            DataClass::Private => "DATA_CLASS_PRIVATE",
            DataClass::Confidential => "DATA_CLASS_CONFIDENTIAL",
            DataClass::Protected => "DATA_CLASS_PROTECTED",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SourceType {
    Unspecified = 0,
    Url = 1,
    Doi = 2,
}
impl SourceType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            SourceType::Unspecified => "SOURCE_TYPE_UNSPECIFIED",
            SourceType::Url => "SOURCE_TYPE_URL",
            SourceType::Doi => "SOURCE_TYPE_DOI",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum EndpointType {
    Unspecified = 0,
    S3 = 1,
    File = 2,
}
impl EndpointType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            EndpointType::Unspecified => "ENDPOINT_TYPE_UNSPECIFIED",
            EndpointType::S3 => "ENDPOINT_TYPE_S3",
            EndpointType::File => "ENDPOINT_TYPE_FILE",
        }
    }
}
///  This file contains parameters for queries that return a list of resources.
///  The results are paginated.
///  The page request specifies the page size and last_id.
///  If page_size is not specified, it defaults to 20.
///  If page_size is -1, it returns all objects.
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct PageRequest {
    ///  This is the last ID of the previous returned request
    #[prost(string, tag = "1")]
    pub last_uuid: ::prost::alloc::string::String,
    ///  Default to 20, -1 for all
    #[prost(int64, tag = "2")]
    pub page_size: i64,
}
///  LabelFilter is used to filter resources by labels.
///  The labels are specified as a map of key-value pairs.
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct LabelFilter {
    #[prost(message, repeated, tag = "1")]
    pub labels: ::prost::alloc::vec::Vec<KeyValue>,
    ///  True if and, if empty or false or
    #[prost(bool, tag = "2")]
    pub and_or_or: bool,
    ///  Should only the keys be considered ?
    #[prost(bool, tag = "3")]
    pub keys_only: bool,
}
///  This is a combined query for either a list of resource IDs or filtered by
///  Label Can be expanded in the future to allow for more complex queries
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct LabelOrIdQuery {
    #[prost(message, optional, tag = "1")]
    pub labels: ::core::option::Option<LabelFilter>,
    #[prost(string, repeated, tag = "2")]
    pub ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
///  A Project is a list of collections with associated users
///  This is used to manage access to multiple collections at the same time
///  Each Collection can only be in one Project at a time
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct Project {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "3")]
    pub user_permissions: ::prost::alloc::vec::Vec<ProjectPermission>,
    #[prost(string, repeated, tag = "4")]
    pub collection_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, tag = "5")]
    pub description: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct ProjectOverview {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub description: ::prost::alloc::string::String,
    #[prost(string, repeated, tag = "4")]
    pub collection_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag = "5")]
    pub user_ids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct User {
    ///  Internal Aruna UserID
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    ///  Oidc subject ID
    #[prost(string, tag = "2")]
    pub external_id: ::prost::alloc::string::String,
    ///  (optional) User display_name
    #[prost(string, tag = "3")]
    pub display_name: ::prost::alloc::string::String,
    ///  Is the user activated
    #[prost(bool, tag = "4")]
    pub active: bool,
    ///  Is the user admin ?
    #[prost(bool, tag = "5")]
    pub is_admin: bool,
}
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct Token {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
    #[prost(enumeration = "TokenType", tag = "4")]
    pub token_type: i32,
    #[prost(message, optional, tag = "5")]
    pub created_at: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(message, optional, tag = "6")]
    pub expires_at: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(string, tag = "7")]
    pub collection_id: ::prost::alloc::string::String,
    #[prost(string, tag = "8")]
    pub project_id: ::prost::alloc::string::String,
    #[prost(enumeration = "Permission", tag = "9")]
    pub permission: i32,
}
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct ProjectPermission {
    #[prost(string, tag = "1")]
    pub user_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub project_id: ::prost::alloc::string::String,
    #[prost(enumeration = "Permission", tag = "3")]
    pub permission: i32,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum Permission {
    Unspecified = 0,
    ///  No permissions granted, used for users that are in the
    None = 1,
    ///  project but have no default permissions
    ///
    ///  Read only
    Read = 2,
    ///  Append objects to the collection cannot modify existing objects
    Append = 3,
    ///  Can Read/Append/Modify objects in the collection
    Modify = 4,
    ///  that owns the object / Create new collections
    ///
    ///  Can modify the collections itself and permanently
    Admin = 5,
}
impl Permission {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Permission::Unspecified => "PERMISSION_UNSPECIFIED",
            Permission::None => "PERMISSION_NONE",
            Permission::Read => "PERMISSION_READ",
            Permission::Append => "PERMISSION_APPEND",
            Permission::Modify => "PERMISSION_MODIFY",
            Permission::Admin => "PERMISSION_ADMIN",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum PermType {
    Unspecified = 0,
    ///  Regular OAuth users
    User = 1,
    ///  Anonymous users without an OAuth token
    Anonymous = 2,
    ///  Access token on behalf of a user
    Token = 3,
}
impl PermType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            PermType::Unspecified => "PERM_TYPE_UNSPECIFIED",
            PermType::User => "PERM_TYPE_USER",
            PermType::Anonymous => "PERM_TYPE_ANONYMOUS",
            PermType::Token => "PERM_TYPE_TOKEN",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum TokenType {
    Unspecified = 0,
    Personal = 1,
    Scoped = 2,
}
impl TokenType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            TokenType::Unspecified => "TOKEN_TYPE_UNSPECIFIED",
            TokenType::Personal => "TOKEN_TYPE_PERSONAL",
            TokenType::Scoped => "TOKEN_TYPE_SCOPED",
        }
    }
}
