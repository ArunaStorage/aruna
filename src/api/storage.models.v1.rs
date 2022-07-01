/// A key value pair for hooks and labels
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KeyValue {
    #[prost(string, tag="1")]
    pub key: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub value: ::prost::alloc::string::String,
}
/// S3 location(s) for an object
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Location {
    /// default location
    #[prost(message, optional, tag="1")]
    pub default: ::core::option::Option<ObjectLocation>,
    /// list of locations
    #[prost(message, repeated, tag="2")]
    pub locations: ::prost::alloc::vec::Vec<ObjectLocation>,
}
/// A location in S3
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ObjectLocation {
    #[prost(oneof="object_location::Location", tags="1, 2")]
    pub location: ::core::option::Option<object_location::Location>,
}
/// Nested message and enum types in `ObjectLocation`.
pub mod object_location {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Location {
        #[prost(string, tag="1")]
        LocationId(::prost::alloc::string::String),
        #[prost(message, tag="2")]
        S3Location(super::S3Location),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct S3Location {
    /// Bucket in S3
    #[prost(string, tag="1")]
    pub bucket: ::prost::alloc::string::String,
    /// Key in S3
    #[prost(string, tag="2")]
    pub key: ::prost::alloc::string::String,
    /// Object storage endpoint
    #[prost(string, tag="3")]
    pub url: ::prost::alloc::string::String,
}
/// Stats for a set of objects
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Stats {
    #[prost(int64, tag="1")]
    pub count: i64,
    #[prost(int64, tag="2")]
    pub acc_size: i64,
    #[prost(double, tag="3")]
    pub avg_object_size: f64,
}
/// Stats for a collection
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CollectionStats {
    #[prost(message, optional, tag="1")]
    pub object_stats: ::core::option::Option<Stats>,
    #[prost(int64, tag="2")]
    pub object_group_count: i64,
}
/// Stats for an object group
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ObjectGroupStats {
    #[prost(message, optional, tag="1")]
    pub total_stats: ::core::option::Option<Stats>,
    #[prost(message, optional, tag="2")]
    pub object_stats: ::core::option::Option<Stats>,
    #[prost(message, optional, tag="3")]
    pub meta_object_stats: ::core::option::Option<Stats>,
}
/// Semver version -> Alpha Beta release are not supported -> Use "latest" for mutable collections that are in development
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Version {
    #[prost(int32, tag="1")]
    pub major: i32,
    #[prost(int32, tag="2")]
    pub minor: i32,
    #[prost(int32, tag="3")]
    pub patch: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Authorization {
    #[prost(string, tag="1")]
    pub id: ::prost::alloc::string::String,
    #[prost(enumeration="Permission", tag="2")]
    pub permission: i32,
    #[prost(enumeration="PermType", tag="3")]
    pub perm_type: i32,
    /// Can be userid, tokenid or anonymous id depending on perm_type
    #[prost(string, tag="4")]
    pub client_id: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HashType {
    #[prost(enumeration="Hashalgorithm", tag="1")]
    pub alg: i32,
    #[prost(string, tag="2")]
    pub hash: ::prost::alloc::string::String,
}
// RULES for Objects:
// 1.  Each object is "owned" by exactly one collection
// 2.  Objects can be "borrowed" to multiple other collections
// 3.  Objects are immutable, updating an object will create a new object with increased revision number
//     only people with modify permissions in the owner collection can update an object
// 3.1 Special cases: 
//     Hooks: Can be added/removed and modified without changing the object revision number
//     Labels: Can be added without changing the object revision number, removing or modifying labels WILL change the object revision number (append only)
//     auto_update: Can be added/removed without changing the object revision number and is collection specific
// 4.  Objects can only be permanently deleted by a person with admin rights on the owner collection

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Object {
    ///ObjectID -> This is not unique across the database -> Composite key with revision
    #[prost(string, tag="1")]
    pub id: ::prost::alloc::string::String,
    /// Filename: Name of the original file e.g.: mydata.json
    #[prost(string, tag="2")]
    pub filename: ::prost::alloc::string::String,
    /// Labels to additionally describe the object
    #[prost(message, repeated, tag="4")]
    pub labels: ::prost::alloc::vec::Vec<KeyValue>,
    /// Hooks to be executed on the object
    #[prost(message, repeated, tag="5")]
    pub hooks: ::prost::alloc::vec::Vec<KeyValue>,
    #[prost(message, optional, tag="6")]
    pub created: ::core::option::Option<::prost_types::Timestamp>,
    /// Each object is owned by exactly one collection
    #[prost(string, tag="7")]
    pub collection_id: ::prost::alloc::string::String,
    /// TODO: Does the User need to know where the object is stored? URL ? Standort ?
    ///
    /// Location of the data
    #[prost(message, optional, tag="8")]
    pub location: ::core::option::Option<Location>,
    /// Lenght of the stored dataset
    #[prost(int64, tag="9")]
    pub content_len: i64,
    #[prost(enumeration="Status", tag="10")]
    pub status: i32,
    /// MD5 hash of the data TODO: Aufblasen auf mehrere Hash-Funktionen
    #[prost(string, tag="11")]
    pub hash: ::prost::alloc::string::String,
    /// Increasing revion number for each update -> This is used in the database
    #[prost(int64, tag="12")]
    pub rev_number: i64,
    /// Is this the latest version of the object?
    #[prost(bool, tag="13")]
    pub latest: bool,
    /// This is a collection specific attribute
    /// Must be false if collection is immutable
    ///
    /// If true, the object will be updated automatically when the data is changed
    #[prost(bool, tag="14")]
    pub auto_update: bool,
}
/// ObjectGroups are optional and can be used to group objects in a collection together
/// They need to refer to objects in the same collection
/// Objectgroups can be changed if the collection is mutable
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ObjectGroup {
    #[prost(string, tag="1")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag="3")]
    pub description: ::prost::alloc::string::String,
    /// Exactly one collection must be specified
    #[prost(string, tag="4")]
    pub collection_id: ::prost::alloc::string::String,
    /// TODO: Is this really needed ?
    #[prost(message, repeated, tag="6")]
    pub labels: ::prost::alloc::vec::Vec<KeyValue>,
    /// TODO: Is this really needed ?
    #[prost(message, repeated, tag="7")]
    pub hooks: ::prost::alloc::vec::Vec<KeyValue>,
    /// Must be in collection objects
    #[prost(message, repeated, tag="8")]
    pub objects: ::prost::alloc::vec::Vec<Object>,
    /// Must be in collection objects
    #[prost(message, repeated, tag="9")]
    pub meta_objects: ::prost::alloc::vec::Vec<Object>,
    #[prost(message, optional, tag="10")]
    pub stats: ::core::option::Option<ObjectGroupStats>,
    #[prost(int64, tag="11")]
    pub rev_number: i64,
}
// RULES for Collections:
// 1. Each object is "owned" by exactly one collection
// 2. Objects can be in multiple collections and must be in the owner collection
// 3. Collections are either mutable with Version.latest == true or immutable with a fixed version number
// 3.1 If a collection gets a fixed version a copy is created with all "latest" objects dereferenced to their respective revisions
// 3.2 Modifying an immutable collection will create a new copy of the collection with a new version number
// 4. Collections can be created by any user, but only the owner can modify or delete them

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Collection {
    #[prost(string, tag="1")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag="3")]
    pub description: ::prost::alloc::string::String,
    #[prost(message, repeated, tag="4")]
    pub labels: ::prost::alloc::vec::Vec<KeyValue>,
    #[prost(message, repeated, tag="5")]
    pub hooks: ::prost::alloc::vec::Vec<KeyValue>,
    #[prost(message, optional, tag="6")]
    pub created: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(message, repeated, tag="7")]
    pub objects: ::prost::alloc::vec::Vec<Object>,
    #[prost(message, repeated, tag="8")]
    pub collection_specification: ::prost::alloc::vec::Vec<Object>,
    #[prost(message, repeated, tag="9")]
    pub object_groups: ::prost::alloc::vec::Vec<ObjectGroup>,
    #[prost(message, repeated, tag="10")]
    pub authorization: ::prost::alloc::vec::Vec<Authorization>,
    #[prost(message, optional, tag="13")]
    pub stats: ::core::option::Option<CollectionStats>,
    #[prost(bool, tag="14")]
    pub is_public: bool,
    #[prost(oneof="collection::Version", tags="11, 12")]
    pub version: ::core::option::Option<collection::Version>,
}
/// Nested message and enum types in `Collection`.
pub mod collection {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Version {
        #[prost(message, tag="11")]
        Version(super::Version),
        #[prost(bool, tag="12")]
        Latest(bool),
    }
}
/// An arbitrary status for Objects
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum Status {
    Initializing = 0,
    Available = 1,
    Unavailable = 2,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum Permission {
    /// Read only
    PermRead = 0,
    /// Append objects to the collection cannot modify existing objects
    PermAppend = 1,
    /// Can Read/Append/Modify objects in the collection that owns the object
    PermModify = 2,
    /// Can modify the collection itself and permanently delete owned objects / move ownership of objects
    PermAdmin = 3,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum PermType {
    /// Regular OAuth users
    User = 0,
    /// Anonymous users without an OAuth token
    Anonymous = 1,
    /// Access token on behalf of a user
    Token = 2,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum Hashalgorithm {
    HashalgoMd5 = 0,
    HashalgoSha1 = 1,
    HashalgoSha256 = 2,
    HashalgoSha512 = 3,
}
