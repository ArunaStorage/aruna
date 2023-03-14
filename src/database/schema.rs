// @generated automatically by Diesel CLI.

pub mod sql_types {
    #[derive(diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "dataclass"))]
    pub struct Dataclass;

    #[derive(diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "endpoint_type"))]
    pub struct EndpointType;

    #[derive(diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "hash_type"))]
    pub struct HashType;

    #[derive(diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "identity_provider_type"))]
    pub struct IdentityProviderType;

    #[derive(diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "key_value_type"))]
    pub struct KeyValueType;

    #[derive(diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "object_status"))]
    pub struct ObjectStatus;

    #[derive(diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "reference_status"))]
    pub struct ReferenceStatus;

    #[derive(diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "resources"))]
    pub struct Resources;

    #[derive(diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "source_type"))]
    pub struct SourceType;

    #[derive(diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "user_rights"))]
    pub struct UserRights;
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::UserRights;

    api_tokens (id) {
        id -> Uuid,
        creator_user_id -> Uuid,
        pub_key -> Int8,
        name -> Nullable<Text>,
        created_at -> Timestamp,
        expires_at -> Nullable<Timestamp>,
        project_id -> Nullable<Uuid>,
        collection_id -> Nullable<Uuid>,
        user_right -> Nullable<UserRights>,
        accesskey -> Varchar,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::KeyValueType;

    collection_key_value (id) {
        id -> Uuid,
        collection_id -> Uuid,
        key -> Varchar,
        value -> Varchar,
        key_value_type -> KeyValueType,
    }
}

diesel::table! {
    collection_object_groups (id) {
        id -> Uuid,
        collection_id -> Uuid,
        object_group_id -> Uuid,
        writeable -> Bool,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::ReferenceStatus;

    collection_objects (id) {
        id -> Uuid,
        collection_id -> Uuid,
        object_id -> Uuid,
        is_latest -> Bool,
        auto_update -> Bool,
        is_specification -> Bool,
        writeable -> Bool,
        reference_status -> ReferenceStatus,
    }
}

diesel::table! {
    collection_version (id) {
        id -> Uuid,
        major -> Int8,
        minor -> Int8,
        patch -> Int8,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::Dataclass;

    collections (id) {
        id -> Uuid,
        shared_version_id -> Uuid,
        name -> Text,
        description -> Text,
        created_at -> Timestamp,
        created_by -> Uuid,
        version_id -> Nullable<Uuid>,
        dataclass -> Nullable<Dataclass>,
        project_id -> Uuid,
    }
}

diesel::table! {
    encryption_keys (id) {
        id -> Uuid,
        hash -> Nullable<Text>,
        object_id -> Uuid,
        endpoint_id -> Uuid,
        is_temporary -> Bool,
        encryption_key -> Text,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::EndpointType;

    endpoints (id) {
        id -> Uuid,
        endpoint_type -> EndpointType,
        name -> Text,
        proxy_hostname -> Varchar,
        internal_hostname -> Varchar,
        documentation_path -> Nullable<Text>,
        is_public -> Bool,
    }
}

diesel::table! {
    external_user_ids (id) {
        id -> Uuid,
        user_id -> Uuid,
        external_id -> Text,
        idp_id -> Uuid,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::HashType;

    hashes (id) {
        id -> Uuid,
        hash -> Text,
        object_id -> Uuid,
        hash_type -> HashType,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::IdentityProviderType;

    identity_providers (id) {
        id -> Uuid,
        name -> Text,
        idp_type -> IdentityProviderType,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::Resources;

    notification_stream_groups (id) {
        id -> Uuid,
        subject -> Text,
        resource_id -> Uuid,
        resource_type -> Resources,
        notify_on_sub_resources -> Bool,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::KeyValueType;

    object_group_key_value (id) {
        id -> Uuid,
        object_group_id -> Uuid,
        key -> Varchar,
        value -> Varchar,
        key_value_type -> KeyValueType,
    }
}

diesel::table! {
    object_group_objects (id) {
        id -> Uuid,
        object_id -> Uuid,
        object_group_id -> Uuid,
        is_meta -> Bool,
    }
}

diesel::table! {
    object_groups (id) {
        id -> Uuid,
        shared_revision_id -> Uuid,
        revision_number -> Int8,
        name -> Nullable<Text>,
        description -> Nullable<Text>,
        created_at -> Timestamp,
        created_by -> Uuid,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::KeyValueType;

    object_key_value (id) {
        id -> Uuid,
        object_id -> Uuid,
        key -> Varchar,
        value -> Varchar,
        key_value_type -> KeyValueType,
    }
}

diesel::table! {
    object_locations (id) {
        id -> Uuid,
        bucket -> Text,
        path -> Text,
        endpoint_id -> Uuid,
        object_id -> Uuid,
        is_primary -> Bool,
        is_compressed -> Bool,
        is_encrypted -> Bool,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::ObjectStatus;
    use super::sql_types::Dataclass;

    objects (id) {
        id -> Uuid,
        shared_revision_id -> Uuid,
        revision_number -> Int8,
        filename -> Text,
        created_at -> Timestamp,
        created_by -> Uuid,
        content_len -> Int8,
        object_status -> ObjectStatus,
        dataclass -> Dataclass,
        source_id -> Nullable<Uuid>,
        origin_id -> Uuid,
    }
}

diesel::table! {
    paths (id) {
        id -> Uuid,
        bucket -> Text,
        path -> Text,
        shared_revision_id -> Uuid,
        collection_id -> Uuid,
        created_at -> Timestamp,
        active -> Bool,
    }
}

diesel::table! {
    projects (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        flag -> Int8,
        created_at -> Timestamp,
        created_by -> Uuid,
    }
}

diesel::table! {
    pub_keys (id) {
        id -> Int8,
        pubkey -> Text,
    }
}

diesel::table! {
    required_labels (id) {
        id -> Uuid,
        collection_id -> Uuid,
        label_key -> Varchar,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::SourceType;

    sources (id) {
        id -> Uuid,
        link -> Text,
        source_type -> SourceType,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::UserRights;

    user_permissions (id) {
        id -> Uuid,
        user_id -> Uuid,
        user_right -> UserRights,
        project_id -> Uuid,
    }
}

diesel::table! {
    users (id) {
        id -> Uuid,
        external_id -> Text,
        display_name -> Text,
        active -> Bool,
    }
}

diesel::table! {
    collection_stats (id) {
        id -> Uuid,
        object_count -> Int8,
        object_group_count -> Int8,
        size -> Numeric,
        last_updated -> Timestamp,
    }
}

diesel::table! {
    object_group_stats (id) {
        id -> Uuid,
        object_count -> Int8,
        size -> Numeric,
        last_updated -> Timestamp,
    }
}

diesel::joinable!(api_tokens -> collections (collection_id));
diesel::joinable!(api_tokens -> projects (project_id));
diesel::joinable!(api_tokens -> pub_keys (pub_key));
diesel::joinable!(api_tokens -> users (creator_user_id));
diesel::joinable!(collection_key_value -> collections (collection_id));
diesel::joinable!(collection_object_groups -> collections (collection_id));
diesel::joinable!(collection_object_groups -> object_groups (object_group_id));
diesel::joinable!(collection_objects -> collections (collection_id));
diesel::joinable!(collection_objects -> objects (object_id));
diesel::joinable!(collections -> collection_version (version_id));
diesel::joinable!(collections -> projects (project_id));
diesel::joinable!(collections -> users (created_by));
diesel::joinable!(encryption_keys -> endpoints (endpoint_id));
diesel::joinable!(encryption_keys -> objects (object_id));
diesel::joinable!(external_user_ids -> identity_providers (idp_id));
diesel::joinable!(external_user_ids -> users (user_id));
diesel::joinable!(hashes -> objects (object_id));
diesel::joinable!(object_group_key_value -> object_groups (object_group_id));
diesel::joinable!(object_group_objects -> object_groups (object_group_id));
diesel::joinable!(object_group_objects -> objects (object_id));
diesel::joinable!(object_groups -> users (created_by));
diesel::joinable!(object_key_value -> objects (object_id));
diesel::joinable!(object_locations -> endpoints (endpoint_id));
diesel::joinable!(object_locations -> objects (object_id));
diesel::joinable!(objects -> sources (source_id));
diesel::joinable!(objects -> users (created_by));
diesel::joinable!(paths -> collections (collection_id));
diesel::joinable!(projects -> users (created_by));
diesel::joinable!(required_labels -> collections (collection_id));
diesel::joinable!(user_permissions -> projects (project_id));
diesel::joinable!(user_permissions -> users (user_id));

diesel::allow_tables_to_appear_in_same_query!(
    api_tokens,
    collection_key_value,
    collection_object_groups,
    collection_objects,
    collection_version,
    collections,
    encryption_keys,
    endpoints,
    external_user_ids,
    hashes,
    identity_providers,
    notification_stream_groups,
    object_group_key_value,
    object_group_objects,
    object_groups,
    object_key_value,
    object_locations,
    objects,
    paths,
    projects,
    pub_keys,
    required_labels,
    sources,
    user_permissions,
    users,
);
