table! {
    api_tokens (id) {
        id -> Uuid,
        creator_user_id -> Uuid,
        token -> Text,
        created_at -> Date,
        expires_at -> Nullable<Date>,
        project_id -> Nullable<Uuid>,
        collection_id -> Nullable<Uuid>,
        user_right -> User_rights,
    }
}

table! {
    collection_key_value (id) {
        id -> Uuid,
        collection_id -> Uuid,
        key -> Varchar,
        value -> Varchar,
        key_value_type -> Key_value_type,
    }
}

table! {
    collection_object_groups (id) {
        id -> Uuid,
        collection_id -> Uuid,
        object_group_id -> Uuid,
        object_group_revision -> Nullable<Int8>,
        writeable -> Bool,
    }
}

table! {
    collection_objects (id) {
        id -> Uuid,
        collection_id -> Uuid,
        object_id -> Uuid,
        object_revision -> Nullable<Int8>,
        is_specification -> Bool,
        writeable -> Bool,
    }
}

table! {
    collection_version (id) {
        id -> Uuid,
        major -> Int8,
        minor -> Int8,
        patch -> Int8,
    }
}

table! {
    collections (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        created_at -> Date,
        created_by -> Uuid,
        version_id -> Nullable<Uuid>,
        dataclass -> Nullable<Dataclass>,
        project_id -> Uuid,
    }
}

table! {
    endpoints (id) {
        id -> Uuid,
        endpoint_type -> Endpoint_type,
        proxy_hostname -> Varchar,
        internal_hostname -> Varchar,
        documentation_path -> Nullable<Text>,
        is_public -> Bool,
    }
}

table! {
    external_user_ids (id) {
        id -> Uuid,
        user_id -> Uuid,
        external_id -> Text,
        idp_id -> Uuid,
    }
}

table! {
    hash_type (id) {
        id -> Uuid,
        name -> Varchar,
    }
}

table! {
    hashes (id) {
        id -> Uuid,
        hash -> Text,
        object_id -> Nullable<Uuid>,
        object_revision -> Nullable<Int8>,
        hash_type -> Uuid,
    }
}

table! {
    identity_providers (id) {
        id -> Uuid,
        name -> Text,
        idp_type -> Identity_provider_type,
    }
}

table! {
    notification_stream_groups (id) {
        id -> Uuid,
        subject -> Text,
        resource_id -> Uuid,
        resource_type -> Resources,
        notify_on_sub_resources -> Bool,
    }
}

table! {
    object_group_key_value (id) {
        id -> Uuid,
        object_group_id -> Uuid,
        object_group_revision -> Int8,
        key -> Varchar,
        value -> Varchar,
        key_value_type -> Key_value_type,
    }
}

table! {
    object_group_objects (id) {
        id -> Uuid,
        object_group_id -> Uuid,
        object_group_revision -> Nullable<Int8>,
        object_id -> Uuid,
        object_revision -> Nullable<Int8>,
        is_meta -> Bool,
        writeable -> Bool,
    }
}

table! {
    object_groups (id, revision_number) {
        id -> Uuid,
        revision_number -> Int8,
        name -> Nullable<Text>,
        description -> Nullable<Text>,
        created_at -> Date,
        created_by -> Uuid,
    }
}

table! {
    object_key_value (id) {
        id -> Uuid,
        object_id -> Uuid,
        object_revision -> Int8,
        key -> Varchar,
        value -> Varchar,
        key_value_type -> Key_value_type,
    }
}

table! {
    object_locations (id) {
        id -> Uuid,
        bucket -> Text,
        path -> Text,
        endpoint_id -> Uuid,
        object_id -> Uuid,
        object_revision -> Nullable<Int8>,
        is_primary -> Nullable<Bool>,
    }
}

table! {
    objects (id, revision_number) {
        id -> Uuid,
        revision_number -> Int8,
        filename -> Text,
        created_at -> Date,
        created_by -> Uuid,
        content_len -> Int8,
        object_status -> Object_status,
        dataclass -> Dataclass,
        source_id -> Nullable<Uuid>,
        origin_id -> Nullable<Uuid>,
        origin_revision -> Nullable<Int8>,
    }
}

table! {
    projects (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        created_at -> Date,
        created_by -> Uuid,
    }
}

table! {
    required_labels (id) {
        id -> Uuid,
        collection_id -> Uuid,
        label_key -> Varchar,
    }
}

table! {
    sources (id) {
        id -> Uuid,
        link -> Text,
        source_type -> Source_type,
    }
}

table! {
    user_permissions (id) {
        id -> Uuid,
        user_id -> Uuid,
        user_right -> User_rights,
        project_id -> Uuid,
    }
}

table! {
    users (id) {
        id -> Uuid,
        display_name -> Text,
        active -> Bool,
    }
}

joinable!(api_tokens -> collections (collection_id));
joinable!(api_tokens -> projects (project_id));
joinable!(api_tokens -> users (creator_user_id));
joinable!(collection_key_value -> collections (collection_id));
joinable!(collection_object_groups -> collections (collection_id));
joinable!(collection_objects -> collections (collection_id));
joinable!(collections -> collection_version (version_id));
joinable!(collections -> projects (project_id));
joinable!(collections -> users (created_by));
joinable!(external_user_ids -> identity_providers (idp_id));
joinable!(external_user_ids -> users (user_id));
joinable!(hashes -> hash_type (hash_type));
joinable!(object_groups -> users (created_by));
joinable!(object_locations -> endpoints (endpoint_id));
joinable!(objects -> sources (source_id));
joinable!(objects -> users (created_by));
joinable!(projects -> users (created_by));
joinable!(required_labels -> collections (collection_id));
joinable!(user_permissions -> projects (project_id));
joinable!(user_permissions -> users (user_id));

allow_tables_to_appear_in_same_query!(
    api_tokens,
    collection_key_value,
    collection_object_groups,
    collection_objects,
    collection_version,
    collections,
    endpoints,
    external_user_ids,
    hash_type,
    hashes,
    identity_providers,
    notification_stream_groups,
    object_group_key_value,
    object_group_objects,
    object_groups,
    object_key_value,
    object_locations,
    objects,
    projects,
    required_labels,
    sources,
    user_permissions,
    users,
);
