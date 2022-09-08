DROP DATABASE IF EXISTS test;
CREATE DATABASE test;
USE test;
/* ----- Type ENUMs ------------------------------------------------ */
-- All ENUM types have to be created before their usage in a table
CREATE TYPE OBJECT_STATUS AS ENUM (
    'INITIALIZING',
    'AVAILABLE',
    'UNAVAILABLE',
    'ERROR'
);
CREATE TYPE ENDPOINT_TYPE AS ENUM ('S3', 'FILE');
CREATE TYPE DATACLASS AS ENUM ('PUBLIC', 'PRIVATE', 'CONFIDENTIAL', 'PROTECTED');
CREATE TYPE SOURCE_TYPE AS ENUM ('S3', 'URL', 'DOI');
CREATE TYPE KEY_VALUE_TYPE AS ENUM ('LABEL', 'HOOK');
CREATE TYPE IDENTITY_PROVIDER_TYPE AS ENUM ('OIDC');
CREATE TYPE USER_RIGHTS AS ENUM ('READ', 'APPEND', 'MODIFY', 'WRITE', 'ADMIN');
CREATE TYPE RESOURCES AS ENUM (
    'PROJECT',
    'COLLECTION',
    'OBJECT',
    'OBJECT_GROUP'
);
CREATE TYPE HASH_TYPE AS ENUM (
    'MD5',
    'SHA1',
    'SHA256',
    'SHA512',
    'MURMUR3A32',
    'XXHASH32'
);
/* ----- Authentication -------------------------------------------- */
-- Table with different identity providers
CREATE TABLE identity_providers (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    idp_type IDENTITY_PROVIDER_TYPE NOT NULL DEFAULT 'OIDC'
);
-- Table with users imported from some aai
CREATE TABLE users (
    id UUID PRIMARY KEY,
    display_name TEXT NOT NULL DEFAULT '',
    active BOOL NOT NULL DEFAULT FALSE -- Users must be activated by an administrator
);
-- Join table to map users to multiple identity providers
CREATE TABLE external_user_ids (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    external_id TEXT NOT NULL,
    idp_id UUID NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (idp_id) REFERENCES identity_providers(id),
    UNIQUE(external_id, idp_id)
);
-- Table with projects which acts as logical space for collections
CREATE TABLE projects (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    flag INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by UUID NOT NULL,
    FOREIGN KEY (created_by) REFERENCES users(id)
);
-- Table with user permissions bound to a specific project
CREATE TABLE user_permissions (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    user_right USER_RIGHTS NOT NULL DEFAULT 'READ',
    project_id UUID NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (project_id) REFERENCES projects(id)
);
/* ----- Collections ----------------------------------------------- */
-- Table with the individual parts of semantic versioning
CREATE TABLE collection_version (
    id UUID PRIMARY KEY,
    major INT NOT NULL,
    minor INT NOT NULL,
    patch INT NOT NULL
);
CREATE INDEX major_version_idx ON collection_version (major);
CREATE INDEX minor_version_idx ON collection_version (minor);
CREATE INDEX patch_version_idx ON collection_version (patch);
-- Table with collections which act as a container for Objects
CREATE TABLE collections (
    id UUID PRIMARY KEY,
    -- Each collection has a shared_version_id / 
    -- This is an internal id used to determine collections that are versions of each other
    -- If Version_id is NULL this is the "latest" if not the specific version will be displayed
    -- There is a Unique constraint on shared_version_id AND version_id including NULL
    shared_version_id UUID NOT NULL,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by UUID NOT NULL,
    version_id UUID REFERENCES collection_version(id),
    dataclass DATACLASS,
    project_id UUID NOT NULL,
    FOREIGN KEY (project_id) REFERENCES projects(id),
    FOREIGN KEY (created_by) REFERENCES users(id)
);
-- Table with the key-value pairs associated with specific collections
CREATE TABLE collection_key_value (
    id UUID PRIMARY KEY,
    collection_id UUID NOT NULL REFERENCES collections(id),
    key VARCHAR(255) NOT NULL,
    value VARCHAR(255) NOT NULL,
    key_value_type KEY_VALUE_TYPE NOT NULL
);
-- Table with label keys for specific collections which will be enforced
CREATE TABLE required_labels (
    id UUID PRIMARY KEY,
    collection_id UUID NOT NULL REFERENCES collections(id),
    label_key VARCHAR(255) NOT NULL
);
/* ----- Objects --------------------------------------------------- */
-- Table with objects sources
CREATE TABLE sources (
    id UUID PRIMARY KEY,
    link TEXT NOT NULL,
    source_type SOURCE_TYPE NOT NULL
);
-- Table with objects which represent individual data blobs
CREATE TABLE objects (
    -- The unique per object id
    id UUID NOT NULL,
    shared_revision_id UUID NOT NULL,
    revision_number INT NOT NULL,
    filename TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by UUID NOT NULL,
    content_len BIGINT NOT NULL DEFAULT 0,
    object_status OBJECT_STATUS NOT NULL DEFAULT 'INITIALIZING',
    dataclass DATACLASS NOT NULL DEFAULT 'PRIVATE',
    source_id UUID REFERENCES sources(id),
    origin_id UUID,
    PRIMARY KEY (id),
    FOREIGN KEY (created_by) REFERENCES users(id)
);
ALTER TABLE objects
ADD FOREIGN KEY (origin_id) REFERENCES objects(id);
-- objects table cannot reference itself until created
CREATE INDEX objects_id_idx ON objects (shared_revision_id, revision_number);
-- Table with endpoints
CREATE TABLE endpoints (
    id UUID PRIMARY KEY,
    endpoint_type ENDPOINT_TYPE NOT NULL,
    proxy_hostname VARCHAR(255) NOT NULL,
    internal_hostname VARCHAR(255) NOT NULL,
    documentation_path TEXT DEFAULT NULL,
    is_public BOOL NOT NULL DEFAULT TRUE
);
-- Table with object locations which describe
CREATE TABLE object_locations (
    id UUID PRIMARY KEY,
    bucket TEXT NOT NULL,
    path TEXT NOT NULL,
    endpoint_id UUID NOT NULL REFERENCES endpoints(id),
    -- Referencing the internal ID NOT the object_id 
    object_id UUID NOT NULL,
    is_primary BOOL NOT NULL DEFAULT TRUE,
    -- TRUE if TRUE otherwise NULL
    UNIQUE (object_id, is_primary),
    FOREIGN KEY (object_id) REFERENCES objects(id)
);
-- Table with hash checksums associated with specific objects
CREATE TABLE hashes (
    id UUID PRIMARY KEY,
    hash TEXT NOT NULL,
    object_id UUID NOT NULL,
    hash_type HASH_TYPE NOT NULL,
    FOREIGN KEY (object_id) REFERENCES objects(id)
);
-- Table with the key-value pairs associated with specific objects
CREATE TABLE object_key_value (
    id UUID PRIMARY KEY,
    object_id UUID NOT NULL,
    key VARCHAR(255) NOT NULL,
    value VARCHAR(255) NOT NULL,
    key_value_type KEY_VALUE_TYPE NOT NULL,
    FOREIGN KEY (object_id) REFERENCES objects(id)
);
/* ----- ObjectGroups ---------------------------------------------- */
-- Table with object groups which act as a single level organization for objects in collections
CREATE TABLE object_groups (
    -- This is the internal ID used to specify a unique object_group
    -- Users only get the object_group_id
    id UUID NOT NULL,
    shared_revision_id UUID NOT NULL,
    revision_number INT NOT NULL,
    name TEXT,
    description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by UUID NOT NULL,
    PRIMARY KEY (id),
    UNIQUE(shared_revision_id, revision_number),
    FOREIGN KEY (created_by) REFERENCES users(id)
);
-- Table with the key-value pairs associated with specific object groups
CREATE TABLE object_group_key_value (
    id UUID PRIMARY KEY,
    object_group_id UUID NOT NULL,
    key VARCHAR(255) NOT NULL,
    value VARCHAR(255) NOT NULL,
    key_value_type KEY_VALUE_TYPE NOT NULL,
    FOREIGN KEY (object_group_id) REFERENCES object_groups(id)
);
/* ----- Join Tables ----------------------------------------------- */
-- Join table between collections and objects
CREATE TABLE collection_objects (
    id UUID PRIMARY KEY,
    collection_id UUID NOT NULL,
    object_id UUID NOT NULL,
    is_specification BOOL NOT NULL DEFAULT FALSE,
    writeable BOOL NOT NULL DEFAULT FALSE,
    FOREIGN KEY (object_id) REFERENCES objects(id),
    -- Funktioniert das ? JA !
    FOREIGN KEY (collection_id) REFERENCES collections(id)
);
-- Join table between collections and object_groups
CREATE TABLE collection_object_groups (
    id UUID PRIMARY KEY,
    collection_id UUID NOT NULL,
    object_group_id UUID NOT NULL,
    writeable BOOL NOT NULL DEFAULT FALSE,
    -- True if read_only otherwise false
    FOREIGN KEY (object_group_id) REFERENCES object_groups(id),
    FOREIGN KEY (collection_id) REFERENCES collections(id)
);
-- Join table between objects and object_groups
CREATE TABLE object_group_objects (
    id UUID PRIMARY KEY,
    object_id UUID NOT NULL,
    object_group_id UUID NOT NULL,
    is_meta BOOL NOT NULL DEFAULT FALSE,
    writeable BOOL NOT NULL DEFAULT FALSE,
    FOREIGN KEY (object_id) REFERENCES objects(id),
    FOREIGN KEY (object_group_id) REFERENCES object_groups(id)
);
/* ----- Authorization --------------------------------------------- */
-- Table with api tokens which are used to authorize user actions in a specific project and/or collection
CREATE TABLE api_tokens (
    id UUID PRIMARY KEY,
    creator_user_id UUID NOT NULL,
    token TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP,
    project_id UUID,
    -- IF collection_id and project_id is NULL, the token is a global personal token of creator_user_id
    collection_id UUID,
    user_right USER_RIGHTS,
    FOREIGN KEY (collection_id) REFERENCES collections(id),
    FOREIGN KEY (project_id) REFERENCES projects(id),
    FOREIGN KEY (creator_user_id) REFERENCES users(id)
);
/* ----- Notification Service -------------------------------------- */
-- Table for the notification service to fire events
CREATE TABLE notification_stream_groups (
    id UUID PRIMARY KEY,
    subject TEXT NOT NULL,
    resource_id UUID NOT NULL,
    resource_type RESOURCES NOT NULL,
    notify_on_sub_resources BOOL NOT NULL DEFAULT FALSE
);
/* ----- Materialized Views --------------------------------------- */
-- Materialized view for the collections table
CREATE MATERIALIZED VIEW collection_stats AS
SELECT col.id AS id,
    COUNT(obj.id) AS object_count,
    SUM(obj.content_len) AS size,
    now() AS last_updated
FROM collections AS col
    JOIN collection_objects AS cobj ON col.id = cobj.collection_id
    JOIN objects AS obj ON cobj.object_id = obj.id
GROUP BY col.id;
-- Materialized view for the object_groups table
CREATE MATERIALIZED VIEW object_group_stats AS
SELECT objgrp.id AS id,
    COUNT(obj.id) AS object_count,
    SUM(obj.content_len) AS size,
    now() AS last_updated
FROM object_groups AS objgrp
    JOIN object_group_objects AS objgrpobj ON objgrp.id = objgrpobj.object_group_id
    JOIN objects AS obj ON objgrpobj.object_id = obj.id
GROUP BY objgrp.id;
-- Insert initial data
INSERT INTO users (id, display_name, active)
VALUES (
        '12345678-1234-1234-1234-111111111111',
        'admin',
        TRUE
    );
INSERT INTO projects (id, name, description, flag, created_by)
VALUES (
        '12345678-1111-1111-1111-111111111111',
        'admin_project',
        'admin description',
        1,
        '12345678-1234-1234-1234-111111111111'
    );
INSERT INTO user_permissions (id, user_id, user_right, project_id)
VALUES (
        '12345678-9999-9999-9999-999999999999',
        '12345678-1234-1234-1234-111111111111',
        'ADMIN',
        '12345678-1111-1111-1111-111111111111'
    );
INSERT INTO api_tokens (id, creator_user_id, token)
VALUES (
        '12345678-8888-8888-8888-999999999999',
        '12345678-1234-1234-1234-111111111111',
        'super-secret-admin-token'
    );