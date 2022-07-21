\c postgres;
DROP DATABASE test;
CREATE DATABASE test;
\c test;

CREATE TYPE OBJECT_STATUS AS ENUM ('INITIALIZING', 'AVAILABLE', 'UNAVAILABLE', 'ERROR');
CREATE TYPE ENDPOINT_TYPE AS ENUM ('S3', 'FILE');
CREATE TYPE DATACLASS AS ENUM ('PUBLIC', 'PRIVATE', 'CONFIDENTIAL', 'PROTECTED');
CREATE TYPE SOURCE_TYPE AS ENUM ('S3', 'URL');
CREATE TYPE KEY_VALUE_TYPE AS ENUM ('LABEL','HOOK');
CREATE TYPE IDP_TYPE AS ENUM ('OIDC');
CREATE TYPE USER_RIGHTS AS ENUM ('READ', 'APPEND', 'MODIFY', 'WRITE', 'ADMIN');
CREATE TYPE RESOURCES AS ENUM ('PROJECT', 'COLLECTION', 'OBJECT', 'OBJECT_GROUP');


CREATE TABLE projects (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    created_at DATE NOT NULL DEFAULT NOW()
);

CREATE TABLE endpoints (
    id UUID PRIMARY KEY,
    endpoint_type ENDPOINT_TYPE NOT NULL,
    proxy_hostname VARCHAR(255) NOT NULL,
    internal_hostname VARCHAR(255) NOT NULL
);

CREATE TABLE sources (
    id UUID PRIMARY KEY,
    link TEXT NOT NULL,
    source_type SOURCE_TYPE NOT NULL
);

CREATE TABLE objects (
    id UUID NOT NULL,
    revision_number INT NOT NULL,
    filename TEXT NOT NULL,
    created_at DATE NOT NULL DEFAULT NOW(),
    content_len BIGINT NOT NULL DEFAULT 0,
    object_status OBJECT_STATUS NOT NULL DEFAULT 'INITIALIZING',
    dataclass DATACLASS NOT NULL DEFAULT 'PRIVATE',
    source_id UUID REFERENCES sources(id), 
    origin_id UUID, 
    origin_revision INT,
    PRIMARY KEY (id, revision_number)
);

-- Because objects cannot reference itself, when created
ALTER TABLE objects ADD FOREIGN KEY (origin_id, origin_revision) REFERENCES objects(id, revision_number);

CREATE INDEX objects_id ON objects (id);


CREATE TABLE hash_type (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    UNIQUE (name)
);

CREATE TABLE object_locations (
    id UUID PRIMARY KEY,
    bucket TEXT NOT NULL,
    path TEXT NOT NULL,
    endpoint_id UUID NOT NULL REFERENCES endpoints(id),
    object_id UUID NOT NULL,
    object_revision INT, 
    is_primary BOOL DEFAULT TRUE, -- TRUE if TRUE otherwise NULL
    UNIQUE (object_id, object_revision, is_primary),
    FOREIGN KEY (object_id, object_revision) REFERENCES objects(id, revision_number)
);


CREATE TABLE hashes (
    id UUID PRIMARY KEY,
    hash TEXT NOT NULL,
    object_id UUID,
    object_revision INT,
    hash_type UUID NOT NULL REFERENCES hash_type(id), 
    FOREIGN KEY (object_id, object_revision) REFERENCES objects(id, revision_number)
);


CREATE TABLE object_key_value (
    id UUID PRIMARY KEY,
    object_id UUID NOT NULL,
    object_revision INT NOT NULL,
    key VARCHAR(255) NOT NULL,
    value VARCHAR(255) NOT NULL,
    key_value_type KEY_VALUE_TYPE NOT NULL,
    FOREIGN KEY (object_id, object_revision) REFERENCES objects(id, revision_number)
);

CREATE TABLE collection_version (
    id UUID PRIMARY KEY,
    major INT NOT NULL,
    minor INT NOT NULL,
    patch INT NOT NULL
);

CREATE TABLE collections(
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    created_at DATE NOT NULL DEFAULT NOW(),
    version_id UUID REFERENCES collection_version(id),
    dataclass DATACLASS,
    project_id UUID NOT NULL,
    FOREIGN KEY (project_id) REFERENCES projects(id)
);

CREATE INDEX major_version_idx ON collection_version (major);
CREATE INDEX minor_version_idx ON collection_version (minor);
CREATE INDEX patch_version_idx ON collection_version (patch);


CREATE TABLE collection_key_value (
    id UUID PRIMARY KEY,
    collection_id UUID NOT NULL REFERENCES collections(id),
    key VARCHAR(255) NOT NULL,
    value VARCHAR(255) NOT NULL,
    key_value_type KEY_VALUE_TYPE NOT NULL
);


CREATE TABLE required_labels (
    id UUID PRIMARY KEY,
    collection_id UUID NOT NULL REFERENCES collections(id),
    label_key VARCHAR(255) NOT NULL
);



CREATE TABLE collection_objects (
    id UUID PRIMARY KEY,
    collection_id UUID NOT NULL,
    object_id UUID NOT NULL,
    object_revision INT,
    is_specification BOOL NOT NULL DEFAULT FALSE,
    writeable BOOL NOT NULL DEFAULT FALSE,
    FOREIGN KEY (object_id, object_revision) REFERENCES objects(id, revision_number), -- Funktioniert das ? JA !
    FOREIGN KEY (collection_id) REFERENCES collections(id)
);

CREATE TABLE object_groups (
    id UUID NOT NULL,
    revision_number INT NOT NULL,
    name TEXT,
    description TEXT,
    created_at DATE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, revision_number)
);

CREATE TABLE collection_object_groups (
    id UUID PRIMARY KEY,
    collection_id UUID NOT NULL,
    object_group_id UUID NOT NULL,
    object_group_revision INT,
    writeable BOOL NOT NULL DEFAULT FALSE, -- True if read_only otherwise false
    FOREIGN KEY (object_group_id, object_group_revision) REFERENCES object_groups(id, revision_number),
    FOREIGN KEY (collection_id) REFERENCES collections(id)
);

CREATE TABLE object_group_key_value (
    id UUID PRIMARY KEY,
    object_group_id UUID NOT NULL,
    object_group_revision INT NOT NULL,
    key VARCHAR(255) NOT NULL,
    value VARCHAR(255) NOT NULL,
    key_value_type KEY_VALUE_TYPE NOT NULL,
    FOREIGN KEY (object_group_id, object_group_revision) REFERENCES object_groups(id, revision_number)
);


CREATE TABLE object_group_objects (
    id UUID PRIMARY KEY,
    object_group_id UUID NOT NULL,
    object_group_revision INT,
    object_id UUID NOT NULL,
    object_revision INT,
    is_meta BOOL NOT NULL DEFAULT FALSE,
    writeable BOOL NOT NULL DEFAULT FALSE,
    FOREIGN KEY (object_id, object_revision) REFERENCES objects(id, revision_number),
    FOREIGN KEY (object_group_id, object_group_revision) REFERENCES object_groups(id, revision_number)
);

CREATE TABLE users (
    id UUID PRIMARY KEY,
    display_name TEXT NOT NULL DEFAULT ''
);

CREATE TABLE idps (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    idp_type IDP_TYPE NOT NULL DEFAULT 'OIDC'
);

CREATE TABLE external_user_ids (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    external_id TEXT NOT NULL,
    idp_id UUID NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (idp_id) REFERENCES idps(id),
    UNIQUE(external_id, idp_id)
);


CREATE TABLE user_permissions (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    user_right USER_RIGHTS NOT NULL DEFAULT 'READ',
    project_id UUID NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (project_id) REFERENCES projects(id)
);


CREATE TABLE api_tokens (
    id UUID PRIMARY KEY,
    creator_user_id UUID NOT NULL,
    token TEXT NOT NULL,
    created_at DATE NOT NULL DEFAULT NOW(),
    expires_at DATE,
    project_id UUID NOT NULL,
    collection_id UUID,
    user_right USER_RIGHTS NOT NULL DEFAULT 'READ',
    FOREIGN KEY (collection_id) REFERENCES collections(id),
    FOREIGN KEY (project_id) REFERENCES projects(id),
    FOREIGN KEY (creator_user_id) REFERENCES users(id)
);


CREATE TABLE notification_stream_groups (
    id UUID PRIMARY KEY,
    subject TEXT NOT NULL,
    resource_id UUID NOT NULL,
    resource_type RESOURCES NOT NULL,
    notify_on_sub_resources BOOL NOT NULL DEFAULT FALSE
);

