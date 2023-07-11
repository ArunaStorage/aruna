/* ----- Type ENUMs ------------------------------------------------ */
-- All ENUM types have to be created before their usage in a table
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'ObjectStatus') THEN
        CREATE TYPE "ObjectStatus" AS ENUM (
            'INITIALIZING',
            'VALIDATING',
            'AVAILABLE',
            'ERROR',
            'DELETED'
        );
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'DataClass') THEN
        CREATE TYPE "DataClass" AS ENUM ('PUBLIC', 'PRIVATE', 'CONFIDENTIAL');
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'KeyValueType') THEN
        CREATE TYPE "KeyValueType" AS ENUM ('LABEL', 'HOOK');
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'UserRights') THEN
        CREATE TYPE "UserRights" AS ENUM ('READ', 'APPEND', 'CASCADING', 'WRITE', 'ADMIN');
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'ObjectType') THEN
        CREATE TYPE "ObjectType" AS ENUM (
            'PROJECT',
            'COLLECTION',
            'DATASET',
            'OBJECT'
        );
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'HashType') THEN
        CREATE TYPE "HashType" AS ENUM (
            'MD5',
            'SHA256'
        );
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'EndpointStatus') THEN
        CREATE TYPE "EndpointStatus" AS ENUM (
            'INITIALIZING',
            'AVAILABLE',
            'DEGRADED',
            'UNAVAILABLE',
            'MAINTENANCE'
        );
    END IF;
END
$$;

/* ----- Authorization --------------------------------------------- */
-- Table with users imported from some aai
-- Join table to map users to multiple identity providers
CREATE TABLE IF NOT EXISTS identity_providers (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    url TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS external_user_ids (
    id UUID PRIMARY KEY,
    external_id VARCHAR(511) NOT NULL,
    idp_id UUID NOT NULL,
    FOREIGN KEY (idp_id) REFERENCES identity_providers(id)
);

CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    external_idp_id UUID[],
    display_name TEXT NOT NULL DEFAULT '',
    email VARCHAR(511) DEFAULT '',
    attributes JSONB NOT NULL
);

/* ----- Object Service -------------------------------------------- */
-- Table with objects which represent individual data blobs
CREATE TABLE IF NOT EXISTS objects (
    id UUID NOT NULL PRIMARY KEY, -- The unique per object id
    shared_id UUID NOT NULL,             -- A shared ID for all updated versions
    revision_number INT NOT NULL,
    path TEXT NOT NULL,                  -- Filename or subpath
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by UUID NOT NULL REFERENCES users(id),
    content_len BIGINT NOT NULL DEFAULT 0,
    key_values JSONB NOT NULL,
    object_status "ObjectStatus" NOT NULL DEFAULT 'INITIALIZING',
    data_class "DataClass" NOT NULL DEFAULT 'PRIVATE',
    object_type "ObjectType" NOT NULL DEFAULT 'PROJECT',
    external_relations JSONB NOT NULL,
    hashes TEXT[] NOT NULL,
    UNIQUE(shared_id, revision_number)
);
CREATE INDEX IF NOT EXISTS objects_shared_rev_idx ON objects (shared_id, revision_number);
CREATE INDEX IF NOT EXISTS objects_shared_single_idx ON objects (shared_id);
CREATE INDEX IF NOT EXISTS objects_pk_idx ON objects (id);

-- Table with endpoints
CREATE TABLE IF NOT EXISTS endpoints (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    host_config JSONB NOT NULL,
    documentation_object UUID DEFAULT NULL,
    is_public BOOL NOT NULL DEFAULT TRUE,
    pubkey TEXT NOT NULL,
    status "EndpointStatus" NOT NULL DEFAULT 'AVAILABLE'
);

-- Table with object locations which describe
CREATE TABLE IF NOT EXISTS object_locations (
    id UUID PRIMARY KEY,
    endpoint_id UUID NOT NULL REFERENCES endpoints(id),
    object_id UUID NOT NULL,
    is_primary BOOL NOT NULL DEFAULT TRUE,
    -- TRUE if TRUE otherwise NULL
    UNIQUE (object_id, is_primary),
    FOREIGN KEY (object_id) REFERENCES objects(id)
);
/* ----- Object Relations ------------------------------------------ */
-- Table to store custom relation types
CREATE TABLE IF NOT EXISTS relation_types (
    id SMALLSERIAL PRIMARY KEY NOT NULL,
    relation_name VARCHAR(511) NOT NULL 
);

-- Table to store all internal relations between objects
CREATE TABLE IF NOT EXISTS internal_relations (
    id UUID PRIMARY KEY NOT NULL,
    origin_pid UUID REFERENCES objects(id) ON DELETE CASCADE,
    type_id INT NOT NULL REFERENCES relation_types(id) ON DELETE CASCADE,
    target_pid UUID REFERENCES objects(id) ON DELETE CASCADE,
    is_persistent BOOL NOT NULL DEFAULT FALSE
);

-- Table with api tokens which are used to authorize user actions in a specific project and/or collection
CREATE TABLE IF NOT EXISTS api_tokens (
    id UUID PRIMARY KEY NOT NULL,
    user_id UUID NOT NULL,
    pub_key SERIAL NOT NULL,
    name TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    used_at TIMESTAMP NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP,
    object_id UUID DEFAULT NULL,
    user_right "UserRights",
    FOREIGN KEY (object_id) REFERENCES objects(id),
    FOREIGN KEY (user_id) REFERENCES users(id)
);

-- Table for available pubkeys
CREATE TABLE IF NOT EXISTS pub_keys (
    id INT PRIMARY KEY, -- This is a serial to make jwt tokens smaller
    pubkey TEXT NOT NULL
);

/* ----- Notification Service -------------------------------------- */
-- Table for the notification service to persist consumer
CREATE TABLE IF NOT EXISTS notification_stream_groups (
    id UUID PRIMARY KEY,
    resource_id UUID NOT NULL,
    subject TEXT NOT NULL,
    notify_on_sub_resources BOOL NOT NULL DEFAULT FALSE
);
