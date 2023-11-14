/* ----- Type ENUMs ------------------------------------------------ */
-- All ENUM types have to be created before their usage in a table
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'ObjectStatus') THEN
        CREATE TYPE "ObjectStatus" AS ENUM (
            'INITIALIZING',
            'VALIDATING',
            'AVAILABLE',
	        'UNAVAILABLE',
            'ERROR',
            'DELETED'
        );
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'DataClass') THEN
        CREATE TYPE "DataClass" AS ENUM ('PUBLIC', 'PRIVATE', 'WORKSPACE', 'CONFIDENTIAL');
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'PermissionLevel') THEN
        CREATE TYPE "PermissionLevel" AS ENUM ('DENY', 'NONE', 'READ', 'APPEND', 'WRITE', 'ADMIN');
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

DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'EndpointVariant') THEN
            CREATE TYPE "EndpointVariant" AS ENUM (
                'PERSISTENT',
                'VOLATILE'
                );
        END IF;
    END
$$;

/*
DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'TriggerType') THEN
            CREATE TYPE "TriggerType" AS ENUM (
                'HOOK_ADDED',
                'RESOURCE_CREATED',
                'LABEL_ADDED',
                'STATIC_LABEL_ADDED',
                'HOOK_STATUS_CHANGED',
                'OBJECT_FINISHED'
                );
        END IF;
    END
$$;
*/

DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'PersistentNotificationVariant') THEN
            CREATE TYPE "PersistentNotificationVariant" AS ENUM (
                'ACCESS_REQUESTED',
                'PERMISSION_GRANTED',
                'PERMISSION_REVOKED',
                'PERMISSION_UPDATED',
                'ANNOUNCEMENT'
                );
        END IF;
    END
$$;

/* ----- Authorization --------------------------------------------- */
-- Table with users imported from some aai
-- Join table to map users to multiple identity providers

CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    display_name TEXT NOT NULL DEFAULT '',
    email VARCHAR(511) NOT NULL DEFAULT '',
    attributes JSONB NOT NULL,
    active BOOL NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS identity_providers (
    issuer_name VARCHAR(255) PRIMARY KEY,
    jwks_endpoint VARCHAR(255),
    audiences VARCHAR(255)[] NOT NULL
);

/* ----- Licenses -------------------------------------- */
-- Table for licenses
CREATE TABLE IF NOT EXISTS licenses (
    tag VARCHAR(511) PRIMARY KEY NOT NULL, -- Common license abbreviation
    name VARCHAR(511) NOT NULL,            -- Full name of the license
    text TEXT NOT NULL,                    -- Full license text
    url VARCHAR(2047) NOT NULL             -- URL to full license text
);

/* ----- Object Service -------------------------------------------- */
-- Table with objects which represent individual data blobs
CREATE TABLE IF NOT EXISTS objects (
    id UUID NOT NULL PRIMARY KEY, -- The unique per object id
    revision_number INT NOT NULL,
    name VARCHAR(511) NOT NULL,          -- Filename or subpath
    description VARCHAR(1023) NOT NULL,                 
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by UUID NOT NULL REFERENCES users(id),
    content_len BIGINT NOT NULL DEFAULT 0,
    count INT NOT NULL DEFAULT 0,
    key_values JSONB NOT NULL,
    object_status "ObjectStatus" NOT NULL DEFAULT 'INITIALIZING',
    data_class "DataClass" NOT NULL DEFAULT 'PRIVATE',
    object_type "ObjectType" NOT NULL DEFAULT 'PROJECT',
    external_relations JSONB NOT NULL,
    hashes JSONB NOT NULL DEFAULT '{}',
    dynamic BOOL NOT NULL DEFAULT TRUE,
    endpoints JSONB NOT NULL DEFAULT '{}',
    metadata_license VARCHAR(511) NOT NULL REFERENCES licenses(tag),
    data_license VARCHAR(511) NOT NULL REFERENCES licenses(tag),
    UNIQUE(id, object_type)
);
CREATE INDEX IF NOT EXISTS objects_pk_idx ON objects (id);

-- Table with endpoints
CREATE TABLE IF NOT EXISTS endpoints (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    host_config JSONB NOT NULL,
    endpoint_variant "EndpointVariant" NOT NULL DEFAULT 'PERSISTENT',
    documentation_object UUID REFERENCES objects(id),
    is_public BOOL NOT NULL DEFAULT TRUE,
    status "EndpointStatus" NOT NULL DEFAULT 'AVAILABLE',
    UNIQUE(name)

);

/* ----- Object Relations ------------------------------------------ */
-- Table to store custom relation types
CREATE TABLE IF NOT EXISTS relation_types (
    --id SMALLSERIAL PRIMARY KEY NOT NULL,
    relation_name VARCHAR(511) PRIMARY KEY NOT NULL 
);

-- Table to store all internal relations between objects
CREATE TABLE IF NOT EXISTS internal_relations (
    id UUID PRIMARY KEY NOT NULL,
    origin_pid UUID NOT NULL,
    origin_type "ObjectType" NOT NULL,
    relation_name VARCHAR(511) REFERENCES relation_types(relation_name),
    target_pid UUID REFERENCES objects(id) ON DELETE CASCADE,
    target_type "ObjectType" NOT NULL,
    target_name VARCHAR(511) NOT NULL,
    FOREIGN KEY (origin_pid, origin_type) REFERENCES objects(id, object_type) ON DELETE CASCADE,
    FOREIGN KEY (target_pid, target_type) REFERENCES objects(id, object_type) ON DELETE CASCADE,
    UNIQUE(origin_pid, relation_name, target_pid)
    --UNIQUE(origin_pid, relation_name, target_name)
);

CREATE INDEX IF NOT EXISTS origin_pid_idx ON internal_relations (origin_pid);
CREATE INDEX IF NOT EXISTS target_pid_idx ON internal_relations (target_pid);

-- Table for available pubkeys
CREATE TABLE IF NOT EXISTS pub_keys (
    id SMALLSERIAL PRIMARY KEY, -- This is a serial to make jwt tokens smaller
    proxy UUID REFERENCES endpoints(id) ON DELETE CASCADE,
    pubkey TEXT NOT NULL,
    UNIQUE(pubkey)
);

/* ----- Notification Service -------------------------------------- */
-- Table for the notification service to persist consumer
CREATE TABLE IF NOT EXISTS stream_consumers (
    id UUID PRIMARY KEY,
    user_id UUID REFERENCES users(id),
    config JSONB NOT NULL
);

-- Table for persistent notifications which are delivered outside the message broker
CREATE TABLE IF NOT EXISTS persistent_notifications (
    id UUID PRIMARY KEY,
    user_id UUID REFERENCES users(id),
    notification_variant "PersistentNotificationVariant" NOT NULL,
    message TEXT NOT NULL,
    refs JSONB NOT NULL
);

/* ----- Hooks -------------------------------------- */
-- Table for persisting hooks 
CREATE TABLE IF NOT EXISTS hooks (
    id UUID PRIMARY KEY NOT NULL,
    name VARCHAR(511) NOT NULL,
    description VARCHAR(1023) NOT NULL,
    project_ids UUID[],
    owner UUID REFERENCES users(id) ON DELETE CASCADE,
    trigger JSONB NOT NULL,
    /*
    trigger_key VARCHAR(511) NOT NULL,
    trigger_value VARCHAR(511) NOT NULL,
     */
    timeout TIMESTAMP NOT NULL,
    hook JSONB NOT NULL
);

/* ----- Workspaces -------------------------------------- */
-- Table for workspace templates
CREATE TABLE IF NOT EXISTS workspaces (
    id UUID PRIMARY KEY NOT NULL,
    name VARCHAR(511) NOT NULL,
    description VARCHAR(1023) NOT NULL,
    owner UUID REFERENCES users(id) ON DELETE CASCADE,
    prefix VARCHAR(511) NOT NULL,
    hook_ids JSONB,
    endpoint_ids JSONB,
    UNIQUE(name)
);
-- Insert predefined relation types
INSERT INTO relation_types (relation_name) VALUES ('BELONGS_TO'), ('VERSION'), ('METADATA'), ('ORIGIN'), ('POLICY') ON CONFLICT (relation_name) DO NOTHING;
-- Create partial unique index for BELONGS_TO relations only
CREATE UNIQUE INDEX IF NOT EXISTS belongs_to_idx ON internal_relations (origin_pid, relation_name, target_name) WHERE relation_name = ('BELONGS_TO')
