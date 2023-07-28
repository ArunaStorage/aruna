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
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'KeyValueType') THEN
        CREATE TYPE "KeyValueType" AS ENUM ('LABEL', 'STATIC_LABEL', 'HOOK', 'STATIC_HOOK');
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


/* ----- Authorization --------------------------------------------- */
-- Table with users imported from some aai
-- Join table to map users to multiple identity providers

CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    attributes JSONB NOT NULL,
    secret TEXT NOT NULL
);

/* ----- Object Service -------------------------------------------- */
-- Table with objects which represent individual data blobs
CREATE TABLE IF NOT EXISTS objects (
    id UUID NOT NULL PRIMARY KEY, -- The unique per object id
    revision_number INT NOT NULL,
    name VARCHAR(511) NOT NULL,          -- Filename or subpath
    raw_content_len BIGINT NOT NULL DEFAULT 0,
    disk_content_len BIGINT NOT NULL DEFAULT 0,
    count INT NOT NULL DEFAULT 0,
    key_values JSONB NOT NULL,
    object_status "ObjectStatus" NOT NULL DEFAULT 'INITIALIZING',
    data_class "DataClass" NOT NULL DEFAULT 'PRIVATE',
    object_type "ObjectType" NOT NULL DEFAULT 'PROJECT',
    hashes JSONB NOT NULL DEFAULT '{}',
    dynamic BOOL NOT NULL DEFAULT TRUE,
    endpoints JSONB NOT NULL DEFAULT '{}',
    children JSONB NOT NULL DEFAULT '{}',
    encryption_key TEXT,
    compressed bool NOT NULL DEFAULT FALSE,
    bucket TEXT,
    key TEXT,
    UNIQUE(id, object_type)
);
CREATE INDEX IF NOT EXISTS objects_pk_idx ON objects (id);

-- Table for available pubkeys
CREATE TABLE IF NOT EXISTS pub_keys (
    id SMALLSERIAL PRIMARY KEY, -- This is a serial to make jwt tokens smaller
    proxy_config JSONB NOT NULL DEFAULT '{}',
    pubkey TEXT NOT NULL,
    UNIQUE(pubkey)
);