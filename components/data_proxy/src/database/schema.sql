/* ----- Authorization --------------------------------------------- */
-- Table with users imported from some aai
-- Join table to map users to multiple identity providers

CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    attributes JSONB NOT NULL,
    secret TEXT NOT NULL
);



CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    data bytea NOT NULL, -- The actual data
);

CREATE TABLE IF NOT EXISTS objects (
    id UUID NOT NULL PRIMARY KEY, -- The unique per object id
    data bytea NOT NULL, -- The actual data
);

CREATE TABLE IF NOT EXISTS pub_keys (
    id UUID NOT NULL PRIMARY KEY, -- The unique per object id
    data bytea NOT NULL, -- The actual data
);
-- Table for available pubkeys
CREATE TABLE IF NOT EXISTS pub_keys (
    id SMALLSERIAL PRIMARY KEY, -- This is a serial to make jwt tokens smaller
    proxy_config JSONB NOT NULL DEFAULT '{}',
    pubkey TEXT NOT NULL,
    UNIQUE(pubkey)
);