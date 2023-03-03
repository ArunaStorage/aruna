-- Your SQL goes here

ALTER TABLE api_tokens ADD accesskey VARCHAR(255) NOT NULL DEFAULT '-';

CREATE TABLE encryption_keys (
    id UUID PRIMARY KEY,
    hash TEXT NOT NULL,
    encryption_key TEXT NOT NULL
);

ALTER TABLE object_locations 
ADD is_compressed BOOL NOT NULL DEFAULT TRUE,
ADD is_encrypted BOOL NOT NULL DEFAULT TRUE;