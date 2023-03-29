-- Your SQL goes here

ALTER TABLE api_tokens ADD secretkey VARCHAR(255) NOT NULL DEFAULT '-';

CREATE TABLE encryption_keys (
    id UUID PRIMARY KEY,
    hash TEXT,
    object_id UUID NOT NULL,
    endpoint_id UUID NOT NULL,
    is_temporary BOOL NOT NULL DEFAULT FALSE,
    encryption_key TEXT NOT NULL,
    FOREIGN KEY (object_id) REFERENCES objects(id),
    FOREIGN KEY (endpoint_id) REFERENCES endpoints(id)
);

ALTER TABLE object_locations 
ADD is_compressed BOOL NOT NULL DEFAULT TRUE,
ADD is_encrypted BOOL NOT NULL DEFAULT TRUE;