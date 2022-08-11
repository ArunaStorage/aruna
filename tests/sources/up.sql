DROP DATABASE IF EXISTS test;
CREATE DATABASE test;
USE test;
/* ----- Authentication -------------------------------------------- */
-- Table with different identity providers
CREATE TABLE identity_providers (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    test TEXT NOT NULL DEFAULT 'OIDC'
);