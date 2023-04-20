-- This file should undo anything in `up.sql`

ALTER TABLE api_tokens DROP COLUMN accesskey;
DROP TABLE encryption_keys;

ALTER TABLE object_locations DROP COLUMN is_encrypted, DROP is_compressed; 