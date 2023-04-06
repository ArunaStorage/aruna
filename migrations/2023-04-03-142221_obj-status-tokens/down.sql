-- This file should undo anything in `up.sql`

ALTER TYPE OBJECT_STATUS DROP VALUE 'FINALIZING';
DROP TYPE ENDPOINT_STATUS;
ALTER TABLE endpoints DROP COLUMN status;
ALTER TABLE users DROP COLUMN is_service_account;
ALTER TABLE users DROP COLUMN email;
ALTER TABLE api_tokens DROP COLUMN used_at;
ALTER TABLE api_tokens DROP COLUMN is_session;