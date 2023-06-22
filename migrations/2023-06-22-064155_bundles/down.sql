-- This file should undo anything in `up.sql`
DROP TABLE IF EXISTS bundles;


ALTER TABLE endpoints DROP COLUMN is_bundler;
ALTER TABLE endpoints DROP COLUMN ssl;