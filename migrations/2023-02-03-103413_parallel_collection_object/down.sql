-- This file should undo anything in `up.sql`
ALTER TABLE collection_objects ADD CONSTRAINT unique_collection_object UNIQUE (object_id, collection_id);
DROP INDEX parallel_collection_object CASCADE;
