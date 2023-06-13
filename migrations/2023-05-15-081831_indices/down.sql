-- This file should undo anything in `up.sql`
CREATE INDEX objects_id_idx ON objects (shared_revision_id, revision_number);
DROP INDEX objects_shared_rev_idx;
DROP INDEX objects_shared_single_idx;
DROP INDEX hashes_objects_idx;
DROP INDEX object_key_value_objects_idx;
DROP INDEX collection_objects_collection_idx;
DROP INDEX collection_objects_objects_idx;