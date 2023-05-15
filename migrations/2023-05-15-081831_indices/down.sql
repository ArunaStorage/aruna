-- This file should undo anything in `up.sql`
CREATE INDEX objects_id_idx ON objects (shared_revision_id, revision_number);
DROP INDEX objects_shared_rev_idx ON objects;
DROP INDEX objects_shared_single_idx ON objects;
DROP INDEX hashes_objects_idx ON hashes;
DROP INDEX object_key_value_objects_idx ON object_key_value;
DROP INDEX collection_objects_collection_idx ON collection_objects;
DROP INDEX collection_objects_objects_idx ON collection_objects;