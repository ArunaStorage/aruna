-- Your SQL goes here
ALTER TABLE collection_objects ADD CONSTRAINT parallel_collection_object UNIQUE (object_id, collection_id, auto_update);
DROP INDEX unique_collection_object CASCADE;
