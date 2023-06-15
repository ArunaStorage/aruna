-- Your SQL goes here

DROP INDEX objects_id_idx ON objects;
CREATE INDEX objects_shared_rev_idx ON objects (shared_revision_id, revision_number);
CREATE INDEX objects_shared_single_idx ON objects (shared_revision_id);
CREATE INDEX hashes_objects_idx ON hashes (object_id);
CREATE INDEX object_key_value_objects_idx ON object_key_value (object_id);
CREATE INDEX collection_objects_collection_idx ON collection_objects (collection_id);
CREATE INDEX collection_objects_objects_idx ON collection_objects (object_id);


ALTER TABLE collections ADD CONSTRAINT uniqe_collection_name_project_id UNIQUE (project_id, name, version_id);
ALTER TABLE collections ADD CONSTRAINT uniqe_collection_name_project_id_without_version UNIQUE (project_id, name) WHERE (version_id is NULL);