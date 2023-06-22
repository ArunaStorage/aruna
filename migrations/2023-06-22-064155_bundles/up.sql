-- Your SQL goes here



CREATE TABLE bundles (
    id UUID PRIMARY KEY NOT NULL,
    bundle_id VARCHAR(511) NOT NULL,
    object_id UUID NOT NULL,
    path VARCHAR(511) NOT NULL, -- /bli/blah/blup.txt
    collection_id UUID NOT NULL,
    FOREIGN KEY (collection_id) REFERENCES collections(id) ON DELETE CASCADE,
    FOREIGN KEY (object_id) REFERENCES objects(id) ON DELETE CASCADE,
    UNIQUE(object_id, bundle_id)
);