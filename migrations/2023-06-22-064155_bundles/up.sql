-- Your SQL goes here

CREATE TABLE bundles (
    id UUID PRIMARY KEY NOT NULL,
    bundle_id VARCHAR(511) NOT NULL,
    object_id UUID NOT NULL,
    endpoint_id UUID NOT NULL,
    collection_id UUID NOT NULL,
    FOREIGN KEY (endpoint_id) REFERENCES endpoints(id) ON DELETE CASCADE,
    FOREIGN KEY (collection_id) REFERENCES collections(id) ON DELETE CASCADE,
    FOREIGN KEY (object_id) REFERENCES objects(id) ON DELETE CASCADE,
    UNIQUE(object_id, bundle_id)
);


ALTER TABLE endpoints ADD COLUMN is_bundler BOOL NOT NULL DEFAULT FALSE;
ALTER TABLE endpoints DROP COLUMN proxy_hostname;
ALTER TABLE endpoints DROP COLUMN internal_hostname;
ALTER TABLE endpoints ADD COLUMN host_config JSONB NOT NULL;