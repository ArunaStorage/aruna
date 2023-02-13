-- Table with unique object paths
CREATE TABLE paths (
    id UUID PRIMARY KEY,
    path TEXT NOT NULL UNIQUE, -- /project-name/collection-name/user-defined-path/lorem.txt
    shared_revision_id UUID NOT NULL,
    collection_id UUID NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    active BOOL NOT NULL DEFAULT FALSE,
    FOREIGN KEY (collection_id) REFERENCES collections(id) ON DELETE CASCADE,
    UNIQUE (path, shared_revision_id, collection_id)
);