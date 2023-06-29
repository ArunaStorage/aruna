-- This file should undo anything in `up.sql`
-- Table with unique object paths
CREATE TABLE paths (
    id UUID PRIMARY KEY,
    bucket TEXT NOT NULL, -- version.collection-name.project-name
    path TEXT NOT NULL, -- /user-defined-path/file-name.txt
    shared_revision_id UUID NOT NULL,
    collection_id UUID NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    active BOOL NOT NULL DEFAULT FALSE,
    FOREIGN KEY (collection_id) REFERENCES collections(id) ON DELETE CASCADE,
    UNIQUE (bucket, path)
);
DROP TABLE IF EXISTS relations;

CREATE INDEX rel_ob_id;
CREATE INDEX rel_ob_path;
CREATE INDEX rel_proj_id;
CREATE INDEX rel_proj_name;
CREATE INDEX rel_col_id;
CREATE INDEX rel_col_path;
CREATE INDEX rel_ob_shared;