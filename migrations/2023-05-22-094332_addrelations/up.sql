-- Your SQL goes here
DROP TABLE IF EXISTS paths;


CREATE TABLE relations (
    id UUID PRIMARY KEY NOT NULL,
    object_id UUID NOT NULL,
    path VARCHAR(511) NOT NULL, -- /bli/blah/blup.txt
    project_id UUID NOT NULL,
    project_name VARCHAR(64) NOT NULL, -- my-project
    collection_id UUID NOT NULL,
    collection_path VARCHAR(64) NOT NULL, -- latest.test
    shared_revision_id UUID NOT NULL,
    path_active BOOL NOT NULL DEFAULT FALSE,
    FOREIGN KEY (collection_id) REFERENCES collections(id) ON DELETE CASCADE,
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
    FOREIGN KEY (object_id) REFERENCES objects(id) ON DELETE CASCADE,
    UNIQUE(object_id, path, project_name, collection_path)
);

CREATE INDEX rel_ob_id ON relations (object_id);
CREATE INDEX rel_ob_path ON relations (path);
CREATE INDEX rel_proj_id ON relations (project_id);
CREATE INDEX rel_proj_name ON relations (project_name);
CREATE INDEX rel_col_id ON relations (collection_id);
CREATE INDEX rel_col_path ON relations (collection_path);
CREATE INDEX rel_ob_shared ON relations (shared_revision_id);