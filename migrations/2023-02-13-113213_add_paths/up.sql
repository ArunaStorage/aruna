-- Table with unique object paths
CREATE TABLE paths (
    id UUID PRIMARY KEY,
    path TEXT NOT NULL UNIQUE, -- /project-name/collection-name/user-defined-path/lorem.txt
    shared_revision_id UUID NOT NULL,
    collection_id UUID NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    active BOOL NOT NULL DEFAULT FALSE,
    FOREIGN KEY (collection_id) REFERENCES collections(id) ON DELETE CASCADE,
    UNIQUE (path)
);

-- Add unique constraint for collection name and project_id 
ALTER TABLE collections ADD CONSTRAINT uniqe_collection_name_project_id UNIQUE (project_id, name, version_id);
-- Add unique constraint for project name
ALTER TABLE projects ADD CONSTRAINT unique_project_name UNIQUE (name);