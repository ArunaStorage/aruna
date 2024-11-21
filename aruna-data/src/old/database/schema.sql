CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    data JSONB NOT NULL -- The actual data
);

CREATE TABLE IF NOT EXISTS objects (
    id UUID NOT NULL PRIMARY KEY,
    data JSONB NOT NULL -- The actual data
);

CREATE TABLE IF NOT EXISTS bundle (
    id UUID NOT NULL PRIMARY KEY, 
    data JSONB NOT NULL -- The actual data
);

CREATE TABLE IF NOT EXISTS object_locations (
    id UUID NOT NULL PRIMARY KEY, 
    data JSONB NOT NULL -- The actual data
);

CREATE TABLE IF NOT EXISTS location_bindings (
    object_id UUID NOT NULL,
    location_id UUID NOT NULL,
    PRIMARY KEY (object_id, location_id),
    CONSTRAINT fk_objects FOREIGN KEY (object_id) REFERENCES objects(id) ON DELETE CASCADE,
    CONSTRAINT fk_locations FOREIGN KEY (location_id) REFERENCES object_locations(id) ON DELETE CASCADE,
    UNIQUE (object_id)
);

CREATE TABLE IF NOT EXISTS pub_keys (
    id SMALLSERIAL NOT NULL PRIMARY KEY, 
    data JSONB NOT NULL -- The actual data
);

CREATE TABLE IF NOT EXISTS multiparts (
    id UUID NOT NULL PRIMARY KEY, 
    data JSONB NOT NULL -- The actual data
);

CREATE TABLE IF NOT EXISTS permissions (
    id TEXT NOT NULL PRIMARY KEY, 
    data JSONB NOT NULL -- The actual data
);