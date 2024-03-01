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