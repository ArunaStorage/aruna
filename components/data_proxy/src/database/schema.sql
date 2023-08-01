CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    data bytea NOT NULL, -- The actual data
);

CREATE TABLE IF NOT EXISTS objects (
    id UUID NOT NULL PRIMARY KEY, -- The unique per object id
    data bytea NOT NULL, -- The actual data
);

CREATE TABLE IF NOT EXISTS object_locations (
    id UUID NOT NULL PRIMARY KEY REFERENCES objects(id), -- The unique per object id
    data bytea NOT NULL, -- The actual data
);

CREATE TABLE IF NOT EXISTS pub_keys (
    id UUID NOT NULL PRIMARY KEY, -- The unique per object id
    data bytea NOT NULL, -- The actual data
);