/* ----- VIEWS ----------------------------------------------------- */
DROP MATERIALIZED VIEW collection_stats;
DROP MATERIALIZED VIEW object_group_stats;
/* ----- Notification Service -------------------------------------- */
DROP TABLE notification_stream_groups;
/* ----- Authorization --------------------------------------------- */
DROP TABLE api_tokens;
/* ----- Join Tables ----------------------------------------------- */
DROP TABLE object_group_objects;
DROP TABLE collection_object_groups;
DROP TABLE collection_objects;
/* ----- ObjectGroups ---------------------------------------------- */
DROP TABLE object_group_key_value;
DROP TABLE object_groups;
/* ----- Objects --------------------------------------------------- */
DROP TABLE object_key_value;
DROP TABLE hashes;
DROP TABLE object_locations;
DROP TABLE endpoints;
DROP TABLE objects;
-- Automatically drops indexes
DROP TABLE sources;
/* ----- Collections ----------------------------------------------- */
DROP TABLE required_labels;
DROP TABLE collection_key_value;
DROP TABLE collections;
DROP TABLE collection_version;
-- Automatically drops indexes
/* ----- Authentication -------------------------------------------- */
DROP TABLE user_permissions;
DROP TABLE projects;
DROP TABLE external_user_ids;
DROP TABLE users;
DROP TABLE identity_providers;
DROP TABLE pub_keys;
/* ----- Type ENUMs ------------------------------------------------ */
DROP TYPE OBJECT_STATUS;
DROP TYPE ENDPOINT_TYPE;
DROP TYPE DATACLASS;
DROP TYPE SOURCE_TYPE;
DROP TYPE KEY_VALUE_TYPE;
DROP TYPE IDENTITY_PROVIDER_TYPE;
DROP TYPE USER_RIGHTS;
DROP TYPE RESOURCES;
DROP TYPE HASH_TYPE;