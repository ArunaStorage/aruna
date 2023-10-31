-- Insert predefined relation types
INSERT INTO relation_types (relation_name) VALUES ('BELONGS_TO'), ('VERSION'), ('METADATA'), ('ORIGIN'), ('POLICY');
-- Create partial unique index for BELONGS_TO relations only
CREATE UNIQUE INDEX belongs_to ON internal_relations (origin_pid, relation_name, target_name) WHERE relation_name = ('BELONGS_TO');


-- Insert global admin and normal user
INSERT INTO users (id, display_name, external_id, attributes, active) 
VALUES
(
    '018A0298-0FF4-995A-C4DC-B6685154E7AB', --01H819G3ZMK5DC9Q5PD18N9SXB
    'test-admin',
    'df5b0209-60e0-4a3b-806d-bbfc99d9e152',
    '{"global_admin": true, "service_account": false, "tokens": {}, "trusted_endpoints": {}, "custom_attributes": [], "permissions": {}}',
    true
);

INSERT INTO users (id, display_name, external_id, attributes, active) 
VALUES
(
    '018A27CF-78B4-D2A2-1F7E-16F6F27B5F8D', --01H8KWYY5MTAH1YZGPYVS7PQWD
    'test-user',
    '39893781-320e-4dbf-be39-c06d8b28e897', -- regular-user 1SH4VR2CGE9PZVWEE0DP5JHT4Q
    '{"global_admin": false, "service_account": false, "tokens": {}, "trusted_endpoints": {}, "custom_attributes": [], "permissions": {}}',
    true
);

INSERT INTO users (id, display_name, external_id, attributes, active) 
VALUES
(
    '018AE07D-5F28-8EB6-3A0A-D37CE945E645', --01HBG7TQS8HTV3M2PKFKMMBSJ5
    'another-test-user',
    'c675b9fe-7275-46c6-af5a-2641114b5375', -- user3
    '{"global_admin": false, "service_account": false, "tokens": {}, "trusted_endpoints": {}, "custom_attributes": [], "permissions": {}}',
    true
);


INSERT INTO endpoints(id, name, host_config, endpoint_variant, is_public, status) VALUES (
    '018a03c0-7e8b-293c-eb14-e10dc4b990db', --01H81W0ZMB54YEP5711Q2BK46V
    'default_endpoint',
    '[
	{"url": "http://localhost:50052", "is_primary": true, "ssl": false, "public": true, "feature": "GRPC"},
	{"url": "http://localhost:1337", "is_primary": true, "ssl": false, "public": true, "feature": "S3"}
    ]',
    'PERSISTENT',
    't',
    'AVAILABLE' 
);

INSERT INTO pub_keys(id, proxy, pubkey) VALUES (1337, '018a03c0-7e8b-293c-eb14-e10dc4b990db', 'MCowBQYDK2VwAyEAnouQBh4GHPCD/k85VIzPyCdOijVg2qlzt2TELwTMy4c=');

INSERT INTO licenses(tag, name, description, url) VALUES ('All_Rights_Reserved', 'All rights reserved', 'All rights reserved', 'license.test.org');

