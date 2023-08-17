INSERT INTO relation_types (relation_name) VALUES ('BELONGS_TO'), ('VERSION'), ('METADATA'), ('ORIGIN'), ('POLICY');

-- 01H819G3ZMK5DC9Q5PD18N9SXB
INSERT INTO users (id, display_name, external_id, attributes, active) 
VALUES
(
    '018A0298-0FF4-995A-C4DC-B6685154E7AB',
    'test-admin',
    'df5b0209-60e0-4a3b-806d-bbfc99d9e152',
    '{"global_admin": true, "service_account": false, "tokens": {}, "trusted_endpoints": {}, "custom_attributes": [], "permissions": {}}',
    false
);