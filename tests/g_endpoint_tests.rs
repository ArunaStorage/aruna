use aruna_rust_api::api::storage::models::v1::EndpointType as ProtoEndpointType;
use aruna_rust_api::api::storage::services::v1::AddEndpointRequest;
use aruna_server::config::DefaultEndpoint;
use aruna_server::database;
use aruna_server::database::models::enums::EndpointType;
use aruna_server::database::models::object::Endpoint;
use serial_test::serial;

#[test]
#[ignore]
#[serial(db)]
fn init_default_endpoint_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Add default endpoint to database
    let default_endpoint = DefaultEndpoint {
        ep_type: EndpointType::S3,
        endpoint_name: "Default_Endpoint".to_string(),
        endpoint_host: "internal_server_name".to_string(),
        endpoint_proxy: "data_proxy.example.com".to_string(),
        endpoint_public: true,
        endpoint_docu: None,
    };
    let db_endpoint = db.init_default_endpoint(default_endpoint.clone()).unwrap();

    // Validate default endpoint creation
    assert!(matches!(db_endpoint.endpoint_type, EndpointType::S3));
    assert_eq!(db_endpoint.name, default_endpoint.endpoint_name);
    assert_eq!(
        db_endpoint.internal_hostname,
        default_endpoint.endpoint_host
    );
    assert_eq!(db_endpoint.proxy_hostname, default_endpoint.endpoint_proxy);
    assert_eq!(
        db_endpoint.documentation_path,
        default_endpoint.endpoint_docu
    );
    assert_eq!(db_endpoint.is_public, default_endpoint.endpoint_public);

    // Try to add default endpoint again
    let another_db_endpoint = db.init_default_endpoint(default_endpoint).unwrap();

    // Validate that the endpoint is the same
    assert_eq!(db_endpoint.id, another_db_endpoint.id);
    assert_eq!(
        db_endpoint.endpoint_type as i32,
        another_db_endpoint.endpoint_type as i32
    );
    assert_eq!(db_endpoint.name, another_db_endpoint.name);
    assert_eq!(
        db_endpoint.proxy_hostname,
        another_db_endpoint.proxy_hostname
    );
    assert_eq!(
        db_endpoint.internal_hostname,
        another_db_endpoint.internal_hostname
    );
    assert_eq!(
        db_endpoint.documentation_path,
        another_db_endpoint.documentation_path
    );
    assert_eq!(db_endpoint.is_public, another_db_endpoint.is_public);
}

#[test]
#[ignore]
#[serial(db)]
fn add_endpoint_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Add endpoint with request
    let add_request = AddEndpointRequest {
        name: "DummyEndpoint_001".to_string(),
        ep_type: ProtoEndpointType::S3 as i32,
        proxy_hostname: "https://proxy.aruna.uni-giessen.de".to_string(),
        internal_hostname: "https://proxy-internal.aruna.uni-giessen.de".to_string(),
        documentation_path: "/somewhere/else/docu.pdf".to_string(),
        is_public: true,
    };

    // Validate endpoint creation
    let Endpoint {
        id,
        endpoint_type,
        name,
        proxy_hostname,
        internal_hostname,
        documentation_path,
        is_public,
    } = db.add_endpoint(&add_request).unwrap();

    let _endpoint_uuid = uuid::Uuid::parse_str(id.to_string().as_str());

    assert!(matches!(endpoint_type, EndpointType::S3));
    assert_eq!(name, add_request.name);
    assert_eq!(proxy_hostname, add_request.proxy_hostname);
    assert_eq!(internal_hostname, add_request.internal_hostname);
    assert_eq!(documentation_path.unwrap(), add_request.documentation_path);
    assert_eq!(is_public, add_request.is_public);
}

#[test]
#[ignore]
#[serial(db)]
fn get_endpoint_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let endpoint_uuid = uuid::Uuid::parse_str("12345678-6666-6666-6666-999999999999").unwrap();

    // Get Endpoint by its uuid
    let Endpoint {
        id,
        endpoint_type,
        name,
        proxy_hostname,
        internal_hostname,
        documentation_path,
        is_public,
    } = db.get_endpoint(&endpoint_uuid).unwrap();

    // Validate returned endpoint
    assert_eq!(endpoint_uuid, id);
    assert!(matches!(endpoint_type, EndpointType::S3));
    assert_eq!(name, "demo_endpoint");
    assert_eq!(proxy_hostname, "url_prox_a");
    assert_eq!(internal_hostname, "url_inter_b");
    assert!(documentation_path.is_none());
    assert!(is_public);
}

#[test]
#[ignore]
#[serial(db)]
fn get_endpoint_by_name_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let endpoint_uuid = uuid::Uuid::parse_str("12345678-6666-6666-6666-999999999999").unwrap();
    let endpoint_name = "demo_endpoint";

    // Get Endpoint by its uuid
    let Endpoint {
        id,
        endpoint_type,
        name,
        proxy_hostname,
        internal_hostname,
        documentation_path,
        is_public,
    } = db.get_endpoint_by_name(endpoint_name).unwrap();

    // Validate returned endpoint
    assert_eq!(endpoint_uuid, id);
    assert!(matches!(endpoint_type, EndpointType::S3));
    assert_eq!(name, "demo_endpoint");
    assert_eq!(proxy_hostname, "url_prox_a");
    assert_eq!(internal_hostname, "url_inter_b");
    assert!(documentation_path.is_none());
    assert!(is_public);
}
