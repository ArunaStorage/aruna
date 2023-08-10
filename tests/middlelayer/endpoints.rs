use crate::common::{init_db::init_handler, test_utils};
use aruna_rust_api::api::storage::services::v2::CreateEndpointRequest;
use aruna_server::database::crud::CrudDb;
use aruna_server::database::dsls::pub_key_dsl::PubKey;
use aruna_server::database::enums::{EndpointVariant, ObjectMapping, ObjectType};
use aruna_server::middlelayer::endpoints_request_types::CreateEP;
use diesel_ulid::DieselUlid;
#[tokio::test]
async fn test_create_ep() {
    // init
    let db_handler = init_handler().await;
    let client = db_handler.database.get_client().await.unwrap();
    let pk = PubKey {
        id: 0,
        proxy: None,
        pubkey: "SERVER_PUBKEY_DUMMY".to_string(),
    };
    let o_id = DieselUlid::generate();
    let user = test_utils::new_user(vec![ObjectMapping::OBJECT(o_id)]);
    let u_id = user.id;
    let doc_obj = test_utils::new_object(u_id, o_id, ObjectType::OBJECT);
    pk.create(&client).await.unwrap();
    user.create(&client).await.unwrap();
    doc_obj.create(&client).await.unwrap();

    // test
    let request = CreateEP(CreateEndpointRequest {
        name: "endpoint_test".to_string(),
        ep_variant: 1,
        is_public: true,
        pubkey: "test".to_string(),
        host_configs: vec![],
    });

    let (ep, pk) = db_handler.create_endpoint(request).await.unwrap();
    assert_eq!(ep.name, "endpoint_test".to_string());
    assert_eq!(ep.endpoint_variant, EndpointVariant::PERSISTENT);
    assert!(ep.is_public);
    assert!(!ep.is_default);
    assert!(ep.host_config.0 .0.is_empty());
    assert!(pk.proxy.is_some());
    assert_eq!(pk.pubkey, "test".to_string());
    ep.delete(&client).await.unwrap();
}
