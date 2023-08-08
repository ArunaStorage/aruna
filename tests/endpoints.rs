use aruna_server::database::crud::CrudDb;
use aruna_server::database::dsls::endpoint_dsl::{Endpoint, HostConfigs};
use aruna_server::database::enums::{EndpointStatus, EndpointVariant, ObjectType};
use diesel_ulid::DieselUlid;
use postgres_types::Json;
use tokio_postgres::GenericClient;

mod init_db;
mod utils;

#[tokio::test]
async fn create_test() {
    let db = init_db::init_db().await;
    let client = db.get_client().await.unwrap();
    let client = client.client();

    let ep_id = DieselUlid::generate();
    let doc_obj = DieselUlid::generate();
    let user = utils::new_user(vec![doc_obj]);
    user.create(client).await.unwrap();
    let create_doc = utils::new_object(user.id, doc_obj, ObjectType::OBJECT);
    create_doc.create(client).await.unwrap();

    let endpoint = Endpoint {
        id: ep_id,
        name: "test".to_string(),
        host_config: Json(HostConfigs(Vec::new())),
        endpoint_variant: EndpointVariant::PERSISTENT,
        documentation_object: doc_obj,
        is_public: true,
        is_default: false,
        status: EndpointStatus::INITIALIZING,
    };
    endpoint.create(client).await.unwrap();

    let new = Endpoint::get(ep_id, client).await.unwrap().unwrap();
    assert_eq!(endpoint, new);
}

#[tokio::test]
async fn delete_test() {
    let db = init_db::init_db().await;
    let client = db.get_client().await.unwrap();

    let client = client.client();

    let ep_id = DieselUlid::generate();
    let doc_obj = DieselUlid::generate();
    let user = utils::new_user(vec![doc_obj]);
    user.create(client).await.unwrap();
    let create_doc = utils::new_object(user.id, doc_obj, ObjectType::OBJECT);
    create_doc.create(client).await.unwrap();

    let endpoint = Endpoint {
        id: ep_id,
        name: "test".to_string(),
        host_config: Json(HostConfigs(Vec::new())),
        endpoint_variant: EndpointVariant::PERSISTENT,
        documentation_object: doc_obj,
        is_public: true,
        is_default: false,
        status: EndpointStatus::INITIALIZING,
    };
    endpoint.create(client).await.unwrap();

    Endpoint::delete_by_id(&ep_id, client).await.unwrap();

    assert!(Endpoint::get(ep_id, client).await.unwrap().is_none());
}
#[tokio::test]
async fn get_by_tests() {
    let db = init_db::init_db().await;
    let client = db.get_client().await.unwrap();
    let client = client.client();

    let ep_id = DieselUlid::generate();
    let doc_obj = DieselUlid::generate();
    let user = utils::new_user(vec![doc_obj]);
    user.create(client).await.unwrap();
    let create_doc = utils::new_object(user.id, doc_obj, ObjectType::OBJECT);
    create_doc.create(client).await.unwrap();

    let endpoint = Endpoint {
        id: ep_id,
        name: "test_123_unique".to_string(),
        host_config: Json(HostConfigs(Vec::new())),
        endpoint_variant: EndpointVariant::PERSISTENT,
        documentation_object: doc_obj,
        is_public: true,
        is_default: true,
        status: EndpointStatus::INITIALIZING,
    };
    endpoint.create(client).await.unwrap();

    let new = Endpoint::get_by_name("test_123_unique".to_string(), client)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(endpoint, new);

    let default = Endpoint::get_default(client).await.unwrap().unwrap();

    assert_eq!(endpoint, default);
}
