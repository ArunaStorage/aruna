use aruna_server::database::crud::CrudDb;
use aruna_server::database::dsls::endpoint_dsl::{Endpoint, HostConfigs};
use aruna_server::database::enums::{EndpointStatus, EndpointVariant, ObjectMapping, ObjectType};
use diesel_ulid::DieselUlid;
use postgres_types::Json;
use tokio_postgres::GenericClient;

use crate::common::{init_db, test_utils};

#[tokio::test]
async fn create_test() {
    let db = init_db::init_db().await;
    let client = db.get_client().await.unwrap();
    let client = client.client();

    let ep_id = DieselUlid::generate();
    let doc_obj = DieselUlid::generate();

    let user = test_utils::new_user(vec![ObjectMapping::PROJECT(doc_obj)]);
    user.create(client).await.unwrap();
    let create_doc = test_utils::new_object(user.id, doc_obj, ObjectType::OBJECT);
    create_doc.create(client).await.unwrap();

    let endpoint = Endpoint {
        id: ep_id,
        name: "create_test".to_string(),
        host_config: Json(HostConfigs(Vec::new())),
        endpoint_variant: EndpointVariant::PERSISTENT,
        documentation_object: Some(doc_obj),
        is_public: true,
        is_default: false,
        status: EndpointStatus::INITIALIZING,
    };
    endpoint.create(client).await.unwrap();

    let new = Endpoint::get(ep_id, client).await.unwrap().unwrap();
    assert_eq!(endpoint, new);
    Endpoint::delete_by_id(&ep_id, client).await.unwrap(); // Needed because of unique constraints
}

#[tokio::test]
async fn delete_test() {
    let db = init_db::init_db().await;
    let client = db.get_client().await.unwrap();

    let client = client.client();

    let ep_id = DieselUlid::generate();
    let doc_obj = DieselUlid::generate();
    let user = test_utils::new_user(vec![ObjectMapping::PROJECT(doc_obj)]);
    user.create(client).await.unwrap();
    let create_doc = test_utils::new_object(user.id, doc_obj, ObjectType::OBJECT);
    create_doc.create(client).await.unwrap();

    let endpoint = Endpoint {
        id: ep_id,
        name: "delete_test".to_string(),
        host_config: Json(HostConfigs(Vec::new())),
        endpoint_variant: EndpointVariant::PERSISTENT,
        documentation_object: Some(doc_obj),
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
    let user = test_utils::new_user(vec![ObjectMapping::PROJECT(doc_obj)]);
    user.create(client).await.unwrap();
    let create_doc = test_utils::new_object(user.id, doc_obj, ObjectType::OBJECT);
    create_doc.create(client).await.unwrap();
    let unique_name = DieselUlid::generate().to_string(); // Endpoint names need to be unique
    let endpoint = Endpoint {
        id: ep_id,
        name: unique_name.clone(),
        host_config: Json(HostConfigs(Vec::new())),
        endpoint_variant: EndpointVariant::PERSISTENT,
        documentation_object: Some(doc_obj),
        is_public: true,
        is_default: true,
        status: EndpointStatus::INITIALIZING,
    };
    endpoint.create(client).await.unwrap();

    let new = Endpoint::get_by_name(unique_name, client)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(endpoint, new);

    let default = Endpoint::get_default(client).await.unwrap().unwrap();

    assert_eq!(endpoint, default);
    Endpoint::delete_by_id(&ep_id, client).await.unwrap(); // Needed because of unique constraints
}
