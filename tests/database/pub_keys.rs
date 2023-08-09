use crate::common::init_db;
use aruna_server::database::crud::CrudDb;
use aruna_server::database::dsls::pub_key_dsl::PubKey;
use tokio_postgres::GenericClient;

#[tokio::test]
async fn test_crud() {
    let db = init_db::init_db().await;
    let client = db.get_client().await.unwrap();
    let client = client.client();

    let key_one = PubKey {
        id: 1,
        proxy: None,
        pubkey: "key_one".to_string(),
    };

    key_one.create(client).await.unwrap();

    let key = PubKey::get(1i16, client).await.unwrap();
    assert!(key.is_some());
    let key_two = PubKey {
        id: 2,
        proxy: None,
        pubkey: "key_two".to_string(),
    };
    let key_three = PubKey {
        id: 3,
        proxy: None,
        pubkey: "key_three".to_string(),
    };
    key_two.create(client).await.unwrap();
    key_three.create(client).await.unwrap();

    let all = PubKey::all(client).await.unwrap();

    assert_eq!(all.len(), 3);

    key_one.delete(client).await.unwrap();
    key_two.delete(client).await.unwrap();
    key_three.delete(client).await.unwrap();

    let empty = PubKey::all(client).await.unwrap();

    assert!(empty.is_empty())
}
