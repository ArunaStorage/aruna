use crate::common::init_db;
use aruna_server::database::crud::CrudDb;
use aruna_server::database::dsls::pub_key_dsl::PubKey;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
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

    let comp = [&key_one, &key_two, &key_three];
    let all = PubKey::all(client).await.unwrap();

    assert!(comp.iter().all(|pk| all.contains(pk)));

    key_one.delete(client).await.unwrap();
    key_two.delete(client).await.unwrap();
    key_three.delete(client).await.unwrap();

    let rest = PubKey::all(client).await.unwrap();

    assert!([&key_one, &key_two, &key_three]
        .iter()
        .all(|k| !rest.contains(k)))
}

#[tokio::test]
async fn test_pub_key_serial_auto_incerement() {
    // Init database connection
    let db = common::init_db::init_db().await;
    let client = db.get_client().await.unwrap();
    let client = client.client();

    // Reusable closure to generate random pubkey string
    let gen_rand_string = || -> String {
        thread_rng()
            .sample_iter(&Alphanumeric)
            .take(32)
            .map(char::from)
            .collect()
    };

    // Generate random strings as key dummy
    let dummy_pubkey_001 = gen_rand_string();
    let dummy_pubkey_002 = gen_rand_string();

    // Persist dummy keys in database with auto serial increment
    let dummy_key_001 = PubKey::create_without_id(None, &dummy_pubkey_001, client)
        .await
        .unwrap();

    // Persist dummy keys in database with auto serial increment
    let dummy_key_002 = PubKey::create_without_id(None, &dummy_pubkey_002, client)
        .await
        .unwrap();

    assert!(dummy_key_002.id > dummy_key_001.id);

    // Fetch pubkey by its key
    for key in vec![dummy_key_001, dummy_key_002] {
        let fetched_key = PubKey::get_by_key(&key.pubkey, client)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(fetched_key, key)
    }
}
