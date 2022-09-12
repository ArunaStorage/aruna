use aruna_server::database::{self};
use serial_test::serial;

#[test]
#[ignore]
#[serial(db)]
fn get_or_add_pubkey_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Insert new element -> Create new serial number
    let result = db.get_or_add_pub_key("pubkey_test_1".to_string()).unwrap();

    // Insert a second "pubkey"
    let _result_2 = db.get_or_add_pub_key("pubkey_test_2".to_string()).unwrap();

    // Try to insert the first serial again -> should be the same as result
    let result_3 = db.get_or_add_pub_key("pubkey_test_1".to_string()).unwrap();
    assert_eq!(result, result_3);
}

#[test]
#[ignore]
#[serial(db)]
fn get_pub_keys_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Insert new element -> Create new serial number
    let result = db.get_pub_keys().unwrap();

    assert!(result.len() == 2);
    for key in result {
        if key.pubkey == *"pubkey_test_1" || key.pubkey == *"pubkey_test_2" {
            continue;
        } else {
            panic!(
                "Expected pubkey_test_1 or pubkey_test_2, got: {:?}",
                key.pubkey
            );
        }
    }
}

#[test]
#[ignore]
#[serial(db)]
fn get_oidc_user_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    let user_id = db.get_oidc_user("admin_test_oidc_id").unwrap().unwrap();

    let parsed_uid = uuid::Uuid::parse_str("12345678-1234-1234-1234-111111111111").unwrap();

    assert_eq!(user_id, parsed_uid)
}
