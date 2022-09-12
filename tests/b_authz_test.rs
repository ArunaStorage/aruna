use aruna_server::{
    database::{ self },
    api::aruna::api::storage::services::v1::RegisterUserRequest,
};
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

    // Expect 2 keys in db
    assert!(result.len() == 2);
    // Iterate through keys
    for key in result {
        // Expect it to be either "pubkey_test_1" or "pub_key_test_2"
        if key.pubkey == *"pubkey_test_1" || key.pubkey == *"pubkey_test_2" {
            continue;
            // Panic otherwise -> unknown pubkey in db
        } else {
            panic!("Expected pubkey_test_1 or pubkey_test_2, got: {:?}", key.pubkey);
        }
    }
}

#[test]
#[ignore]
#[serial(db)]
fn get_oidc_user_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Get admin user via (fake) oidc id
    let user_id = db.get_oidc_user("admin_test_oidc_id").unwrap().unwrap();

    // Expect the user to have the following uuid
    let parsed_uid = uuid::Uuid::parse_str("12345678-1234-1234-1234-111111111111").unwrap();
    assert_eq!(user_id, parsed_uid)
}

#[test]
#[ignore]
#[serial(db)]
fn register_user_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Build request for new user
    let req = RegisterUserRequest { display_name: "test_user_1".to_string() };
    // Create new user
    db.register_user(req, "test_user_1_oidc".to_string()).unwrap();
}