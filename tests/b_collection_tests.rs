use aruna_server::api::aruna::api::storage::models::v1::{Authorization, KeyValue};
use aruna_server::api::aruna::api::storage::services::v1::*;
use aruna_server::database;
use serial_test::serial;
use std::str::FromStr;

#[test]
#[ignore]
#[serial(db)]
fn test() {
    let db = database::connection::Database::new();

    let creator = uuid::Uuid::parse_str("12345678-1234-1234-1234-111111111111").unwrap();

    let request = CreateNewCollectionRequest {
        name: "new_collection".to_owned(),
        description: "this_is_a_demo_collection".to_owned(),
        project_id: "12345678-1111-1111-1111-111111111111".to_owned(),
        labels: vec![KeyValue {
            key: "test_key".to_owned(),
            value: "test_value".to_owned(),
        }],
        hooks: vec![KeyValue {
            key: "test_key".to_owned(),
            value: "test_value".to_owned(),
        }],
        dataclass: 1,
    };

    let result = db.create_new_collection(request, creator).unwrap();
    let id = uuid::Uuid::from_str(&result.collection_id).unwrap();

    assert!(!id.is_nil())
}
