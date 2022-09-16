use std::str::FromStr;

use aruna_server::{
    database,
    api::aruna::api::storage::{
        services::v1::{ CreateObjectGroupRequest, CreateNewCollectionRequest },
        models::v1::KeyValue,
    },
};
use serial_test::serial;

// #[test]
// #[ignore]
// #[serial(db)]
// fn create_object_group_test() {
//     let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

//     let creator = uuid::Uuid::parse_str("12345678-1234-1234-1234-111111111111").unwrap();

//     let request = CreateNewCollectionRequest {
//         name: "grp_new_collection".to_owned(),
//         description: "this_is_a_demo_collection_for_grps".to_owned(),
//         project_id: "12345678-1111-1111-1111-111111111111".to_owned(),
//         labels: vec![KeyValue {
//             key: "test_key_grp".to_owned(),
//             value: "test_value_grp".to_owned(),
//         }],
//         hooks: vec![KeyValue {
//             key: "test_key_grp".to_owned(),
//             value: "test_value_grp".to_owned(),
//         }],
//         dataclass: 2,
//     };

//     let result = db.create_new_collection(request, creator).unwrap();
//     let col_id = uuid::Uuid::from_str(&result.collection_id).unwrap();

//     assert!(!col_id.is_nil());

//     let request = CreateObjectGroupRequest {
//         name: "Testgroup".to_string(),
//         description: "Group_descr".to_string(),
//         collection_id: col_id.to_string(),
//         object_ids: todo!(),
//         meta_object_ids: todo!(),
//         labels: todo!(),
//         hooks: todo!(),
//     };
// }