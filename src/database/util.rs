// use crate::api::sciobjsdb::api::storage::models::v1::KeyValue;

// use super::models::Label;

// pub fn to_db_labels(labels: Vec<KeyValue>) -> Vec<Label> {
//     let db_labels = labels
//         .into_iter()
//         .map(|pair| {
//             let collection_uuid = uuid::Uuid::new_v4();
//             Label {
//                 id: collection_uuid,
//                 key: pair.key,
//                 value: pair.value,
//             }
//         })
//         .collect();

//     return db_labels;
// }

// pub fn to_proto_labels(labels: Vec<Label>) -> Vec<KeyValue> {
//     let proto_labels = labels
//         .into_iter()
//         .map(|label| KeyValue {
//             key: label.key,
//             value: label.value,
//         })
//         .collect();

//     return proto_labels;
// }
