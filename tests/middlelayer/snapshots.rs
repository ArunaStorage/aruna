use crate::common::{init_db::init_handler, test_utils};
use aruna_rust_api::api::storage::services::v2::ArchiveProjectRequest;
use aruna_server::database::crud::CrudDb;
use aruna_server::database::dsls::internal_relation_dsl::InternalRelation;
use aruna_server::database::dsls::object_dsl::Object;
use aruna_server::database::enums::{ObjectMapping, ObjectType};
use aruna_server::middlelayer::snapshot_request_types::SnapshotRequest;
use diesel_ulid::DieselUlid;

#[tokio::test]
async fn test_archive() {
    let db_handler = init_handler().await;
    let p_id = DieselUlid::generate();
    let ulids = vec![
        ObjectMapping::PROJECT(p_id),
        ObjectMapping::COLLECTION(DieselUlid::generate()),
        ObjectMapping::COLLECTION(DieselUlid::generate()),
        ObjectMapping::DATASET(DieselUlid::generate()),
        ObjectMapping::DATASET(DieselUlid::generate()),
        ObjectMapping::OBJECT(DieselUlid::generate()),
        ObjectMapping::OBJECT(DieselUlid::generate()),
    ];

    let user = test_utils::new_user(ulids.clone());
    let mut objects = Vec::new();
    for u in &ulids {
        match u {
            ObjectMapping::PROJECT(id) => {
                objects.push(test_utils::new_object(user.id, *id, ObjectType::PROJECT))
            }
            ObjectMapping::COLLECTION(id) => {
                objects.push(test_utils::new_object(user.id, *id, ObjectType::COLLECTION))
            }
            ObjectMapping::DATASET(id) => {
                objects.push(test_utils::new_object(user.id, *id, ObjectType::DATASET))
            }
            ObjectMapping::OBJECT(id) => {
                objects.push(test_utils::new_object(user.id, *id, ObjectType::OBJECT))
            }
        }
    }
    let p_c1 = test_utils::new_internal_relation(&objects[0], &objects[1]);
    let p_c2 = test_utils::new_internal_relation(&objects[0], &objects[2]);
    let c1_d1 = test_utils::new_internal_relation(&objects[1], &objects[3]);
    let c2_d2 = test_utils::new_internal_relation(&objects[2], &objects[4]);
    let d1_o1 = test_utils::new_internal_relation(&objects[3], &objects[5]);
    let d2_o2 = test_utils::new_internal_relation(&objects[4], &objects[6]);
    let rels = vec![p_c1, p_c2, c1_d1, c2_d2, d1_o1, d2_o2];
    let client = db_handler.database.get_client().await.unwrap();
    user.create(&client).await.unwrap();
    Object::batch_create(&objects, &client).await.unwrap();
    InternalRelation::batch_create(&rels, &client)
        .await
        .unwrap();

    // test
    let request = SnapshotRequest::Project(ArchiveProjectRequest {
        project_id: p_id.to_string(),
    });
    let (_, archive) = db_handler.snapshot(request).await.unwrap();
    assert!(archive.iter().all(|o| !o.object.dynamic));
}
