use crate::common::{init_db::init_handler, test_utils};
use aruna_rust_api::api::storage::services::v2::{
    DeleteCollectionRequest, DeleteDatasetRequest, DeleteObjectRequest, DeleteProjectRequest,
};
use aruna_server::database::crud::CrudDb;
use aruna_server::database::dsls::internal_relation_dsl::{
    InternalRelation, INTERNAL_RELATION_VARIANT_VERSION,
};
use aruna_server::database::dsls::object_dsl::Object;
use aruna_server::database::enums::{ObjectStatus, ObjectType};
use aruna_server::middlelayer::delete_request_types::DeleteRequest;
use diesel_ulid::DieselUlid;

#[tokio::test]
async fn delete_project() {
    // init
    let db_handler = init_handler().await;
    let client = db_handler.database.get_client().await.unwrap();

    // create user + project
    let mut user = test_utils::new_user(vec![]);
    user.create(&client).await.unwrap();
    let project_id = DieselUlid::generate();
    let mut project = test_utils::new_object(user.id, project_id, ObjectType::PROJECT);
    project.create(&client).await.unwrap();

    // test request
    let delete_request = DeleteRequest::Project(DeleteProjectRequest {
        project_id: project_id.to_string(),
    });
    db_handler.delete_resource(delete_request).await.unwrap();
    assert_eq!(
        Object::get(project_id, &client)
            .await
            .unwrap()
            .unwrap()
            .object_status,
        ObjectStatus::DELETED
    );
}

#[tokio::test]
async fn delete_collection() {
    // init
    let db_handler = init_handler().await;
    let client = &db_handler.database.get_client().await.unwrap();

    // create user + collection
    let mut user = test_utils::new_user(vec![]);
    user.create(client).await.unwrap();
    let collection_id = DieselUlid::generate();
    let mut collection = test_utils::new_object(user.id, collection_id, ObjectType::COLLECTION);
    collection.create(client).await.unwrap();

    // Test request
    let delete_request = DeleteRequest::Collection(DeleteCollectionRequest {
        collection_id: collection_id.to_string(),
    });
    db_handler.delete_resource(delete_request).await.unwrap();
    assert_eq!(
        Object::get(collection_id, client)
            .await
            .unwrap()
            .unwrap()
            .object_status,
        ObjectStatus::DELETED
    );
}

#[tokio::test]
async fn delete_dataset() {
    // init
    let db_handler = init_handler().await;
    let client = &db_handler.database.get_client().await.unwrap();

    // create user + dataset
    let mut user = test_utils::new_user(vec![]);
    user.create(client).await.unwrap();
    let dataset_id = DieselUlid::generate();
    let mut dataset = test_utils::new_object(user.id, dataset_id, ObjectType::DATASET);
    dataset.create(client).await.unwrap();

    // Test request
    let delete_request = DeleteRequest::Dataset(DeleteDatasetRequest {
        dataset_id: dataset_id.to_string(),
    });
    db_handler.delete_resource(delete_request).await.unwrap();
    assert_eq!(
        Object::get(dataset_id, client)
            .await
            .unwrap()
            .unwrap()
            .object_status,
        ObjectStatus::DELETED
    );
}

#[tokio::test]
async fn delete_object() {
    // init
    let db_handler = init_handler().await;
    let client = &db_handler.database.get_client().await.unwrap();

    // create user + objects
    let mut user = test_utils::new_user(vec![]);
    user.create(client).await.unwrap();
    let object = DieselUlid::generate();
    let object_v1 = DieselUlid::generate();
    let object_v2 = DieselUlid::generate();
    let mut objects: Vec<Object> = Vec::new();
    for o in [object, object_v1, object_v2] {
        objects.push(test_utils::new_object(user.id, o, ObjectType::OBJECT));
    }
    let relation_one = InternalRelation {
        id: DieselUlid::generate(),
        origin_pid: object_v1,
        origin_type: ObjectType::OBJECT,
        relation_name: INTERNAL_RELATION_VARIANT_VERSION.to_string(),
        target_pid: object,
        target_type: ObjectType::OBJECT,
    };
    let relation_two = InternalRelation {
        id: DieselUlid::generate(),
        origin_pid: object_v2,
        origin_type: ObjectType::OBJECT,
        relation_name: INTERNAL_RELATION_VARIANT_VERSION.to_string(),
        target_pid: object,
        target_type: ObjectType::OBJECT,
    };
    Object::batch_create(&objects, client).await.unwrap();
    InternalRelation::batch_create(&vec![relation_one, relation_two], client)
        .await
        .unwrap();

    // Test request
    let delete_request = DeleteRequest::Object(DeleteObjectRequest {
        object_id: object.to_string(),
        with_revisions: true,
    });
    db_handler.delete_resource(delete_request).await.unwrap();
    assert_eq!(
        Object::get(object, client)
            .await
            .unwrap()
            .unwrap()
            .object_status,
        ObjectStatus::DELETED
    );
    assert_eq!(
        Object::get(object_v1, client)
            .await
            .unwrap()
            .unwrap()
            .object_status,
        ObjectStatus::DELETED
    );
    assert_eq!(
        Object::get(object_v2, client)
            .await
            .unwrap()
            .unwrap()
            .object_status,
        ObjectStatus::DELETED
    );
}
