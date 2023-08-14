use crate::common::init_db::init_handler;
use crate::common::test_utils;
use aruna_rust_api::api::storage::services::v2::{
    UpdateCollectionDataClassRequest, UpdateCollectionDescriptionRequest,
    UpdateCollectionNameRequest, UpdateDatasetDataClassRequest, UpdateDatasetDescriptionRequest,
    UpdateDatasetNameRequest, UpdateProjectDataClassRequest, UpdateProjectDescriptionRequest,
    UpdateProjectNameRequest,
};
use aruna_server::database::crud::CrudDb;
use aruna_server::database::dsls::object_dsl::Object;
use aruna_server::database::enums::{DataClass, ObjectMapping, ObjectType};
use aruna_server::middlelayer::update_request_types::{
    DataClassUpdate, DescriptionUpdate, NameUpdate,
};
use diesel_ulid::DieselUlid;

#[tokio::test]
async fn test_update_dataclass() {
    // Init
    let db_handler = init_handler().await;
    let resources = vec![
        ObjectMapping::PROJECT(DieselUlid::generate()),
        ObjectMapping::COLLECTION(DieselUlid::generate()),
        ObjectMapping::DATASET(DieselUlid::generate()),
    ];
    let user = test_utils::new_user(resources.clone());
    let mut objects = Vec::new();
    for r in resources {
        objects.push(test_utils::object_from_mapping(user.id, r));
    }
    let client = db_handler.database.get_client().await.unwrap();
    user.create(&client).await.unwrap();
    Object::batch_create(&objects, &client).await.unwrap();

    // Test
    assert!(
        Object::get_objects(&objects.iter().map(|o| o.id).collect(), &client)
            .await
            .unwrap()
            .iter()
            .all(|o| o.data_class == DataClass::PRIVATE)
    );
    for r in objects {
        match r.object_type {
            ObjectType::PROJECT => {
                let request = DataClassUpdate::Project(UpdateProjectDataClassRequest {
                    project_id: r.id.to_string(),
                    data_class: 1,
                });
                db_handler.update_dataclass(request).await.unwrap();
                assert_eq!(
                    Object::get(r.id, &client)
                        .await
                        .unwrap()
                        .unwrap()
                        .data_class,
                    DataClass::PUBLIC
                );
            }
            ObjectType::COLLECTION => {
                let request = DataClassUpdate::Collection(UpdateCollectionDataClassRequest {
                    collection_id: r.id.to_string(),
                    data_class: 1,
                });
                db_handler.update_dataclass(request).await.unwrap();
                assert_eq!(
                    Object::get(r.id, &client)
                        .await
                        .unwrap()
                        .unwrap()
                        .data_class,
                    DataClass::PUBLIC
                );
            }
            ObjectType::DATASET => {
                let request = DataClassUpdate::Dataset(UpdateDatasetDataClassRequest {
                    dataset_id: r.id.to_string(),
                    data_class: 3,
                });
                assert!(db_handler.update_dataclass(request).await.is_err());
            }
            _ => panic!(),
        };
    }
}
#[tokio::test]
async fn test_update_name() {
    // Init
    let db_handler = init_handler().await;
    let resources = vec![
        ObjectMapping::PROJECT(DieselUlid::generate()),
        ObjectMapping::COLLECTION(DieselUlid::generate()),
        ObjectMapping::DATASET(DieselUlid::generate()),
    ];
    let user = test_utils::new_user(resources.clone());
    let mut objects = Vec::new();
    for r in resources {
        objects.push(test_utils::object_from_mapping(user.id, r));
    }
    let client = db_handler.database.get_client().await.unwrap();
    user.create(&client).await.unwrap();
    Object::batch_create(&objects, &client).await.unwrap();

    // Test
    assert!(
        Object::get_objects(&objects.iter().map(|o| o.id).collect(), &client)
            .await
            .unwrap()
            .iter()
            .all(|o| o.name == *"a")
    );
    for r in objects {
        match r.object_type {
            ObjectType::PROJECT => {
                let request = NameUpdate::Project(UpdateProjectNameRequest {
                    project_id: r.id.to_string(),
                    name: "project_name".to_string(),
                });
                db_handler.update_name(request).await.unwrap();
                assert_eq!(
                    Object::get(r.id, &client).await.unwrap().unwrap().name,
                    *"project_name"
                );
            }
            ObjectType::COLLECTION => {
                let request = NameUpdate::Collection(UpdateCollectionNameRequest {
                    collection_id: r.id.to_string(),
                    name: "collection_name".to_string(),
                });
                db_handler.update_name(request).await.unwrap();
                assert_eq!(
                    Object::get(r.id, &client).await.unwrap().unwrap().name,
                    *"collection_name"
                );
            }
            ObjectType::DATASET => {
                let request = NameUpdate::Dataset(UpdateDatasetNameRequest {
                    dataset_id: r.id.to_string(),
                    name: "dataset_name".to_string(),
                });
                db_handler.update_name(request).await.unwrap();
                assert_eq!(
                    Object::get(r.id, &client).await.unwrap().unwrap().name,
                    *"dataset_name"
                );
            }
            _ => panic!(),
        };
    }
}
#[tokio::test]
async fn test_update_description() {
    // Init
    let db_handler = init_handler().await;
    let resources = vec![
        ObjectMapping::PROJECT(DieselUlid::generate()),
        ObjectMapping::COLLECTION(DieselUlid::generate()),
        ObjectMapping::DATASET(DieselUlid::generate()),
    ];
    let user = test_utils::new_user(resources.clone());
    let mut objects = Vec::new();
    for r in resources {
        objects.push(test_utils::object_from_mapping(user.id, r));
    }
    let client = db_handler.database.get_client().await.unwrap();
    user.create(&client).await.unwrap();
    Object::batch_create(&objects, &client).await.unwrap();

    // Test
    assert!(
        Object::get_objects(&objects.iter().map(|o| o.id).collect(), &client)
            .await
            .unwrap()
            .iter()
            .all(|o| o.description == *"b")
    );
    for r in objects {
        match r.object_type {
            ObjectType::PROJECT => {
                let request = DescriptionUpdate::Project(UpdateProjectDescriptionRequest {
                    project_id: r.id.to_string(),
                    description: "project_description".to_string(),
                });
                db_handler.update_description(request).await.unwrap();
                assert_eq!(
                    Object::get(r.id, &client)
                        .await
                        .unwrap()
                        .unwrap()
                        .description,
                    *"project_description"
                );
            }
            ObjectType::COLLECTION => {
                let request = DescriptionUpdate::Collection(UpdateCollectionDescriptionRequest {
                    collection_id: r.id.to_string(),
                    description: "collection_description".to_string(),
                });
                db_handler.update_description(request).await.unwrap();
                assert_eq!(
                    Object::get(r.id, &client)
                        .await
                        .unwrap()
                        .unwrap()
                        .description,
                    *"collection_description"
                );
            }
            ObjectType::DATASET => {
                let request = DescriptionUpdate::Dataset(UpdateDatasetDescriptionRequest {
                    dataset_id: r.id.to_string(),
                    description: "dataset_description".to_string(),
                });
                db_handler.update_description(request).await.unwrap();
                assert_eq!(
                    Object::get(r.id, &client)
                        .await
                        .unwrap()
                        .unwrap()
                        .description,
                    *"dataset_description"
                );
            }
            _ => panic!(),
        };
    }
}
