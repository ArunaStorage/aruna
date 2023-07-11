use aruna_server::database::crud::CrudDb;
use aruna_server::database::object_dsl::{ExternalRelations, KeyValues, Object};
use aruna_server::database::user_dsl::User;
use diesel_ulid::DieselUlid;
use postgres_types::Json;

mod init_db;

#[tokio::test]
async fn create_object() {
    let db = crate::init_db::init_db().await;
    let client = db.get_client().await.unwrap();

    let user = User {
        id: DieselUlid::generate(),
        display_name: "aha".to_string(),
        email: "aja".to_string(),
        attributes: "jop".to_string(),
    };

    user.create(&client).await.unwrap();

    let obj_id = DieselUlid::generate();
    let create_object = Object {
        id: obj_id,
        shared_id: DieselUlid::generate(),
        revision_number: 0,
        path: "a".to_string(),
        created_at: None,
        content_len: 1337,
        created_by: user.id,
        key_values: Json(KeyValues(vec![])),
        object_status: aruna_server::database::enums::ObjectStatus::AVAILABLE,
        data_class: aruna_server::database::enums::DataClass::CONFIDENTIAL,
        object_type: aruna_server::database::enums::ObjectType::OBJECT,
        external_relations: Json(ExternalRelations(vec![])),
        hashes: vec![],
    };
    create_object.create(&client).await.unwrap();
    let get_obj = Object::get(obj_id, &client).await.unwrap().unwrap();
    assert!(get_obj == create_object);
}
