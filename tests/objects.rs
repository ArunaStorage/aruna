use aruna_server::database::crud::CrudDb;
use aruna_server::database::object_dsl::{ExternalRelations, KeyValues, Object};
use aruna_server::database::user_dsl::User;
use aruna_server::database::user_dsl::{Permission, UserAttributes};
use diesel_ulid::DieselUlid;
use postgres_types::Json;

mod init_db;

#[tokio::test]
async fn create_object() {
    let db = crate::init_db::init_db().await;
    let client = db.get_client().await.unwrap();

    let obj_id = DieselUlid::generate();

    let attributes = Json(UserAttributes {
        global_admin: false,
        service_account: false,
        custom_attributes: Vec::new(),
        permissions: vec![Permission {
            resource_id: obj_id,
            permission_level: aruna_server::database::enums::PermissionLevels::WRITE,
        }],
    });

    let user = User {
        id: DieselUlid::generate(),
        display_name: "aha".to_string(),
        email: "aja".to_string(),
        attributes,
        active: true,
    };

    user.create(&client).await.unwrap();

    let create_object = Object {
        id: obj_id,
        shared_id: DieselUlid::generate(),
        revision_number: 0,
        name: "a".to_string(),
        description: "b".to_string(),
        count: 1,
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
    dbg!(&get_obj);
    dbg!(&create_object);
    assert!(get_obj == create_object);
}
