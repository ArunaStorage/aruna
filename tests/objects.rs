use aruna_server::database::crud::CrudDb;
use aruna_server::database::internal_relation_dsl::InternalRelation;
use aruna_server::database::object_dsl::{
    ExternalRelations,
    Hashes, //InternalRelation2,
    KeyValues,
    Object,
    ObjectWithRelations,
};
use aruna_server::database::object_dsl::{Inbound, Outbound};
use aruna_server::database::relation_type_dsl::RelationType;
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
        hashes: Json(Hashes(Vec::new())),
    };
    create_object.create(&client).await.unwrap();
    let get_obj = Object::get(obj_id, &client).await.unwrap().unwrap();
    assert!(get_obj == create_object);
}
#[tokio::test]
async fn get_object_with_relations_test() {
    let db = crate::init_db::init_db().await;
    let mut client = db.get_client().await.unwrap();
    let transaction = client
        .transaction()
        .await
        .map_err(|e| {
            log::error!("{}", e);
            tonic::Status::unavailable("Database not avaliable.")
        })
        .unwrap();

    let client = transaction.client();

    let dataset_id = DieselUlid::generate();
    let collection_one = DieselUlid::generate();
    let collection_two = DieselUlid::generate();
    let object_one = DieselUlid::generate();
    let object_two = DieselUlid::generate();
    let object_vec = vec![
        dataset_id,
        collection_one,
        collection_two,
        object_one,
        object_two,
    ];

    let attributes = Json(UserAttributes {
        global_admin: false,
        service_account: false,
        custom_attributes: Vec::new(),
        permissions: object_vec
            .iter()
            .map(|o| Permission {
                resource_id: *o,
                permission_level: aruna_server::database::enums::PermissionLevels::WRITE,
            })
            .collect(),
    });

    let user = User {
        id: DieselUlid::generate(),
        display_name: "aha".to_string(),
        email: "aja".to_string(),
        attributes,
        active: true,
    };

    user.create(&client).await.unwrap();

    let insert = "INSERT INTO relation_types (id, relation_name) VALUES ($1, $2);";
    let prepared = client.prepare(insert).await.unwrap();
    let rel_type = RelationType {
        id: 1,
        relation_name: "BELONGS_TO".to_string(),
    };

    client
        .execute(&prepared, &[&rel_type.id, &rel_type.relation_name])
        .await
        .unwrap();

    let create_dataset = Object {
        id: dataset_id,
        shared_id: DieselUlid::generate(),
        revision_number: 0,
        name: "dataset".to_string(),
        description: "test".to_string(),
        count: 2,
        created_at: None,
        content_len: 0,
        created_by: user.id,
        key_values: Json(KeyValues(vec![])),
        object_status: aruna_server::database::enums::ObjectStatus::AVAILABLE,
        data_class: aruna_server::database::enums::DataClass::PUBLIC,
        object_type: aruna_server::database::enums::ObjectType::DATASET,
        external_relations: Json(ExternalRelations(vec![])),
        hashes: Json(Hashes(Vec::new())),
    };
    let create_collection_one = Object {
        id: collection_one,
        shared_id: DieselUlid::generate(),
        revision_number: 0,
        name: "collection_one".to_string(),
        description: "test".to_string(),
        count: 3,
        created_at: None,
        content_len: 0,
        created_by: user.id,
        key_values: Json(KeyValues(vec![])),
        object_status: aruna_server::database::enums::ObjectStatus::AVAILABLE,
        data_class: aruna_server::database::enums::DataClass::PUBLIC,
        object_type: aruna_server::database::enums::ObjectType::COLLECTION,
        external_relations: Json(ExternalRelations(vec![])),
        hashes: Json(Hashes(Vec::new())),
    };
    let create_collection_two = Object {
        id: collection_two,
        shared_id: DieselUlid::generate(),
        revision_number: 0,
        name: "collection_two".to_string(),
        description: "test".to_string(),
        count: 3,
        created_at: None,
        content_len: 0,
        created_by: user.id,
        key_values: Json(KeyValues(vec![])),
        object_status: aruna_server::database::enums::ObjectStatus::AVAILABLE,
        data_class: aruna_server::database::enums::DataClass::PUBLIC,
        object_type: aruna_server::database::enums::ObjectType::COLLECTION,
        external_relations: Json(ExternalRelations(vec![])),
        hashes: Json(Hashes(Vec::new())),
    };
    let create_object_one = Object {
        id: object_one,
        shared_id: DieselUlid::generate(),
        revision_number: 0,
        name: "object_one".to_string(),
        description: "test".to_string(),
        count: 1,
        created_at: None,
        content_len: 0,
        created_by: user.id,
        key_values: Json(KeyValues(vec![])),
        object_status: aruna_server::database::enums::ObjectStatus::AVAILABLE,
        data_class: aruna_server::database::enums::DataClass::PUBLIC,
        object_type: aruna_server::database::enums::ObjectType::OBJECT,
        external_relations: Json(ExternalRelations(vec![])),
        hashes: Json(Hashes(Vec::new())),
    };
    let create_object_two = Object {
        id: object_two,
        shared_id: DieselUlid::generate(),
        revision_number: 0,
        name: "object_two".to_string(),
        description: "test".to_string(),
        count: 1,
        created_at: None,
        content_len: 0,
        created_by: user.id,
        key_values: Json(KeyValues(vec![])),
        object_status: aruna_server::database::enums::ObjectStatus::AVAILABLE,
        data_class: aruna_server::database::enums::DataClass::PUBLIC,
        object_type: aruna_server::database::enums::ObjectType::OBJECT,
        external_relations: Json(ExternalRelations(vec![])),
        hashes: Json(Hashes(Vec::new())),
    };
    let creates = vec![
        create_dataset.clone(),
        create_object_one,
        create_object_two,
        create_collection_one,
        create_collection_two,
    ];

    for c in creates {
        c.create(&client).await.unwrap();
    }
    let create_relation_one = InternalRelation {
        id: DieselUlid::generate(),
        origin_pid: collection_one,
        target_pid: dataset_id,
        is_persistent: true,
        type_id: 1,
    };
    let create_relation_two = InternalRelation {
        id: DieselUlid::generate(),
        origin_pid: collection_two,
        target_pid: dataset_id,
        is_persistent: true,
        type_id: 1,
    };
    let create_relation_three = InternalRelation {
        id: DieselUlid::generate(),
        origin_pid: dataset_id,
        target_pid: object_one,
        is_persistent: true,
        type_id: 1,
    };
    let create_relation_four = InternalRelation {
        id: DieselUlid::generate(),
        origin_pid: dataset_id,
        target_pid: object_two,
        is_persistent: true,
        type_id: 1,
    };
    let rels = vec![
        create_relation_one.clone(),
        create_relation_two.clone(),
        create_relation_three.clone(),
        create_relation_four.clone(),
    ];
    for r in rels {
        dbg!(&r);
        r.create(&client).await.unwrap();
    }
    let compare_owr = ObjectWithRelations {
        object: create_dataset,
        inbound: Json(Inbound(vec![create_relation_one, create_relation_two])),
        outbound: Json(Outbound(vec![
            create_relation_three,
            create_relation_four.clone(),
        ])),
    };
    let object_with_relations = Object::get_object_with_relations(&dataset_id, &client)
        .await
        .unwrap();

    dbg!(&object_with_relations);
    dbg!(&compare_owr);
    assert!(object_with_relations == compare_owr);
}
