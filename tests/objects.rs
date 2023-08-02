use aruna_server::database::dsls::object_dsl::{KeyValue, KeyValueVariant};
use aruna_server::database::enums::ObjectType;
use aruna_server::database::{
    crud::CrudDb,
    dsls::{
        internal_relation_dsl::InternalRelation,
        object_dsl::{ExternalRelations, Hashes, KeyValues, Object, ObjectWithRelations},
        user_dsl::{User, UserAttributes},
    },
};
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use postgres_types::Json;

mod init_db;

#[tokio::test]
async fn create_object() {
    let db = init_db::init_db().await;
    let client = db.get_client().await.unwrap();

    let obj_id = DieselUlid::generate();

    let user = new_user(vec![obj_id]);
    user.create(&client).await.unwrap();

    let create_object = new_object(user.id, obj_id, ObjectType::OBJECT);
    create_object.create(&client).await.unwrap();

    let get_obj = Object::get(obj_id, &client).await.unwrap().unwrap();
    assert_eq!(get_obj, create_object);
}
#[tokio::test]
async fn get_object_with_relations_test() {
    let db = init_db::init_db().await;
    let mut client = db.get_client().await.unwrap();
    let transaction = client
        .transaction()
        .await
        .map_err(|e| {
            log::error!("{}", e);
            tonic::Status::unavailable("Database not available.")
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
    let user = new_user(object_vec.clone());
    user.create(client).await.unwrap();

    let create_dataset = new_object(user.id, dataset_id, ObjectType::DATASET);
    let create_collection_one = new_object(user.id, collection_one, ObjectType::COLLECTION);
    let create_collection_two = new_object(user.id, collection_two, ObjectType::COLLECTION);
    let create_object_one = new_object(user.id, object_one, ObjectType::OBJECT);
    let create_object_two = new_object(user.id, object_two, ObjectType::OBJECT);

    let create_relation_one = new_relation(&create_collection_one, &create_dataset);
    let create_relation_two = new_relation(&create_collection_two, &create_dataset);
    let create_relation_three = new_relation(&create_dataset, &create_object_one);
    let create_relation_four = new_relation(&create_dataset, &create_object_two);

    let creates = vec![
        create_dataset.clone(),
        create_object_one.clone(),
        create_object_two.clone(),
        create_collection_one.clone(),
        create_collection_two.clone(),
    ];

    Object::batch_create(&creates, client).await.unwrap();

    let rels = vec![
        create_relation_one.clone(),
        create_relation_two.clone(),
        create_relation_three.clone(),
        create_relation_four.clone(),
    ];
    InternalRelation::batch_create(&rels, client).await.unwrap();

    let compare_owr = ObjectWithRelations {
        object: create_dataset,
        inbound: Json(DashMap::default()),
        inbound_belongs_to: Json(DashMap::from_iter([
            (create_relation_one.origin_pid, create_relation_one.clone()),
            (create_relation_two.origin_pid, create_relation_two.clone()),
        ])),
        outbound: Json(DashMap::default()),
        outbound_belongs_to: Json(DashMap::from_iter([
            (
                create_relation_three.target_pid,
                create_relation_three.clone(),
            ),
            (
                create_relation_four.target_pid,
                create_relation_four.clone(),
            ),
        ])),
    };
    let object_with_relations = Object::get_object_with_relations(&dataset_id, client)
        .await
        .unwrap();
    assert_eq!(object_with_relations, compare_owr);

    let objects_with_relations = Object::get_objects_with_relations(&object_vec, client)
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    assert!(!objects_with_relations.is_empty());
    let compare_collection_one = ObjectWithRelations {
        object: create_collection_one,
        inbound: Json(DashMap::default()),
        inbound_belongs_to: Json(DashMap::default()),
        outbound: Json(DashMap::default()),
        outbound_belongs_to: Json(DashMap::from_iter([(
            create_relation_one.target_pid,
            create_relation_one,
        )])),
    };
    let compare_collection_two = ObjectWithRelations {
        object: create_collection_two,
        inbound: Json(DashMap::default()),
        inbound_belongs_to: Json(DashMap::default()),
        outbound: Json(DashMap::default()),
        outbound_belongs_to: Json(DashMap::from_iter([(
            create_relation_two.target_pid,
            create_relation_two,
        )])),
    };
    let compare_object_one = ObjectWithRelations {
        object: create_object_one,
        inbound: Json(DashMap::default()),
        inbound_belongs_to: Json(DashMap::from_iter([(
            create_relation_three.origin_pid,
            create_relation_three,
        )])),
        outbound: Json(DashMap::default()),
        outbound_belongs_to: Json(DashMap::default()),
    };
    let compare_object_two = ObjectWithRelations {
        object: create_object_two,
        inbound: Json(DashMap::default()),
        inbound_belongs_to: Json(DashMap::from_iter([(
            create_relation_four.origin_pid,
            create_relation_four,
        )])),
        outbound: Json(DashMap::default()),
        outbound_belongs_to: Json(DashMap::default()),
    };
    let compare_owrs = vec![
        compare_collection_one,
        compare_collection_two,
        compare_owr,
        compare_object_one,
        compare_object_two,
    ];
    assert!(objects_with_relations
        .iter()
        .all(|o| compare_owrs.contains(o)))
}
#[tokio::test]
async fn test_keyvals() {
    let db = init_db::init_db().await;
    let client = db.get_client().await.unwrap();

    let obj_id = DieselUlid::generate();

    let user = new_user(vec![obj_id]);
    user.create(&client).await.unwrap();

    let create_object = new_object(user.id, obj_id, ObjectType::OBJECT);
    create_object.create(&client).await.unwrap();

    let kv = KeyValue {
        key: "one".to_string(),
        value: "two".to_string(),
        variant: KeyValueVariant::LABEL,
    };
    Object::add_key_value(&obj_id, &client, kv.clone())
        .await
        .unwrap();

    let object = Object::get(obj_id, &client).await.unwrap().unwrap();
    let test_object = create_object.clone();
    let comp_obj = Object {
        id: obj_id,
        revision_number: create_object.revision_number,
        name: create_object.name,
        description: create_object.description,
        created_at: create_object.created_at,
        created_by: create_object.created_by,
        content_len: create_object.content_len,
        count: create_object.count,
        key_values: Json(KeyValues(vec![kv.clone()])),
        object_status: create_object.object_status,
        data_class: create_object.data_class,
        object_type: create_object.object_type,
        external_relations: create_object.external_relations,
        hashes: create_object.hashes,
        dynamic: create_object.dynamic,
        endpoints: create_object.endpoints,
    };
    assert_eq!(object, comp_obj);
    object.remove_key_value(&client, kv).await.unwrap();
    let object = Object::get(obj_id, &client).await.unwrap().unwrap();
    let comp_obj = Object {
        id: obj_id,
        revision_number: test_object.revision_number,
        name: test_object.name,
        description: test_object.description,
        created_at: test_object.created_at,
        created_by: test_object.created_by,
        content_len: test_object.content_len,
        count: test_object.count,
        key_values: Json(KeyValues(Vec::new())),
        object_status: test_object.object_status,
        data_class: test_object.data_class,
        object_type: test_object.object_type,
        external_relations: test_object.external_relations,
        hashes: test_object.hashes,
        dynamic: test_object.dynamic,
        endpoints: test_object.endpoints,
    };
    assert_eq!(object, comp_obj);
}

fn new_user(object_ids: Vec<DieselUlid>) -> User {
    let attributes = Json(UserAttributes {
        global_admin: false,
        service_account: false,
        custom_attributes: Vec::new(),
        tokens: DashMap::default(),
        trusted_endpoints: DashMap::default(),
        permissions: DashMap::from_iter(
            object_ids
                .iter()
                .map(|o| (*o, aruna_server::database::enums::DbPermissionLevel::WRITE)),
        ),
    });
    User {
        id: DieselUlid::generate(),
        display_name: "test1".to_string(),
        external_id: None,
        email: "test2@test3".to_string(),
        attributes,
        active: true,
    }
}
fn new_object(user_id: DieselUlid, object_id: DieselUlid, object_type: ObjectType) -> Object {
    Object {
        id: object_id,
        revision_number: 0,
        name: "a".to_string(),
        description: "b".to_string(),
        count: 1,
        created_at: None,
        content_len: 1337,
        created_by: user_id,
        key_values: Json(KeyValues(vec![])),
        object_status: aruna_server::database::enums::ObjectStatus::AVAILABLE,
        data_class: aruna_server::database::enums::DataClass::CONFIDENTIAL,
        object_type,
        external_relations: Json(ExternalRelations(DashMap::default())),
        hashes: Json(Hashes(Vec::new())),
        dynamic: false,
        endpoints: Json(DashMap::default()),
    }
}
fn new_relation(origin: &Object, target: &Object) -> InternalRelation {
    InternalRelation {
        id: DieselUlid::generate(),
        origin_pid: origin.id,
        origin_type: origin.object_type.clone(),
        target_pid: target.id,
        target_type: target.object_type.clone(),
        relation_name: "BELONGS_TO".to_string(),
    }
}
