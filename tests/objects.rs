use aruna_server::database::dsls::object_dsl::{
    DefinedVariant, ExternalRelation, KeyValue, KeyValueVariant,
};
use aruna_server::database::enums::{DataClass, ObjectStatus, ObjectType};
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
    let transaction = client.transaction().await.unwrap();

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
    let archive = object_vec.clone();
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
        .all(|o| compare_owrs.contains(o)));

    // Test archive
    let archived_objects = Object::archive(&archive, client).await.unwrap();
    transaction.commit().await.unwrap();
    for o in archived_objects {
        assert!(!o.object.dynamic);
    }
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
#[tokio::test]
async fn test_external_relations() {
    let db = init_db::init_db().await;
    let mut client = db.get_client().await.unwrap();
    let transaction = client.transaction().await.unwrap();

    let client = transaction.client();

    let obj_id = DieselUlid::generate();

    let user = new_user(vec![obj_id]);
    user.create(client).await.unwrap();

    let create_object = new_object(user.id, obj_id, ObjectType::OBJECT);
    create_object.create(client).await.unwrap();
    let url = ExternalRelation {
        identifier: "test.test/abc".to_string(),
        defined_variant: DefinedVariant::URL,
        custom_variant: None,
    };
    let id = ExternalRelation {
        identifier: "a.b/c".to_string(),
        defined_variant: DefinedVariant::IDENTIFIER,
        custom_variant: None,
    };
    let custom = ExternalRelation {
        identifier: "ThIs Is A cUsToM fLaG".to_string(),
        defined_variant: DefinedVariant::CUSTOM,
        custom_variant: Some("This is not a URL or an identifier".to_string()),
    };
    let rels = vec![url.clone(), id.clone(), custom.clone()];
    Object::add_external_relations(&obj_id, client, rels.clone())
        .await
        .unwrap();
    let mut compare_obj = Object {
        id: obj_id,
        revision_number: create_object.revision_number,
        name: create_object.name,
        description: create_object.description,
        created_at: create_object.created_at,
        created_by: create_object.created_by,
        content_len: create_object.content_len,
        count: create_object.count,
        key_values: create_object.key_values,
        object_status: create_object.object_status,
        data_class: create_object.data_class,
        object_type: create_object.object_type,
        external_relations: Json(ExternalRelations(DashMap::from_iter(
            rels.clone().into_iter().map(|r| (r.identifier.clone(), r)),
        ))),
        hashes: create_object.hashes,
        dynamic: create_object.dynamic,
        endpoints: create_object.endpoints,
    };
    let obj = Object::get(obj_id, client).await.unwrap().unwrap();
    //dbg!(&obj);
    assert_eq!(compare_obj, obj);
    let rm_rels = vec![custom, id];
    let remain_rels = vec![url];
    Object::remove_external_relation(&obj_id, client, rm_rels)
        .await
        .unwrap();
    let rm = Object::get(obj_id, client).await.unwrap().unwrap();
    transaction.commit().await.unwrap();
    compare_obj.external_relations = Json(ExternalRelations(DashMap::from_iter(
        remain_rels.into_iter().map(|e| (e.identifier.clone(), e)),
    )));
    assert_eq!(compare_obj, rm);
}

#[tokio::test]
async fn test_updates() {
    let db = init_db::init_db().await;
    let mut client = db.get_client().await.unwrap();
    let transaction = client.transaction().await.unwrap();

    let client = transaction.client();

    let obj_id = DieselUlid::generate();
    let dat_id = DieselUlid::generate();
    let col_id = DieselUlid::generate();
    let proj_id = DieselUlid::generate();

    let user = new_user(vec![obj_id]);
    user.create(client).await.unwrap();

    let mut create_object = new_object(user.id, obj_id, ObjectType::OBJECT);
    let mut create_dataset = new_object(user.id, dat_id, ObjectType::DATASET);
    let mut create_collection = new_object(user.id, col_id, ObjectType::COLLECTION);
    let mut create_project = new_object(user.id, proj_id, ObjectType::PROJECT);
    create_object.create(client).await.unwrap();
    create_dataset.create(client).await.unwrap();
    create_collection.create(client).await.unwrap();
    create_project.create(client).await.unwrap();

    // Update Object
    create_object.description = "This is a new description.".to_string();
    create_object.data_class = DataClass::PUBLIC;
    create_object.key_values = Json(KeyValues(vec![KeyValue {
        key: "NewKey".to_string(),
        value: "NewValue".to_string(),
        variant: KeyValueVariant::LABEL,
    }]));
    create_object.update(client).await.unwrap();
    let updated_object = Object::get(obj_id, client).await.unwrap().unwrap();
    assert_eq!(updated_object, create_object);

    // Update Dataset name
    let new_name = "new_dataset_name.xyz".to_string();
    Object::update_name(dat_id, new_name.clone(), client)
        .await
        .unwrap();
    let updated_dataset = Object::get(dat_id, client).await.unwrap().unwrap();
    create_dataset.name = new_name;
    assert_eq!(updated_dataset, create_dataset);

    // Update Collection description
    let new_description = "New description".to_string();
    Object::update_description(col_id, new_description.clone(), client)
        .await
        .unwrap();
    let updated_collection = Object::get(col_id, client).await.unwrap().unwrap();
    create_collection.description = new_description;
    assert_eq!(updated_collection, create_collection);

    // Update Project dataclass
    let new_dataclass = DataClass::PUBLIC;
    Object::update_dataclass(proj_id, new_dataclass.clone(), client)
        .await
        .unwrap();
    let updated_project = Object::get(proj_id, client).await.unwrap().unwrap();
    transaction.commit().await.unwrap();
    create_project.data_class = new_dataclass;
    assert_eq!(updated_project, create_project);
}
#[tokio::test]
async fn test_delete() {
    let db = init_db::init_db().await;
    let mut client = db.get_client().await.unwrap();
    let transaction = client.transaction().await.unwrap();

    let client = transaction.client();

    let mut obj_ids = Vec::new();
    for _ in 1..5 {
        obj_ids.push(DieselUlid::generate());
    }
    let user = new_user(obj_ids.clone());
    user.create(client).await.unwrap();

    let mut objects = Vec::new();
    for id in &obj_ids {
        objects.push(new_object(user.id, *id, ObjectType::OBJECT));
    }
    Object::batch_create(&objects, client).await.unwrap();
    let objects = Object::get_objects(&obj_ids, client).await.unwrap();
    for o in objects {
        assert_eq!(o.object_status, ObjectStatus::AVAILABLE);
    }
    Object::set_deleted(&obj_ids, client).await.unwrap();
    let deleted = Object::get_objects(&obj_ids, client).await.unwrap();
    transaction.commit().await.unwrap();
    for o in deleted {
        assert_eq!(o.object_status, ObjectStatus::DELETED);
    }
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
