use aruna_server::database::dsls::internal_relation_dsl::InternalRelation;
use aruna_server::database::dsls::object_dsl::{ExternalRelations, Hashes, KeyValues, Object};
use aruna_server::database::dsls::user_dsl::{User, UserAttributes};
use aruna_server::database::enums::{DataClass, ObjectStatus, ObjectType};
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use postgres_types::Json;

pub fn new_user(object_ids: Vec<DieselUlid>) -> User {
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
pub fn new_object(user_id: DieselUlid, object_id: DieselUlid, object_type: ObjectType) -> Object {
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
        object_status: ObjectStatus::AVAILABLE,
        data_class: DataClass::CONFIDENTIAL,
        object_type,
        external_relations: Json(ExternalRelations(DashMap::default())),
        hashes: Json(Hashes(Vec::new())),
        dynamic: false,
        endpoints: Json(DashMap::default()),
    }
}
pub fn new_relation(origin: &Object, target: &Object) -> InternalRelation {
    InternalRelation {
        id: DieselUlid::generate(),
        origin_pid: origin.id,
        origin_type: origin.object_type.clone(),
        target_pid: target.id,
        target_type: target.object_type.clone(),
        relation_name: "BELONGS_TO".to_string(),
    }
}
