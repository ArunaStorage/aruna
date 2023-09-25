use crate::common::{init, test_utils};
use aruna_server::database::dsls::hook_dsl::{Hook, HookVariant, TriggerType};
use aruna_server::database::dsls::internal_relation_dsl::InternalRelation;
use aruna_server::database::dsls::object_dsl::{
    DefinedVariant, ExternalRelation, Hierarchy, KeyValue, KeyValueVariant,
};
use aruna_server::database::enums::{DataClass, ObjectStatus, ObjectType};
use aruna_server::database::{
    crud::CrudDb,
    dsls::object_dsl::{ExternalRelations, KeyValues, Object, ObjectWithRelations},
    enums::ObjectMapping,
};
use chrono::{Days, NaiveDateTime, Utc};
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use postgres_types::Json;
use tokio_postgres::GenericClient;

#[tokio::test]
async fn create_hook() {
    let db = init::init_database().await;
    let client = db.get_client().await.unwrap();
    let proj_id = DieselUlid::generate();
    let mut user = test_utils::new_user(vec![ObjectMapping::PROJECT(proj_id)]);
    user.create(&client).await.unwrap();

    let hook_id = DieselUlid::generate();
    let mut hook = Hook {
        id: hook_id,
        name: "HookName".to_string(),
        description: "SOME_DESCRIPTION".to_string(),
        owner: user.id,
        project_id: proj_id,
        trigger_type: TriggerType::HOOK_ADDED,
        trigger_key: "TEST_KEY".to_string(),
        trigger_value: "TEST_VALUE".to_string(),
        timeout: chrono::Utc::now()
            .naive_utc()
            .checked_add_days(chrono::Days::new(1))
            .unwrap(),
        hook: Json(HookVariant::Internal(
            aruna_server::database::dsls::hook_dsl::InternalHook::AddLabel {
                key: "HOOK_STATUS".to_string(),
                value: "HOOK_TRIGGERED_SUCCESSFULL".to_string(),
            },
        )),
    };

    let mut create_project = test_utils::new_object(user.id, proj_id, ObjectType::PROJECT);
    let obj_id = DieselUlid::generate();
    let mut create_object = test_utils::new_object(user.id, obj_id, ObjectType::OBJECT);
    create_project.create(&client).await.unwrap();
    hook.create(&client).await.unwrap();

    create_object.create(&client).await.unwrap();
    assert!(Object::get(obj_id, &client)
        .await
        .unwrap()
        .unwrap()
        .key_values
        .0
         .0
        .is_empty());
    let keyval = KeyValue {
        key: "TEST_KEY".to_string(),
        value: "TEST_VALUE".to_string(),
        variant: KeyValueVariant::HOOK,
    };
    Object::add_key_value(&obj_id, &client, keyval)
        .await
        .unwrap();

    let get_obj = Object::get(obj_id, &client).await.unwrap().unwrap();
    dbg!(get_obj);
    //assert!(get_obj.key_values.0 .0.contains(KeyValue ));
}
