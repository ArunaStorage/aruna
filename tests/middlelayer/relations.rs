use std::sync::Arc;

use crate::common::{init_db::init_handler, test_utils};
use aruna_rust_api::api::storage::models::v2::relation::Relation as RelationEnum;
use aruna_rust_api::api::storage::models::v2::ExternalRelation as APIExternalRelation;
use aruna_rust_api::api::storage::models::v2::InternalRelation as APIInternalRelation;
use aruna_rust_api::api::storage::models::v2::Relation;
use aruna_rust_api::api::storage::services::v2::ModifyRelationsRequest;
use aruna_server::caching::cache::Cache;
use aruna_server::database::crud::CrudDb;
use aruna_server::database::dsls::internal_relation_dsl::{
    InternalRelation, INTERNAL_RELATION_VARIANT_METADATA,
};
use aruna_server::database::dsls::object_dsl::ObjectWithRelations;
use aruna_server::database::dsls::object_dsl::{DefinedVariant, ExternalRelation, Object};
use aruna_server::database::enums::{ObjectMapping, ObjectType};
use aruna_server::middlelayer::relations_request_types::ModifyRelations;
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use itertools::Itertools;
use postgres_types::Json;

#[tokio::test]
async fn test_modify() {
    // init
    let db_handler = init_handler().await;
    let client = db_handler.database.get_client().await.unwrap();
    let origin = DieselUlid::generate();
    let target = DieselUlid::generate();
    let mut user = test_utils::new_user(vec![
        ObjectMapping::DATASET(origin),
        ObjectMapping::OBJECT(target),
    ]);
    user.create(&client).await.unwrap();
    let objects = vec![
        test_utils::new_object(user.id, origin, ObjectType::DATASET),
        test_utils::new_object(user.id, target, ObjectType::OBJECT),
    ];
    Object::batch_create(&objects, &client).await.unwrap();
    let belongs_to = test_utils::new_internal_relation(&objects[0], &objects[1]);
    let other = InternalRelation {
        id: DieselUlid::generate(),
        origin_pid: origin,
        origin_type: ObjectType::DATASET,
        relation_name: INTERNAL_RELATION_VARIANT_METADATA.to_string(),
        target_pid: target,
        target_type: ObjectType::OBJECT,
        target_name: objects[1].name.to_string(),
    };
    InternalRelation::batch_create(&vec![belongs_to, other], &client)
        .await
        .unwrap();

    // test
    let rel_mod_one = Relation {
        relation: Some(RelationEnum::External(APIExternalRelation {
            identifier: "test.test".to_string(),
            defined_variant: 1, // URL
            custom_variant: None,
        })),
    };
    let rel_mod_two = Relation {
        relation: Some(RelationEnum::Internal(APIInternalRelation {
            resource_id: target.to_string(),
            defined_variant: 5, // POLICY
            custom_variant: None,
            resource_variant: 4,
            direction: 2,
        })),
    };
    let rel_del_one = Relation {
        relation: Some(RelationEnum::Internal(APIInternalRelation {
            resource_id: target.to_string(),
            defined_variant: 1, // BELONGS_TO
            custom_variant: None,
            resource_variant: 4,
            direction: 2,
        })),
    };
    let request = ModifyRelations(ModifyRelationsRequest {
        resource_id: origin.to_string(),
        add_relations: vec![rel_mod_one, rel_mod_two],
        remove_relations: vec![rel_del_one],
    });

    let cache = Arc::new(Cache::new());
    for obj in objects {
        cache.add_object(ObjectWithRelations {
            object: obj.clone(),
            inbound: Json(DashMap::default()),
            inbound_belongs_to: Json(DashMap::default()),
            outbound: Json(DashMap::default()),
            outbound_belongs_to: Json(DashMap::default()),
        });
    }

    let (obj, mod_lab) = db_handler.get_resource(request, cache).await.unwrap();
    let owr = db_handler
        .modify_relations(obj, mod_lab.relations_to_add, mod_lab.relations_to_remove)
        .await
        .unwrap();
    assert!(owr.inbound.0.is_empty());
    assert!(owr.inbound_belongs_to.0.is_empty());
    assert_eq!(owr.outbound.0.len(), 2);
    assert!(owr.outbound_belongs_to.0.is_empty());
    assert!(owr
        .object
        .external_relations
        .0
         .0
        .into_iter()
        .map(|i| i.1)
        .contains(&ExternalRelation {
            identifier: "test.test".to_string(),
            defined_variant: DefinedVariant::URL,
            custom_variant: None,
        }))
}
