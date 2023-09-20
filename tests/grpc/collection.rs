use std::str::FromStr;

use aruna_rust_api::api::storage::{
    models::v2::{
        relation::Relation::Internal, DataClass, DataEndpoint, InternalRelation,
        InternalRelationVariant, KeyValue, KeyValueVariant, Relation, RelationDirection,
        ResourceVariant, Status,
    },
    services::v2::{
        collection_service_server::CollectionService, create_collection_request::Parent,
        CreateCollectionRequest,
    },
};
use diesel_ulid::DieselUlid;
use tonic::Request;

use crate::common::{
    init::{
        init_cache, init_collection_service_manual, init_database, init_database_handler,
        init_nats_client, init_permission_handler, init_project_service_manual, init_search_client,
    },
    test_utils::{
        add_token, fast_track_grpc_project_create, rand_string, ADMIN_OIDC_TOKEN,
        DEFAULT_ENDPOINT_ULID, USER_OIDC_TOKEN,
    },
};
use aruna_server::database::{crud::CrudDb, dsls::object_dsl::Object, enums::ObjectStatus};

#[tokio::test]
async fn grpc_create_collection() {
    // Init gRPC services
    let db = init_database().await;
    let nats = init_nats_client().await;
    let db_handler = init_database_handler(db.clone(), nats).await;
    let cache = init_cache(db.clone(), true).await;
    let auth = init_permission_handler(db.clone(), cache.clone()).await;
    let search = init_search_client().await;

    let project_service = init_project_service_manual(
        db_handler.clone(),
        auth.clone(),
        cache.clone(),
        search.clone(),
        DEFAULT_ENDPOINT_ULID.to_string(),
    )
    .await;
    let collection_service = init_collection_service_manual(db_handler, auth, cache, search).await;

    // Create random project
    let project = fast_track_grpc_project_create(&project_service, ADMIN_OIDC_TOKEN).await;

    // Create request with OIDC token in header
    let create_request = CreateCollectionRequest {
        name: "test-collection".to_string(),
        description: "Test Description".to_string(),
        key_values: vec![],
        external_relations: vec![],
        data_class: DataClass::Private as i32,
        parent: Some(Parent::ProjectId(project.id.to_string())),
    };
    let grpc_request = add_token(Request::new(create_request), ADMIN_OIDC_TOKEN);

    // Create project via gRPC service
    let create_response = collection_service
        .create_collection(grpc_request)
        .await
        .unwrap()
        .into_inner();

    let proto_collection = create_response.collection.unwrap();

    assert!(!proto_collection.id.is_empty());
    assert_eq!(proto_collection.name, "test-collection");
    assert_eq!(&proto_collection.description, "Test Description");
    assert_eq!(proto_collection.key_values, vec![]);
    assert_eq!(
        proto_collection.relations,
        vec![Relation {
            relation: Some(Internal(InternalRelation {
                resource_id: project.id.to_string(),
                resource_variant: ResourceVariant::Project as i32,
                defined_variant: InternalRelationVariant::BelongsTo as i32,
                custom_variant: None,
                direction: RelationDirection::Inbound as i32,
            }))
        }]
    );
    assert_eq!(proto_collection.data_class, 2);
    assert_eq!(proto_collection.status, Status::Available as i32);
    assert!(proto_collection.dynamic);
    assert_eq!(
        proto_collection.endpoints,
        vec![DataEndpoint {
            id: DEFAULT_ENDPOINT_ULID.to_string(),
            full_synced: true,
        }]
    );
}

#[tokio::test]
async fn grpc_get_collections() {
    // Init gRPC services
    let db = init_database().await;
    let nats = init_nats_client().await;
    let db_handler = init_database_handler(db.clone(), nats).await;
    let cache = init_cache(db.clone(), true).await;
    let auth = init_permission_handler(db.clone(), cache.clone()).await;
    let search = init_search_client().await;

    let project_service = init_project_service_manual(
        db_handler.clone(),
        auth.clone(),
        cache.clone(),
        search.clone(),
        DEFAULT_ENDPOINT_ULID.to_string(),
    )
    .await;
    let collection_service = init_collection_service_manual(db_handler, auth, cache, search).await;

    // Create random project and collection
    let project = fast_track_grpc_project_create(&project_service, ADMIN_OIDC_TOKEN).await;

    // Create collection
}

#[tokio::test]
async fn grpc_update_collection() {}

#[tokio::test]
async fn grpc_delete_collection() {}

#[tokio::test]
async fn grpc_snapshot_collection() {}
