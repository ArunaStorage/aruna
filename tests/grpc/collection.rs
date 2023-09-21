use std::str::FromStr;

use aruna_rust_api::api::storage::{
    models::v2::{
        relation::Relation::Internal, DataClass, DataEndpoint, InternalRelation,
        InternalRelationVariant, Relation, RelationDirection, ResourceVariant, Status,
    },
    services::v2::{
        collection_service_server::CollectionService, create_collection_request::Parent,
        CreateCollectionRequest, GetCollectionRequest,
    },
};
use diesel_ulid::DieselUlid;
use tonic::Request;

use crate::common::{
    init::{
        init_auth_service_manual, init_cache, init_collection_service_manual, init_database,
        init_database_handler, init_nats_client, init_permission_handler,
        init_project_service_manual, init_search_client,
    },
    test_utils::{
        add_token, fast_track_grpc_collection_create, fast_track_grpc_permission_add,
        fast_track_grpc_project_create, ADMIN_OIDC_TOKEN, DEFAULT_ENDPOINT_ULID, USER_OIDC_TOKEN,
    },
};
use aruna_server::database::enums::DbPermissionLevel;

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

    let auth_service =
        init_auth_service_manual(db_handler.clone(), auth.clone(), cache.clone()).await;

    let project_service = init_project_service_manual(
        db_handler.clone(),
        auth.clone(),
        cache.clone(),
        search.clone(),
        DEFAULT_ENDPOINT_ULID.to_string(),
    )
    .await;

    let collection_service =
        init_collection_service_manual(db_handler.clone(), auth, cache, search).await;

    // Get normal user id as DieselUlid
    let user_ulid = DieselUlid::from_str("01H8KWYY5MTAH1YZGPYVS7PQWD").unwrap();

    // Create random project and collection
    let project = fast_track_grpc_project_create(&project_service, ADMIN_OIDC_TOKEN).await;

    // Create collection
    let collection = fast_track_grpc_collection_create(
        &collection_service,
        ADMIN_OIDC_TOKEN,
        Parent::ProjectId(project.id.clone()),
    )
    .await;
    let collection_ulid = DieselUlid::from_str(&collection.id).unwrap();

    // Try get Collection that does not exist
    let mut get_request = GetCollectionRequest {
        collection_id: DieselUlid::generate().to_string(),
    };

    let tokenless_request = add_token(tonic::Request::new(get_request.clone()), ADMIN_OIDC_TOKEN);

    let response = collection_service.get_collection(tokenless_request).await;

    assert!(response.is_err());

    // Try get Collection without token
    get_request.collection_id = collection.id.clone();

    let response = collection_service
        .get_collection(tonic::Request::new(get_request.clone()))
        .await;

    assert!(response.is_err());

    // Get Collection without permissions
    let grpc_request = add_token(tonic::Request::new(get_request.clone()), USER_OIDC_TOKEN);

    let response = collection_service.get_collection(grpc_request).await;

    assert!(response.is_err());

    // Get Collection with permissions
    let grpc_request = add_token(tonic::Request::new(get_request.clone()), USER_OIDC_TOKEN);

    fast_track_grpc_permission_add(
        &auth_service,
        ADMIN_OIDC_TOKEN,
        &user_ulid,
        &collection_ulid,
        DbPermissionLevel::READ,
    )
    .await;

    let proto_collection = collection_service
        .get_collection(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .collection
        .unwrap();

    // Validate returned collection
    assert_eq!(collection.id, proto_collection.id);
    assert_eq!(collection.name, proto_collection.name);
    assert_eq!(collection.description, proto_collection.description);
    assert_eq!(collection.key_values, proto_collection.key_values);
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
    assert_eq!(collection.data_class, proto_collection.data_class);
    assert_eq!(collection.status, proto_collection.status);
    assert_eq!(collection.dynamic, proto_collection.dynamic);
}

#[tokio::test]
async fn grpc_update_collection() {}

#[tokio::test]
async fn grpc_delete_collection() {}

#[tokio::test]
async fn grpc_snapshot_collection() {}
