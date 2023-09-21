use std::str::FromStr;

use aruna_rust_api::api::storage::{
    models::v2::{
        relation::Relation::Internal, DataClass, DataEndpoint, InternalRelation,
        InternalRelationVariant, Relation, RelationDirection, ResourceVariant, Status,
    },
    services::v2::{
        collection_service_server::CollectionService, create_collection_request::Parent,
        CreateCollectionRequest, GetCollectionRequest, UpdateCollectionDataClassRequest,
        UpdateCollectionDescriptionRequest, UpdateCollectionNameRequest,
    },
};
use diesel_ulid::DieselUlid;
use tonic::Request;

use crate::common::{
    init::init_grpc_services,
    test_utils::{
        add_token, fast_track_grpc_collection_create, fast_track_grpc_permission_add,
        fast_track_grpc_project_create, ADMIN_OIDC_TOKEN, DEFAULT_ENDPOINT_ULID, GENERIC_USER_ULID,
        USER_OIDC_TOKEN,
    },
};
use aruna_server::database::enums::DbPermissionLevel;

#[tokio::test]
async fn grpc_create_collection() {
    // Init gRPC services
    let (_, project_service, collection_service, _, _, _) = init_grpc_services().await;

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
    let (auth_service, project_service, collection_service, _, _, _) = init_grpc_services().await;

    // Get normal user id as DieselUlid
    let user_ulid = DieselUlid::from_str(GENERIC_USER_ULID).unwrap();

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
async fn grpc_update_collection_name() {
    // Init gRPC services
    let (auth_service, project_service, collection_service, _, _, _) = init_grpc_services().await;

    // Create random Project + Collection
    let project = fast_track_grpc_project_create(&project_service, ADMIN_OIDC_TOKEN).await;
    let collection = fast_track_grpc_collection_create(
        &collection_service,
        ADMIN_OIDC_TOKEN,
        Parent::ProjectId(project.id.clone()),
    )
    .await;

    // Create user/resource ulids
    let user_ulid = DieselUlid::from_str(GENERIC_USER_ULID).unwrap();
    let collection_ulid = DieselUlid::from_str(&collection.id).unwrap();

    // Update Collection without token
    let mut inner_request = UpdateCollectionNameRequest {
        collection_id: collection.id.to_string(),
        name: "updated-name".to_string(),
    };

    let grpc_request = Request::new(inner_request.clone());

    let response = collection_service
        .update_collection_name(grpc_request)
        .await;

    assert!(response.is_err());

    // Update non-existing Collection
    inner_request.collection_id = DieselUlid::generate().to_string();

    let grpc_request = add_token(Request::new(inner_request.clone()), ADMIN_OIDC_TOKEN);

    let response = collection_service
        .update_collection_name(grpc_request)
        .await;

    assert!(response.is_err());

    // Update Collection without sufficient permissions
    inner_request.collection_id = collection.id.to_string();

    let grpc_request = add_token(Request::new(inner_request.clone()), USER_OIDC_TOKEN);

    let response = collection_service
        .update_collection_name(grpc_request)
        .await;

    assert!(response.is_err());

    // Update Collection with sufficient permissions
    fast_track_grpc_permission_add(
        &auth_service,
        ADMIN_OIDC_TOKEN,
        &user_ulid,
        &collection_ulid,
        DbPermissionLevel::WRITE,
    )
    .await;

    let grpc_request = add_token(Request::new(inner_request.clone()), USER_OIDC_TOKEN);

    let proto_collection = collection_service
        .update_collection_name(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .collection
        .unwrap();

    assert_eq!(collection.id, proto_collection.id);
    assert_eq!(&proto_collection.name, "updated-name");
}

#[tokio::test]
async fn grpc_update_collection_description() {
    // Init gRPC services
    let (auth_service, project_service, collection_service, _, _, _) = init_grpc_services().await;

    // Create random Project + Collection
    let project = fast_track_grpc_project_create(&project_service, ADMIN_OIDC_TOKEN).await;
    let collection = fast_track_grpc_collection_create(
        &collection_service,
        ADMIN_OIDC_TOKEN,
        Parent::ProjectId(project.id.clone()),
    )
    .await;

    // Create user/resource ulids
    let user_ulid = DieselUlid::from_str(GENERIC_USER_ULID).unwrap();
    let collection_ulid = DieselUlid::from_str(&collection.id).unwrap();

    // Update Collection without token
    let mut inner_request = UpdateCollectionDescriptionRequest {
        collection_id: collection.id.to_string(),
        description: "Updated description".to_string(),
    };

    let grpc_request = Request::new(inner_request.clone());

    let response = collection_service
        .update_collection_description(grpc_request)
        .await;

    assert!(response.is_err());

    // Update non-existing Collection
    inner_request.collection_id = DieselUlid::generate().to_string();

    let grpc_request = add_token(Request::new(inner_request.clone()), ADMIN_OIDC_TOKEN);

    let response = collection_service
        .update_collection_description(grpc_request)
        .await;

    assert!(response.is_err());

    // Update Collection without sufficient permissions
    inner_request.collection_id = collection.id.to_string();

    let grpc_request = add_token(Request::new(inner_request.clone()), USER_OIDC_TOKEN);

    let response = collection_service
        .update_collection_description(grpc_request)
        .await;

    assert!(response.is_err());

    // Update Collection with sufficient permissions
    fast_track_grpc_permission_add(
        &auth_service,
        ADMIN_OIDC_TOKEN,
        &user_ulid,
        &collection_ulid,
        DbPermissionLevel::WRITE,
    )
    .await;

    let grpc_request = add_token(Request::new(inner_request.clone()), USER_OIDC_TOKEN);

    let proto_collection = collection_service
        .update_collection_description(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .collection
        .unwrap();

    assert_eq!(collection.id, proto_collection.id);
    assert_eq!(&proto_collection.description, "Updated description");
}

#[tokio::test]
async fn grpc_update_collection_dataclass() {
    // Init gRPC services
    let (auth_service, project_service, collection_service, _, _, _) = init_grpc_services().await;

    // Create random Project + Collection
    let project = fast_track_grpc_project_create(&project_service, ADMIN_OIDC_TOKEN).await;
    let collection = fast_track_grpc_collection_create(
        &collection_service,
        ADMIN_OIDC_TOKEN,
        Parent::ProjectId(project.id.clone()),
    )
    .await;

    // Create user/resource ulids
    let user_ulid = DieselUlid::from_str(GENERIC_USER_ULID).unwrap();
    let collection_ulid = DieselUlid::from_str(&collection.id).unwrap();

    // Change dataclass of non-existing collection
    let mut inner_request = UpdateCollectionDataClassRequest {
        collection_id: DieselUlid::generate().to_string(),
        data_class: DataClass::Public as i32,
    };

    let grpc_request = add_token(Request::new(inner_request.clone()), ADMIN_OIDC_TOKEN);

    let response = collection_service
        .update_collection_data_class(grpc_request)
        .await;

    assert!(response.is_err());

    // Change dataclass without token
    inner_request.collection_id = collection_ulid.to_string();

    let grpc_request = Request::new(inner_request.clone());

    let response = collection_service
        .update_collection_data_class(grpc_request)
        .await;

    assert!(response.is_err());

    // Change dataclass without sufficient permissions
    let grpc_request = add_token(Request::new(inner_request.clone()), USER_OIDC_TOKEN);

    let response = collection_service
        .update_collection_data_class(grpc_request)
        .await;

    assert!(response.is_err());

    // Change to stricter dataclass with sufficient permissions
    inner_request.data_class = DataClass::Confidential as i32;

    let grpc_request = add_token(Request::new(inner_request.clone()), USER_OIDC_TOKEN);

    let response = collection_service
        .update_collection_data_class(grpc_request)
        .await;

    assert!(response.is_err());

    // Change to more relaxed dataclass with sufficient permissions
    inner_request.data_class = DataClass::Public as i32;

    fast_track_grpc_permission_add(
        &auth_service,
        ADMIN_OIDC_TOKEN,
        &user_ulid,
        &collection_ulid,
        DbPermissionLevel::WRITE,
    )
    .await;

    let grpc_request = add_token(Request::new(inner_request.clone()), USER_OIDC_TOKEN);

    let proto_collection = collection_service
        .update_collection_data_class(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .collection
        .unwrap();

    assert_eq!(collection.id, proto_collection.id);
    assert_eq!(proto_collection.data_class, DataClass::Public as i32);
}

#[tokio::test]
async fn grpc_delete_collection() {}

#[tokio::test]
async fn grpc_snapshot_collection() {}
