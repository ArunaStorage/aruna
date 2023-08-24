use aruna_rust_api::api::storage::{
    models::v2::{DataEndpoint, Status},
    services::v2::{project_service_server::ProjectService, CreateProjectRequest},
};
use aruna_server::grpc::projects::ProjectServiceImpl;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use tonic::Request;

use crate::common::{
    init::{
        init_cache, init_database, init_database_handler, init_nats_client,
        init_permission_handler, init_search_client,
    },
    test_utils::{self, add_token, DEFAULT_ENDPOINT_ULID},
};

#[tokio::test]
async fn grpc_create_project() {
    let db = init_database().await;
    let cache = init_cache(db.clone(), true).await;
    let natsio_handler = init_nats_client().await;
    let db_handler = init_database_handler(db.clone(), natsio_handler).await;
    let search_client = init_search_client().await;
    let authorizer = init_permission_handler(db.clone(), cache.clone()).await;

    // Init project service
    let project_service = ProjectServiceImpl::new(
        db_handler,
        authorizer,
        cache,
        search_client,
        test_utils::DEFAULT_ENDPOINT_ULID.to_string(),
    )
    .await;

    // Create request with OIDC token in header
    let project_name = rand_string(32);

    let create_request = CreateProjectRequest {
        name: project_name.to_string(),
        description: "something".to_string(),
        key_values: vec![],
        external_relations: vec![],
        data_class: 1,
        preferred_endpoint: "".to_string(),
    };

    
    let grpc_request = add_token(Request::new(create_request), test_utils::ADMIN_OIDC_TOKEN);

    // Create project via gRPC service
    let create_response = project_service
        .create_project(grpc_request)
        .await
        .unwrap()
        .into_inner();

    let proto_project = create_response.project.unwrap();

    assert!(!proto_project.id.is_empty());
    assert_eq!(proto_project.name, project_name);
    assert_eq!(&proto_project.description, "something");
    assert_eq!(proto_project.key_values, vec![]);
    assert_eq!(proto_project.relations, vec![]);
    assert_eq!(proto_project.data_class, 1);
    assert_eq!(proto_project.relations, vec![]);
    assert_eq!(proto_project.status, Status::Available as i32);
    assert_eq!(proto_project.dynamic, true);
    assert_eq!(
        proto_project.endpoints,
        vec![DataEndpoint {
            id: DEFAULT_ENDPOINT_ULID.to_string(),
            full_synced: true,
        }]
    );
}
