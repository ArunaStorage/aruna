use aruna_rust_api::api::storage::{
    models::v2::{DataClass, DataEndpoint, Status},
    services::v2::{
        project_service_server::ProjectService, CreateProjectRequest, GetProjectRequest,
        GetProjectsRequest,
    },
};
use tonic::Request;

use crate::common::{
    init::init_project_service,
    test_utils::{
        self, add_token, fast_track_grpc_project_create, rand_string, DEFAULT_ENDPOINT_ULID,
        USER_OIDC_TOKEN,
    },
};

#[tokio::test]
async fn grpc_create_project() {
    // Init ProjectService
    let project_service = init_project_service().await;

    // Create request with OIDC token in header
    let project_name = rand_string(32);

    let create_request = CreateProjectRequest {
        name: project_name.to_string(),
        description: "Something".to_string(),
        key_values: vec![],
        external_relations: vec![],
        data_class: DataClass::Public as i32,
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
    assert_eq!(&proto_project.description, "Something");
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

#[tokio::test]
async fn grpc_get_projects() {
    // Init ProjectService
    let project_service = init_project_service().await;

    // Create multiple projects
    let project_01 = fast_track_grpc_project_create(&project_service, USER_OIDC_TOKEN).await;
    let project_02 = fast_track_grpc_project_create(&project_service, USER_OIDC_TOKEN).await;

    // Fetch single project
    let get_request = GetProjectRequest {
        project_id: project_01.id.to_string(),
    };

    let grpc_request = add_token(Request::new(get_request), USER_OIDC_TOKEN);

    let get_response = project_service
        .get_project(grpc_request)
        .await
        .unwrap()
        .into_inner();

    let proto_project = get_response.project.unwrap();

    assert_eq!(project_01, proto_project);

    // Fetch all projects
    let get_all_request = GetProjectsRequest {
        project_ids: vec![project_01.id.to_string(), project_02.id.to_string()],
    };

    let grpc_request = add_token(Request::new(get_all_request), USER_OIDC_TOKEN);

    let get_all_response = project_service
        .get_projects(grpc_request)
        .await
        .unwrap()
        .into_inner();

    let proto_projects = get_all_response.projects;

    for project in vec![project_01, project_02] {
        assert!(proto_projects.contains(&project))
    }
}
