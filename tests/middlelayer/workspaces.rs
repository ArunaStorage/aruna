use crate::common::{
    endpoint_mock,
    init::{init_database_handler_middlelayer, init_permission_handler, init_token_handler},
    test_utils::{self, rand_string},
};
use aruna_rust_api::api::storage::{
    models::v2::EndpointHostConfig,
    services::v2::{CreateEndpointRequest, CreateWorkspaceRequest, CreateWorkspaceTemplateRequest},
};
use aruna_server::{
    auth::structs::Context,
    database::{
        crud::CrudDb,
        dsls::{
            hook_dsl::{Hook, HookVariant, InternalHook},
            object_dsl::Object,
        },
        enums::DataClass,
    },
    middlelayer::{
        endpoints_request_types::CreateEP,
        workspace_request_types::{CreateTemplate, CreateWorkspace},
    },
};
use diesel_ulid::DieselUlid;
use postgres_types::Json;
use std::{net::SocketAddr, str::FromStr};

#[tokio::test]
async fn create_template() {
    // Init
    let db_handler = init_database_handler_middlelayer().await;
    let mut user = test_utils::new_user(vec![]);
    let client = db_handler.database.get_client().await.unwrap();
    user.create(&client).await.unwrap();

    // Create template without specified endpoints and hooks
    let request = CreateTemplate(CreateWorkspaceTemplateRequest {
        owner_id: user.id.to_string(),
        prefix: "abc".to_string(),
        name: "middlelayer_template_test".to_string(),
        hook_ids: vec![],
        description: "some_desc".to_string(),
        endpoint_id: vec![],
    });
    let workspace_id = db_handler
        .create_workspace_template(request, user.id)
        .await
        .unwrap();
    db_handler.get_ws_template(&workspace_id).await.unwrap();
    // Delete template
    db_handler
        .delete_workspace_template(workspace_id.to_string(), &user.id)
        .await
        .unwrap();
    db_handler.get_ws_template(&workspace_id).await.unwrap_err();
}
#[tokio::test]
async fn get_templates() {
    // Init
    let db_handler = init_database_handler_middlelayer().await;
    let mut user = test_utils::new_user(vec![]);
    let client = db_handler.database.get_client().await.unwrap();
    user.create(&client).await.unwrap();

    // Create ws templates
    let mut ids = Vec::new();
    for _ in 1..10 {
        let temp = CreateTemplate(CreateWorkspaceTemplateRequest {
            owner_id: user.id.to_string(),
            prefix: "abc".to_string(),
            name: rand_string(10),
            hook_ids: vec![],
            description: rand_string(10),
            endpoint_id: vec![],
        });
        ids.push(
            db_handler
                .create_workspace_template(temp, user.id)
                .await
                .unwrap(),
        );
    }
    // list owned
    let temps = db_handler.get_owned_ws(&user.id).await.unwrap();
    assert_eq!(temps.len(), ids.len());
    assert!(temps.iter().all(|t| t.owner == user.id));
}
#[tokio::test]
async fn create_workspace() {
    // Init
    let db_handler = init_database_handler_middlelayer().await;
    let token_handler =
        init_token_handler(db_handler.database.clone(), db_handler.cache.clone()).await;
    let authorizer = init_permission_handler(db_handler.cache.clone(), token_handler).await;
    let mut user = test_utils::new_user(vec![]);
    let client = db_handler.database.get_client().await.unwrap();
    user.create(&client).await.unwrap();

    // TODO: Create & mock endpoints
    // -> Default endpoint
    let default_endpoint = "01H81W0ZMB54YEP5711Q2BK46V".to_string();
    endpoint_mock::start_server("0.0.0.0:50052".parse::<SocketAddr>().unwrap())
        .await
        .unwrap();
    // -> Custom endpoint
    let request = CreateEP(CreateEndpointRequest {
        name: "workspace_test".to_string(),
        ep_variant: 1,
        is_public: true,
        pubkey: "MCowBQYDK2VwAyEAWBBLB9+sOZ4pSjM7U3DCSoq5R4xQYG4W27iwI1QoMN0=".to_string(),
        host_configs: vec![EndpointHostConfig {
            url: "http://localhost:50098".to_string(),
            is_primary: true,
            ssl: false,
            public: true,
            host_variant: 1,
        }],
    });
    let (ep, _pk) = db_handler.create_endpoint(request).await.unwrap();
    endpoint_mock::start_server("0.0.0.0:50098".parse::<SocketAddr>().unwrap())
        .await
        .unwrap();

    // Create hooks
    let mut hook = Hook {
        id: DieselUlid::generate(),
        name: "test".to_string(),
        description: "".to_string(),
        project_ids: vec![],
        owner: user.id,
        trigger_type: aruna_server::database::dsls::hook_dsl::TriggerType::OBJECT_CREATED,
        trigger_key: "some_key".to_string(),
        trigger_value: "some_value".to_string(),
        timeout: chrono::Utc::now()
            .naive_utc()
            .checked_add_days(chrono::Days::new(1))
            .unwrap(),
        hook: Json(HookVariant::Internal(InternalHook::AddLabel {
            key: "test".to_string(),
            value: "succeeded".to_string(),
        })),
    };
    hook.create(&client).await.unwrap();
    // Create templates
    // -> Invalid because hook invalid
    let invalid = CreateTemplate(CreateWorkspaceTemplateRequest {
        owner_id: user.id.to_string(),
        prefix: "abc".to_string(),
        name: rand_string(10),
        hook_ids: vec![DieselUlid::generate().to_string()],
        description: rand_string(10),
        endpoint_id: vec![],
    });
    // -> Template with default endpoint
    let default_endpoint_template = CreateTemplate(CreateWorkspaceTemplateRequest {
        owner_id: user.id.to_string(),
        prefix: "abc".to_string(),
        name: rand_string(10),
        hook_ids: vec![],
        description: rand_string(10),
        endpoint_id: vec![],
    });
    // -> Template with custom endpoint
    let custom_endpoint_template = CreateTemplate(CreateWorkspaceTemplateRequest {
        owner_id: user.id.to_string(),
        prefix: "abc".to_string(),
        name: rand_string(10),
        hook_ids: vec![],
        description: rand_string(10),
        endpoint_id: vec![ep.id.to_string()],
    });
    assert!(db_handler
        .create_workspace_template(invalid, user.id)
        .await
        .is_err());
    let default_template_id = db_handler
        .create_workspace_template(default_endpoint_template, user.id)
        .await
        .unwrap();
    let custom_template_id = db_handler
        .create_workspace_template(custom_endpoint_template, user.id)
        .await
        .unwrap();

    // Create workspace instances
    let default_workspace = CreateWorkspace(CreateWorkspaceRequest {
        workspace_template: default_template_id.to_string(),
        description: "This is a workspace with default endpoint config".to_string(),
    });
    let custom_workspace = CreateWorkspace(CreateWorkspaceRequest {
        workspace_template: custom_template_id.to_string(),
        description: "This is a workspace with custom endpoint config".to_string(),
    });
    let (default_instance_id, .., token_one) = db_handler
        .create_workspace(
            authorizer.clone(),
            default_workspace,
            default_endpoint.clone(),
        )
        .await
        .unwrap();
    let (custom_instance_id, .., token_two) = db_handler
        .create_workspace(
            authorizer.clone(),
            custom_workspace,
            default_endpoint.clone(),
        )
        .await
        .unwrap();
    let default_instance = Object::get(default_instance_id, &client)
        .await
        .unwrap()
        .unwrap();
    let custom_instance = Object::get(custom_instance_id, &client)
        .await
        .unwrap()
        .unwrap();
    assert!(default_instance
        .endpoints
        .0
        .contains_key(&DieselUlid::from_str(&default_endpoint).unwrap()));
    assert!(custom_instance.endpoints.0.contains_key(&ep.id));
    assert_eq!(default_instance.data_class, DataClass::WORKSPACE);
    assert_eq!(custom_instance.data_class, DataClass::WORKSPACE);
    authorizer
        .check_permissions(
            &token_one,
            vec![Context::res_ctx(
                default_instance_id,
                aruna_server::database::enums::DbPermissionLevel::APPEND,
                true,
            )],
        )
        .await
        .unwrap();
    authorizer
        .check_permissions(
            &token_two,
            vec![Context::res_ctx(
                default_instance_id,
                aruna_server::database::enums::DbPermissionLevel::APPEND,
                true,
            )],
        )
        .await
        .unwrap_err();
    authorizer
        .check_permissions(
            &token_two,
            vec![Context::res_ctx(
                custom_instance_id,
                aruna_server::database::enums::DbPermissionLevel::APPEND,
                true,
            )],
        )
        .await
        .unwrap();
    authorizer
        .check_permissions(
            &token_one,
            vec![Context::res_ctx(
                custom_instance_id,
                aruna_server::database::enums::DbPermissionLevel::APPEND,
                true,
            )],
        )
        .await
        .unwrap_err();
}
