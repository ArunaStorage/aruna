use crate::common::init::init_database_handler_middlelayer;
use crate::common::test_utils;
use crate::common::test_utils::rand_string;
use aruna_rust_api::api::storage::services::v2::{
    CreateProjectRequest, CreateRuleBindingRequest, CreateRuleRequest, DeleteRuleBindingRequest,
    UpdateRuleRequest,
};
use aruna_server::database::crud::CrudDb;
use aruna_server::database::dsls::license_dsl::ALL_RIGHTS_RESERVED;
use aruna_server::database::dsls::rule_dsl::{Rule, RuleBinding};
use aruna_server::middlelayer::create_request_types::CreateRequest;
use aruna_server::middlelayer::rule_request_types::{
    CreateRule, CreateRuleBinding, DeleteRuleBinding, UpdateRule,
};
use diesel_ulid::DieselUlid;

#[tokio::test]
async fn create_rule() {
    // init
    let db_handler = init_database_handler_middlelayer().await;

    // create user
    let mut user = test_utils::new_user(vec![]);
    user.create(&db_handler.database.get_client().await.unwrap())
        .await
        .unwrap();

    // create rule
    let request = CreateRuleRequest {
        rule: "1 == 1".to_string(),
        description: "test rule".to_string(),
        public: true,
    };
    let rule_id = db_handler
        .create_rule(CreateRule(request.clone()), user.id)
        .await
        .unwrap();

    // Get rule
    let client = db_handler.database.get_client().await.unwrap();
    let rule = Rule::get(rule_id, &client).await.unwrap().unwrap();
    assert_eq!(&request.rule, &rule.rule_expressions);
    assert_eq!(&request.description, &rule.description);
    assert_eq!(&request.public, &rule.is_public);
    assert_eq!(user.id, rule.owner_id);
}

#[tokio::test]
async fn update_rule() {
    // init
    let db_handler = init_database_handler_middlelayer().await;

    // create user
    let mut user = test_utils::new_user(vec![]);
    user.create(&db_handler.database.get_client().await.unwrap())
        .await
        .unwrap();

    // create rule
    let request = CreateRuleRequest {
        rule: "1 == 1".to_string(),
        description: "test rule".to_string(),
        public: true,
    };
    let rule_id = db_handler
        .create_rule(CreateRule(request.clone()), user.id)
        .await
        .unwrap();
    let cached_rule = db_handler.cache.get_rule(&rule_id).unwrap();

    // Update rule
    let request = UpdateRuleRequest {
        id: rule_id.to_string(),
        rule: "false".to_string(),
        description: "new".to_string(),
        public: false,
    };
    let updated = db_handler
        .update_rule(UpdateRule(request.clone()), cached_rule)
        .await
        .unwrap();

    // Get rule
    assert_eq!(&request.rule, &updated.rule.rule_expressions);
    assert_eq!(&request.description, &updated.rule.description);
    assert_eq!(&request.public, &updated.rule.is_public);
    assert_eq!(user.id, updated.rule.owner_id);
}

#[tokio::test]
async fn delete_rule() {
    // init
    let db_handler = init_database_handler_middlelayer().await;

    // create user
    let mut user = test_utils::new_user(vec![]);
    user.create(&db_handler.database.get_client().await.unwrap())
        .await
        .unwrap();

    // create rule
    let request = CreateRuleRequest {
        rule: "false".to_string(),
        description: "test rule".to_string(),
        public: true,
    };
    let rule_id = db_handler
        .create_rule(CreateRule(request.clone()), user.id)
        .await
        .unwrap();
    let cached_rule = db_handler.cache.get_rule(&rule_id).unwrap();

    // Delete rule
    db_handler.delete_rule(&cached_rule).await.unwrap();

    // Get rule
    assert!(db_handler.cache.get_rule(&rule_id).is_none());
    let client = db_handler.database.get_client().await.unwrap();
    assert!(Rule::get(rule_id, &client).await.unwrap().is_none());
}
#[tokio::test]
async fn create_and_delete_rule_binding() {
    // init
    let db_handler = init_database_handler_middlelayer().await;

    // create user
    let mut user = test_utils::new_user(vec![]);
    user.create(&db_handler.database.get_client().await.unwrap())
        .await
        .unwrap();

    // create rule
    let request = CreateRuleRequest {
        rule: "false".to_string(),
        description: "test rule".to_string(),
        public: true,
    };
    let rule_id = db_handler
        .create_rule(CreateRule(request.clone()), user.id)
        .await
        .unwrap();
    let _cached_rule = db_handler.cache.get_rule(&rule_id).unwrap();

    // create object
    let default_endpoint = DieselUlid::generate();
    let project_name = rand_string(30).to_lowercase();
    let request = CreateRequest::Project(
        CreateProjectRequest {
            name: project_name,
            title: "".to_string(),
            description: "test".to_string(),
            key_values: vec![],
            relations: vec![],
            data_class: 1,
            preferred_endpoint: "".to_string(),
            metadata_license_tag: ALL_RIGHTS_RESERVED.to_string(),
            default_data_license_tag: ALL_RIGHTS_RESERVED.to_string(),
            authors: vec![],
        },
        default_endpoint.to_string(),
    );
    let (project, _) = db_handler
        .create_resource(request, user.id, false)
        .await
        .unwrap();

    // create rule binding
    let request = CreateRuleBinding(CreateRuleBindingRequest {
        rule_id: rule_id.to_string(),
        object_id: project.object.id.to_string(),
        cascading: true,
    });
    db_handler.create_rule_binding(request).await.unwrap();

    // Check binding
    assert!(db_handler
        .cache
        .get_rule_bindings(&project.object.id)
        .unwrap()
        .contains(&RuleBinding {
            rule_id,
            origin_id: project.object.id,
            object_id: project.object.id,
            cascading: true,
        }));
    let client = db_handler.database.get_client().await.unwrap();
    assert!(RuleBinding::all(&client)
        .await
        .unwrap()
        .contains(&RuleBinding {
            rule_id,
            origin_id: project.object.id,
            object_id: project.object.id,
            cascading: true,
        }));

    // Delete binding
    let request = DeleteRuleBinding(DeleteRuleBindingRequest {
        rule_id: rule_id.to_string(),
        object_id: project.object.id.to_string(),
    });
    db_handler.delete_rule_binding(request).await.unwrap();

    // Check deletion
    assert!(db_handler
        .cache
        .get_rule_bindings(&project.object.id)
        .is_none());
    assert!(!RuleBinding::all(&client)
        .await
        .unwrap()
        .contains(&RuleBinding {
            rule_id,
            origin_id: project.object.id,
            object_id: project.object.id,
            cascading: true,
        }));
}

#[tokio::test]
async fn evaluate_rules() {
// init
    let db_handler = init_database_handler_middlelayer().await;

    // create user
    let mut user = test_utils::new_user(vec![]);
    user.create(&db_handler.database.get_client().await.unwrap())
        .await
        .unwrap();

    // create rule
    let request = CreateRuleRequest {
        rule: "false".to_string(),
        description: "test rule".to_string(),
        public: true,
    };
    let rule_id = db_handler
        .create_rule(CreateRule(request.clone()), user.id)
        .await
        .unwrap();
    let _cached_rule = db_handler.cache.get_rule(&rule_id).unwrap();

    // create object
    let default_endpoint = DieselUlid::generate();
    let project_name = rand_string(30).to_lowercase();
    let request = CreateRequest::Project(
        CreateProjectRequest {
            name: project_name,
            title: "a valid title for a project".to_string(),
            description: "test".to_string(),
            key_values: vec![],
            relations: vec![],
            data_class: 1,
            preferred_endpoint: "".to_string(),
            metadata_license_tag: ALL_RIGHTS_RESERVED.to_string(),
            default_data_license_tag: ALL_RIGHTS_RESERVED.to_string(),
            authors: vec![],
        },
        default_endpoint.to_string(),
    );
    let (project, _) = db_handler
        .create_resource(request, user.id, false)
        .await
        .unwrap();

    // create rule binding
    let request = CreateRuleBinding(CreateRuleBindingRequest {
        rule_id: rule_id.to_string(),
        object_id: project.object.id.to_string(),
        cascading: true,
    });
    db_handler.create_rule_binding(request).await.unwrap();
    todo!()
}

#[tokio::test]
async fn evaluate_and_update_rules() {
    todo!()
}
