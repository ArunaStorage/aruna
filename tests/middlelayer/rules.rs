use aruna_rust_api::api::storage::services::v2::{CreateRuleRequest, UpdateRuleRequest};
use aruna_server::database::crud::CrudDb;
use aruna_server::database::dsls::rule_dsl::Rule;
use aruna_server::middlelayer::rule_request_types::{CreateRule, UpdateRule};
use crate::common::init::init_database_handler_middlelayer;
use crate::common::test_utils;

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
    let request = CreateRuleRequest{
        rule: "1 == 1".to_string(),
        description: "test rule".to_string(),
        public: true,
    };
    let rule_id = db_handler.create_rule(CreateRule(request.clone()), user.id).await.unwrap();
    
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
    let request = CreateRuleRequest{
        rule: "1 == 1".to_string(),
        description: "test rule".to_string(),
        public: true,
    };
    let rule_id = db_handler.create_rule(CreateRule(request.clone()), user.id).await.unwrap();
    let cached_rule = db_handler.cache.get_rule(&rule_id).unwrap();
    
    // Update rule
    let request = UpdateRuleRequest {
        id: rule_id.to_string(),
        rule: "false".to_string(),
        description: "new".to_string(),
        public: false,
    };
    let updated = db_handler.update_rule(UpdateRule(request.clone()), cached_rule).await.unwrap();

    // Get rule
    assert_eq!(&request.rule, &updated.rule.rule_expressions);
    assert_eq!(&request.description, &updated.rule.description);
    assert_eq!(&request.public, &updated.rule.is_public);
    assert_eq!(user.id, updated.rule.owner_id);
}