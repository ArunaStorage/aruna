pub mod common;
use aruna_server::database::dsls::user_dsl::APIToken;

#[tokio::test]
async fn server_authorization() {
    // Init
    let db_handler = common::init::init_database().await;
    let cache = common::init::init_cache(db_handler.clone(), true).await;
    let token_handler = common::init::init_token_handler(db_handler.clone(), cache.clone()).await;
    let permission_handler =
        common::init::init_permission_handler(cache.clone(), token_handler.clone()).await;

    // TODO:
    // - Token testing
    let token = APIToken {
        pub_key: 1,
        name: "this_is_a_name".to_string(),
        created_at: None,
        expires_at: None,
        object_id: None,
        user_rights: None,
    };
    // - Context testing
    // - Permission testing
}
