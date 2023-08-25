use crate::common::init::init_database_handler_middlelayer;
use crate::common::test_utils;
use aruna_rust_api::api::storage::services::v2::{
    ActivateUserRequest, DeactivateUserRequest, UpdateUserDisplayNameRequest,
    UpdateUserEmailRequest,
};
use aruna_server::database::crud::CrudDb;
use aruna_server::database::dsls::user_dsl::User;
use aruna_server::middlelayer::user_request_types::{
    ActivateUser, DeactivateUser, UpdateUserEmail, UpdateUserName,
};

// #[tokio::test]
// async fn test_register_user() {
//     let db_handler = init_handler().await;
//     let display_name = "test_name".to_string();
//     let email = "test.test@test.org".to_string();
//     let request = RegisterUser(RegisterUserRequest {
//         display_name: display_name.clone(),
//         email: email.clone(),
//         project: "".to_string(),
//     });
//     let (id, user) = db_handler.register_user(request, None).await.unwrap();
//     assert_eq!(user.id, id);
//     assert_eq!(user.email, email);
//     assert_eq!(user.display_name, display_name);
//     assert!(!user.active);
//     assert!(!user.attributes.0.global_admin);
//     assert!(!user.attributes.0.service_account);
//     assert!(user.attributes.0.tokens.is_empty());
//     assert!(user.attributes.0.trusted_endpoints.is_empty());
//     assert!(user.attributes.0.custom_attributes.is_empty());
//     assert!(user.attributes.0.permissions.is_empty());
//     let db_user = User::get(id, &db_handler.database.get_client().await.unwrap())
//         .await
//         .unwrap()
//         .unwrap();
//     assert_eq!(db_user, user);
// }
#[tokio::test]
async fn test_activate_user() {
    let db_handler = init_database_handler_middlelayer().await;
    let client = db_handler.database.get_client().await.unwrap();
    let mut user = test_utils::new_user(vec![]);
    user.create(&client).await.unwrap();
    // Test activation
    let request = ActivateUser(ActivateUserRequest {
        user_id: user.id.to_string(),
    });
    let (id, user) = db_handler.activate_user(request).await.unwrap();
    assert!(user.active);
    let db_user = User::get(id, &client).await.unwrap().unwrap();
    assert!(db_user.active);
    // test deactivation
    let request = DeactivateUser(DeactivateUserRequest {
        user_id: user.id.to_string(),
    });
    let (id, user) = db_handler.deactivate_user(request).await.unwrap();
    assert!(!user.active);
    let db_user = User::get(id, &client).await.unwrap().unwrap();
    assert!(!db_user.active);
}
#[tokio::test]
async fn test_update_display_name() {
    let db_handler = init_database_handler_middlelayer().await;
    let client = db_handler.database.get_client().await.unwrap();
    let mut user = test_utils::new_user(vec![]);
    user.create(&client).await.unwrap();
    // Test activation
    let new_display_name = "updated_name".to_string();
    let request = UpdateUserName(UpdateUserDisplayNameRequest {
        new_display_name: new_display_name.clone(),
    });
    let new = db_handler
        .update_display_name(request, user.id)
        .await
        .unwrap();
    assert_eq!(&new.display_name, &new_display_name);
    let db_user = User::get(user.id, &client).await.unwrap().unwrap();
    assert_eq!(&db_user.display_name, &new_display_name);
}
#[tokio::test]
async fn test_update_email() {
    let db_handler = init_database_handler_middlelayer().await;
    let client = db_handler.database.get_client().await.unwrap();
    let mut user = test_utils::new_user(vec![]);
    user.create(&client).await.unwrap();
    // Test activation
    let new_email = "updated@test.org".to_string();
    let request = UpdateUserEmail(UpdateUserEmailRequest {
        user_id: user.id.to_string(),
        new_email: new_email.clone(),
    });
    let new = db_handler.update_email(request, user.id).await.unwrap();
    assert_eq!(&new.email, &new_email);
    let db_user = User::get(user.id, &client).await.unwrap().unwrap();
    assert_eq!(&db_user.email, &new_email);
}
