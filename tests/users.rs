use aruna_server::database::{
    crud::CrudDb,
    dsls::user_dsl::{User, UserAttributes},
};
use diesel_ulid::DieselUlid;

mod init_db;

#[tokio::test]
async fn create_user_test() {
    // Init database connection
    let db = crate::init_db::init_db().await;
    let client = db.get_client().await.unwrap();

    // Define and create user in database
    let user = User {
        id: DieselUlid::generate(),
        display_name: "aha".to_string(),
        email: "aja".to_string(),
        attributes: postgres_types::Json(UserAttributes {
            global_admin: false,
            service_account: false,
            custom_attributes: vec![],
            permissions: vec![],
        }),
        active: true,
    };

    user.create(&client).await.unwrap();

    // Validate creation
    if let Some(created_user) = User::get(user.id, &client).await.unwrap() {
        assert_eq!(user.id, created_user.id);
        assert_eq!(user.display_name, created_user.display_name);
        assert_eq!(user.email, created_user.email);
        assert_eq!(user.attributes, created_user.attributes);
        assert_eq!(user.active, created_user.active);
    } else {
        panic!("User should exist.")
    }
}

#[tokio::test]
async fn update_user_name_test() {
    let db = crate::init_db::init_db().await;
    let client = db.get_client().await.unwrap();

    // Define and create user in database
    let user = User {
        id: DieselUlid::generate(),
        display_name: "aha".to_string(),
        email: "aja".to_string(),
        attributes: postgres_types::Json(UserAttributes {
            global_admin: false,
            service_account: true,
            permissions: vec![],
            custom_attributes: vec![],
        }),
        active: true,
    };

    user.create(&client).await.unwrap();

    // Update user display name
    User::update_display_name(&client, &user.id, "tja")
        .await
        .unwrap();

    // Validate update
    if let Some(updated_user) = User::get(user.id, &client).await.unwrap() {
        assert_eq!(user.id, updated_user.id);
        assert_eq!(updated_user.display_name, "tja");
        assert_eq!(user.email, updated_user.email);
        assert_eq!(user.attributes, updated_user.attributes);
        assert_eq!(user.active, updated_user.active);
    } else {
        panic!("Object should still exist.");
    }
}

#[tokio::test]
async fn update_user_email_test() {
    let db = crate::init_db::init_db().await;
    let client = db.get_client().await.unwrap();

    // Define and create user in database
    let user = User {
        id: DieselUlid::generate(),
        display_name: "aha".to_string(),
        email: "aja".to_string(),
        attributes: postgres_types::Json(UserAttributes {
            global_admin: false,
            service_account: true,
            permissions: vec![],
            custom_attributes: vec![],
        }),
        active: true,
    };

    user.create(&client).await.unwrap();

    // Update user email
    User::update_email(&client, &user.id, "willy_nilly")
        .await
        .unwrap();

    // Validate update
    if let Some(updated_user) = User::get(user.id, &client).await.unwrap() {
        assert_eq!(user.id, updated_user.id);
        assert_eq!(user.display_name, updated_user.display_name);
        assert_eq!(updated_user.email, "willy_nilly");
        assert_eq!(user.attributes, updated_user.attributes);
        assert_eq!(user.active, updated_user.active);
    } else {
        panic!("Object should still exist.");
    }
}

#[tokio::test]
async fn update_user_admin_test() {
    let db = crate::init_db::init_db().await;
    let client = db.get_client().await.unwrap();

    // Define and create user in database
    let user = User {
        id: DieselUlid::generate(),
        display_name: "aha".to_string(),
        email: "aja".to_string(),
        attributes: postgres_types::Json(UserAttributes {
            global_admin: false,
            service_account: true,
            permissions: vec![],
            custom_attributes: vec![],
        }),
        active: true,
    };

    user.create(&client).await.unwrap();

    // Update user email
    User::set_user_global_admin(&client, &user.id, true)
        .await
        .unwrap();

    // Validate update
    if let Some(updated_user) = User::get(user.id, &client).await.unwrap() {
        assert_eq!(user.id, updated_user.id);
        assert_eq!(user.display_name, updated_user.display_name);
        assert_eq!(user.email, updated_user.email);
        assert!(updated_user.attributes.0.global_admin);
        assert_eq!(user.active, updated_user.active);
    } else {
        panic!("Object should still exist.");
    }
}

#[tokio::test]
async fn update_user_service_account_test() {
    let db = crate::init_db::init_db().await;
    let client = db.get_client().await.unwrap();

    // Define and create user in database
    let user = User {
        id: DieselUlid::generate(),
        display_name: "aha".to_string(),
        email: "aja".to_string(),
        attributes: postgres_types::Json(UserAttributes {
            global_admin: false,
            service_account: true,
            permissions: vec![],
            custom_attributes: vec![],
        }),
        active: true,
    };

    user.create(&client).await.unwrap();

    // Update user email
    User::set_user_service_account(&client, &user.id, true)
        .await
        .unwrap();

    // Validate update
    if let Some(updated_user) = User::get(user.id, &client).await.unwrap() {
        assert_eq!(user.id, updated_user.id);
        assert_eq!(user.display_name, updated_user.display_name);
        assert_eq!(user.email, updated_user.email);
        assert!(updated_user.attributes.0.service_account);
        assert_eq!(user.active, updated_user.active);
    } else {
        panic!("Object should still exist.");
    }
}

#[tokio::test]
async fn delete_user_test() {
    let db = crate::init_db::init_db().await;
    let client = db.get_client().await.unwrap();

    // Define and create user in database
    let user = User {
        id: DieselUlid::generate(),
        display_name: "aha".to_string(),
        email: "aja".to_string(),
        attributes: postgres_types::Json(UserAttributes {
            global_admin: false,
            service_account: true,
            permissions: vec![],
            custom_attributes: vec![],
        }),
        active: true,
    };

    user.create(&client).await.unwrap();

    // Delete user
    user.delete(&client).await.unwrap();

    // Validate update
    if User::get(user.id, &client).await.unwrap().is_some() {
        panic!("User should not exist anymore")
    }
}
