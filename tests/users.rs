use aruna_server::database::enums::ObjectType;
use aruna_server::database::{
    crud::CrudDb,
    dsls::user_dsl::{APIToken, User, UserAttributes},
};
use dashmap::DashMap;
//use deadpool_postgres::GenericClient;
use diesel_ulid::DieselUlid;

mod init_db;

#[tokio::test]
async fn create_user_test() {
    // Init database connection
    let db = init_db::init_db().await;
    let client = db.get_client().await.unwrap();

    // Define and create user in database
    let user = User {
        id: DieselUlid::generate(),
        display_name: "aha".to_string(),
        external_id: None,
        email: "aja".to_string(),
        attributes: postgres_types::Json(UserAttributes {
            global_admin: false,
            service_account: false,
            trusted_endpoints: DashMap::default(),
            tokens: DashMap::default(),
            custom_attributes: vec![],
            permissions: DashMap::default(),
        }),
        active: true,
    };

    user.create(&client).await.unwrap();

    // Validate creation
    if let Some(created_user) = User::get(user.id, &client).await.unwrap() {
        assert_eq!(user.id, created_user.id);
        assert_eq!(user.display_name, created_user.display_name);
        assert_eq!(user.email, created_user.email);
        assert_eq!(user.attributes.0, created_user.attributes.0);
        assert_eq!(user.active, created_user.active);
    } else {
        panic!("User should exist.")
    }
}

#[tokio::test]
async fn update_user_name_test() {
    let db = init_db::init_db().await;
    let client = db.get_client().await.unwrap();

    // Define and create user in database
    let user = User {
        id: DieselUlid::generate(),
        display_name: "aha".to_string(),
        external_id: None,
        email: "aja".to_string(),
        attributes: postgres_types::Json(UserAttributes {
            global_admin: false,
            service_account: true,
            trusted_endpoints: DashMap::default(),
            tokens: DashMap::default(),
            permissions: DashMap::default(),
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
    let db = init_db::init_db().await;
    let client = db.get_client().await.unwrap();

    // Define and create user in database
    let user = User {
        id: DieselUlid::generate(),
        display_name: "aha".to_string(),
        external_id: None,
        email: "aja".to_string(),
        attributes: postgres_types::Json(UserAttributes {
            global_admin: false,
            service_account: true,
            trusted_endpoints: DashMap::default(),
            tokens: DashMap::default(),
            permissions: DashMap::default(),
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
    let db = init_db::init_db().await;
    let client = db.get_client().await.unwrap();

    // Define and create user in database
    let user = User {
        id: DieselUlid::generate(),
        display_name: "aha".to_string(),
        external_id: None,
        email: "aja".to_string(),
        attributes: postgres_types::Json(UserAttributes {
            global_admin: false,
            service_account: true,
            trusted_endpoints: DashMap::default(),
            tokens: DashMap::default(),
            permissions: DashMap::default(),
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
    let db = init_db::init_db().await;
    let client = db.get_client().await.unwrap();

    // Define and create user in database
    let user = User {
        id: DieselUlid::generate(),
        display_name: "aha".to_string(),
        external_id: None,
        email: "aja".to_string(),
        attributes: postgres_types::Json(UserAttributes {
            global_admin: false,
            service_account: true,
            trusted_endpoints: DashMap::default(),
            tokens: DashMap::default(),
            permissions: DashMap::default(),
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
    let db = init_db::init_db().await;
    let client = db.get_client().await.unwrap();

    // Define and create user in database
    let user = User {
        id: DieselUlid::generate(),
        display_name: "aha".to_string(),
        external_id: None,
        email: "aja".to_string(),
        attributes: postgres_types::Json(UserAttributes {
            global_admin: false,
            service_account: true,
            trusted_endpoints: DashMap::default(),
            tokens: DashMap::default(),
            permissions: DashMap::default(),
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

#[tokio::test]
async fn add_permission_user_test() {
    let db = init_db::init_db().await;
    let client = db.get_client().await.unwrap();

    // Define and create user in database
    let user = User {
        id: DieselUlid::generate(),
        display_name: "aha".to_string(),
        external_id: None,
        email: "aja".to_string(),
        attributes: postgres_types::Json(UserAttributes {
            global_admin: false,
            service_account: true,
            trusted_endpoints: DashMap::default(),
            tokens: DashMap::default(),
            permissions: DashMap::default(),
            custom_attributes: vec![],
        }),
        active: true,
    };

    user.create(&client).await.unwrap();

    User::add_user_permission(
        &client,
        &user.id,
        [(
            DieselUlid::generate(),
            aruna_server::database::enums::DbPermissionLevel::ADMIN,
        )]
        .into_iter()
        .collect(),
    )
    .await
    .unwrap();

    User::add_user_permission(
        &client,
        &user.id,
        [(
            DieselUlid::generate(),
            aruna_server::database::enums::DbPermissionLevel::READ,
        )]
        .into_iter()
        .collect(),
    )
    .await
    .unwrap();

    assert_eq!(
        User::get(user.id, &client)
            .await
            .unwrap()
            .unwrap()
            .attributes
            .0
            .permissions
            .len(),
        2
    );
}

#[tokio::test]
async fn remove_user_permission_test() {
    let db = init_db::init_db().await;
    let client = db.get_client().await.unwrap();

    let perm1 = DieselUlid::generate();
    let perm2 = DieselUlid::generate();
    let perm3 = DieselUlid::generate();
    // Define and create user in database
    let user = User {
        id: DieselUlid::generate(),
        display_name: "aha".to_string(),
        external_id: None,
        email: "aja".to_string(),
        attributes: postgres_types::Json(UserAttributes {
            global_admin: false,
            service_account: true,
            trusted_endpoints: DashMap::default(),
            tokens: DashMap::default(),
            permissions: DashMap::from_iter([
                (
                    perm1,
                    aruna_server::database::enums::DbPermissionLevel::ADMIN,
                ),
                (
                    perm2,
                    aruna_server::database::enums::DbPermissionLevel::READ,
                ),
                (
                    perm3,
                    aruna_server::database::enums::DbPermissionLevel::WRITE,
                ),
            ]),
            custom_attributes: vec![],
        }),
        active: true,
    };

    user.create(&client).await.unwrap();

    assert_eq!(
        User::get(user.id, &client)
            .await
            .unwrap()
            .unwrap()
            .attributes
            .0
            .permissions
            .len(),
        3
    );

    User::remove_user_permission(&client, &user.id, &perm2)
        .await
        .unwrap();

    assert_eq!(
        User::get(user.id, &client)
            .await
            .unwrap()
            .unwrap()
            .attributes
            .0
            .permissions
            .len(),
        2
    );

    User::remove_user_permission(&client, &user.id, &perm1)
        .await
        .unwrap();

    assert_eq!(
        User::get(user.id, &client)
            .await
            .unwrap()
            .unwrap()
            .attributes
            .0
            .permissions
            .len(),
        1
    );

    User::remove_user_permission(&client, &user.id, &perm3)
        .await
        .unwrap();

    assert_eq!(
        User::get(user.id, &client)
            .await
            .unwrap()
            .unwrap()
            .attributes
            .0
            .permissions
            .len(),
        0
    );
}

#[tokio::test]
async fn user_token_test() {
    let db = init_db::init_db().await;
    let client = db.get_client().await.unwrap();

    let perm1 = DieselUlid::generate();
    let perm2 = DieselUlid::generate();
    let perm3 = DieselUlid::generate();
    // Define and create user in database
    let user = User {
        id: DieselUlid::generate(),
        display_name: "aha".to_string(),
        external_id: None,
        email: "aja".to_string(),
        attributes: postgres_types::Json(UserAttributes {
            global_admin: false,
            service_account: true,
            permissions: DashMap::default(),
            trusted_endpoints: DashMap::default(),
            tokens: [
                (
                    perm1,
                    APIToken {
                        pub_key: 1,
                        name: "test".to_string(),
                        created_at: chrono::Utc::now().naive_utc(),
                        expires_at: chrono::Utc::now().naive_utc(),
                        object_id: DieselUlid::generate(),
                        object_type: ObjectType::COLLECTION,
                        user_rights: aruna_server::database::enums::DbPermissionLevel::ADMIN,
                    },
                ),
                (
                    perm2,
                    APIToken {
                        pub_key: 1,
                        name: "test".to_string(),
                        created_at: chrono::Utc::now().naive_utc(),
                        expires_at: chrono::Utc::now().naive_utc(),
                        object_id: DieselUlid::generate(),
                        object_type: ObjectType::OBJECT,
                        user_rights: aruna_server::database::enums::DbPermissionLevel::ADMIN,
                    },
                ),
                (
                    perm3,
                    APIToken {
                        pub_key: 1,
                        name: "test".to_string(),
                        created_at: chrono::Utc::now().naive_utc(),
                        expires_at: chrono::Utc::now().naive_utc(),
                        object_id: DieselUlid::generate(),
                        object_type: ObjectType::DATASET,
                        user_rights: aruna_server::database::enums::DbPermissionLevel::ADMIN,
                    },
                ),
            ]
            .into_iter()
            .collect(),
            custom_attributes: vec![],
        }),
        active: true,
    };

    user.create(&client).await.unwrap();

    assert_eq!(
        User::get(user.id, &client)
            .await
            .unwrap()
            .unwrap()
            .attributes
            .0
            .tokens
            .len(),
        3
    );

    User::remove_user_token(&client, &user.id, &perm2)
        .await
        .unwrap();

    assert_eq!(
        User::get(user.id, &client)
            .await
            .unwrap()
            .unwrap()
            .attributes
            .0
            .tokens
            .len(),
        2
    );

    User::remove_user_token(&client, &user.id, &perm1)
        .await
        .unwrap();

    assert_eq!(
        User::get(user.id, &client)
            .await
            .unwrap()
            .unwrap()
            .attributes
            .0
            .tokens
            .len(),
        1
    );

    User::remove_user_token(&client, &user.id, &perm3)
        .await
        .unwrap();

    assert_eq!(
        User::get(user.id, &client)
            .await
            .unwrap()
            .unwrap()
            .attributes
            .0
            .tokens
            .len(),
        0
    );
}

#[tokio::test]
async fn user_status_test() {
    let db = init_db::init_db().await;
    let mut client = db.get_client().await.unwrap();
    let transaction = client.transaction().await.unwrap();
    let client = transaction.client();

    let id = DieselUlid::generate();

    let user = User {
        id,
        display_name: "aha".to_string(),
        external_id: None,
        email: "aja".to_string(),
        attributes: postgres_types::Json(UserAttributes {
            global_admin: false,
            service_account: true,
            permissions: DashMap::default(),
            trusted_endpoints: DashMap::default(),
            tokens: DashMap::default(),
            custom_attributes: Vec::new(),
        }),
        active: false,
    };
    user.create(client).await.unwrap();

    User::activate_user(client, &id).await.unwrap();
    assert!(User::get(id, client).await.unwrap().unwrap().active);

    User::deactivate_user(client, &id).await.unwrap();
    assert!(!User::get(id, client).await.unwrap().unwrap().active);
    transaction.commit().await.unwrap();
}
