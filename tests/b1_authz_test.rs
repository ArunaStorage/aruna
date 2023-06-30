use std::str::FromStr;

use aruna_rust_api::api::storage::{
    models::v1::ProjectPermission,
    services::v1::{
        ActivateUserRequest, AddUserToProjectRequest, CreateApiTokenRequest,
        CreateNewCollectionRequest, CreateProjectRequest, DeleteApiTokenRequest,
        DeleteApiTokensRequest, ExpiresAt, GetApiTokenRequest, GetApiTokensRequest,
        GetNotActivatedUsersRequest, GetUserProjectsRequest, RegisterUserRequest,
        UpdateUserDisplayNameRequest,
    },
};

use aruna_server::{
    database::{self},
    server::services::authz::Context,
};
use serial_test::serial;
mod common;

#[test]
#[ignore]
#[serial(db)]
fn get_or_add_pubkey_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Insert new element -> Create new serial number
    let result = db
        .get_or_add_pub_key(
            "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAGBO4KKuag6RMkOG0b1Hlt9oH/R0leUioCSS7Hm61GR8=\n-----END PUBLIC KEY-----\n".to_string(), None
        )
        .unwrap();
    // Insert a second "pubkey"
    let _result_2 = db
        .get_or_add_pub_key(
            "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAQRcVuLEdJcrsduL4hU0PtpNPubYVIgx8kZVV/Elv9dI=\n-----END PUBLIC KEY-----\n".to_string(), None
        )
        .unwrap();
    // Try to insert the first serial again -> should be the same as result
    let result_3 = db
        .get_or_add_pub_key(
            "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAGBO4KKuag6RMkOG0b1Hlt9oH/R0leUioCSS7Hm61GR8=\n-----END PUBLIC KEY-----\n".to_string(), None
        )
        .unwrap();
    assert_eq!(result, result_3);
}

#[test]
#[ignore]
#[serial(db)]
fn get_pub_keys_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Insert new element -> Create new serial number
    let result = db.get_pub_keys().unwrap();

    // Iterate through keys
    for key in result {
        // Expect it to be either "pubkey_test_1" or "pub_key_test_2"
        if key.pubkey == *"-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAGBO4KKuag6RMkOG0b1Hlt9oH/R0leUioCSS7Hm61GR8=\n-----END PUBLIC KEY-----\n"
            || key.pubkey == *"-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAQRcVuLEdJcrsduL4hU0PtpNPubYVIgx8kZVV/Elv9dI=\n-----END PUBLIC KEY-----\n"
            || key.pubkey == *"-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAnl3AKP1/g4qfy4UZH+MRxJC/C/mAuVVxwN+2zU99g54=\n-----END PUBLIC KEY-----\n"
        {
            continue;
            // Panic otherwise -> unknown pubkey in db
        } else {
            panic!(
                "Expected -----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAGBO4KKuag6RMkOG0b1Hlt9oH/R0leUioCSS7Hm61GR8=\n-----END PUBLIC KEY-----\n or -----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAQRcVuLEdJcrsduL4hU0PtpNPubYVIgx8kZVV/Elv9dI=\n-----END PUBLIC KEY-----\n, got: {:?}",
                key.pubkey
            );
        }
    }
}

#[test]
#[ignore]
#[serial(db)]
fn get_oidc_user_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Get admin user via (fake) oidc id
    let user_id = db
        .get_oidc_user("df5b0209-60e0-4a3b-806d-bbfc99d9e152")
        .unwrap()
        .unwrap();

    // Expect the user to have the following uuid
    let parsed_uid = common::functions::get_admin_user_ulid();
    assert_eq!(user_id, parsed_uid)
}

#[test]
#[ignore]
#[serial(db)]
fn register_user_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Build request for new user
    let req = RegisterUserRequest {
        display_name: "test_user_1".to_string(),
        email: "e@mail.dev".to_string(),
        project: String::new(),
    };
    // Create new user
    let _resp = db
        .register_user(req, "test_user_1_oidc".to_string())
        .unwrap();
}

#[test]
#[ignore]
#[serial(db)]
fn activate_user_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Add another user
    // Build request for new user
    let req_2 = RegisterUserRequest {
        display_name: "test_user_2".to_string(),
        email: "e@mail.dev".to_string(),
        project: String::new(),
    };
    // Create new user
    let resp_2 = db
        .register_user(req_2, "test_user_2_oidc".to_string())
        .unwrap();

    // Build request for new user
    let req = ActivateUserRequest {
        user_id: resp_2.user_id,
        project_perms: None,
    };

    db.activate_user(req).unwrap();
}

#[test]
#[ignore]
#[serial(db)]
fn create_api_token_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Create new user
    let user_req = RegisterUserRequest {
        display_name: "test_user_3".to_string(),
        email: "e@mail.dev".to_string(),
        project: String::new(),
    };
    let user_resp = db
        .register_user(user_req, "test_user_3_oidc".to_string())
        .unwrap();
    let user_id = diesel_ulid::DieselUlid::from_str(&user_resp.user_id).unwrap();

    // Activate the user
    let req = ActivateUserRequest {
        user_id: user_resp.user_id,
        project_perms: None,
    };
    db.activate_user(req).unwrap();

    // Add fresh pubkey
    let pubkey_result = db
        .get_or_add_pub_key(
            "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAGBO4KKuag6RMkOG0b1Hlt9oH/R0leUioCSS7Hm61GR8=\n-----END PUBLIC KEY-----\n".to_string(), None
        )
        .unwrap();

    // Create personal token for the user
    let req = CreateApiTokenRequest {
        project_id: String::new(),
        collection_id: String::new(),
        name: "personal_u1_token".to_string(),
        expires_at: None,
        permission: 1,
        is_session: false,
    };
    let _token = db.create_api_token(req, user_id, pubkey_result).unwrap();

    // Create personal token with timestamp
    let req = CreateApiTokenRequest {
        project_id: String::new(),
        collection_id: String::new(),
        name: "personal_u1_token".to_string(),
        expires_at: Some(ExpiresAt {
            timestamp: Some(prost_types::Timestamp::default()),
        }),
        permission: 1,
        is_session: false,
    };
    let _token = db.create_api_token(req, user_id, pubkey_result).unwrap();

    // Create token with error
    let req = CreateApiTokenRequest {
        project_id: diesel_ulid::DieselUlid::generate().to_string(),
        collection_id: diesel_ulid::DieselUlid::generate().to_string(),
        name: "broken_token".to_string(),
        expires_at: None,
        permission: 1,
        is_session: false,
    };
    let res = db.create_api_token(req, user_id, pubkey_result);

    assert!(res.is_err())
}

#[test]
#[ignore]
#[serial(db)]
fn get_api_token_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    let col = common::functions::create_collection(common::functions::TCreateCollection {
        project_id: common::functions::get_regular_project_ulid().to_string(),
        ..Default::default()
    });

    // Create new user
    let user_req = RegisterUserRequest {
        display_name: "test_user_4".to_string(),
        email: "e@mail.dev".to_string(),
        project: String::new(),
    };
    let user_resp = db
        .register_user(user_req, "test_user_4_oidc".to_string())
        .unwrap();
    let user_id = diesel_ulid::DieselUlid::from_str(&user_resp.user_id).unwrap();

    // Activate the user
    let req = ActivateUserRequest {
        user_id: user_resp.user_id,
        project_perms: None,
    };
    db.activate_user(req).unwrap();

    // Add fresh pubkey
    let pubkey_result = db
        .get_or_add_pub_key(
            "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAnl3AKP1/g4qfy4UZH+MRxJC/C/mAuVVxwN+2zU99g54=\n-----END PUBLIC KEY-----\n".to_string(), None
        )
        .unwrap();

    // Create personal token for the user
    let req = CreateApiTokenRequest {
        project_id: String::new(),
        collection_id: String::new(),
        name: "personal_u2_token".to_string(),
        expires_at: None,
        permission: 1,
        is_session: false,
    };
    // Create a initial token
    let (initial_token, _, _) = db.create_api_token(req, user_id, pubkey_result).unwrap();

    // Get the token by id
    let get_api_token_req_id = GetApiTokenRequest {
        token_id: initial_token.id,
    };
    let get_token_by_id = db.get_api_token(get_api_token_req_id, user_id).unwrap();
    assert_eq!(initial_token.name, get_token_by_id.token.unwrap().name);

    // Create personal token for the user
    let req = CreateApiTokenRequest {
        project_id: String::new(),
        collection_id: col.id,
        name: "collection_token".to_string(),
        expires_at: None,
        permission: 1,
        is_session: false,
    };
    // Create a initial token
    let (tok, _, _) = db.create_api_token(req, user_id, pubkey_result).unwrap();
    assert!(tok.name == "collection_token");

    // ------ FAILS ---------
    // Get the token (failure)
    let get_api_token_req_id = GetApiTokenRequest {
        token_id: "asdasd".to_string(),
    };
    let get_token_by_id = db.get_api_token(get_api_token_req_id, user_id);
    assert!(get_token_by_id.is_err());

    // Get the token (failure / empty)
    let get_api_token_req_id = GetApiTokenRequest {
        token_id: String::new(),
    };
    let get_token_by_id = db.get_api_token(get_api_token_req_id, user_id);
    assert!(get_token_by_id.is_err());
}

#[test]
#[ignore]
#[serial(db)]
fn get_api_tokens_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Create new user
    let user_req = RegisterUserRequest {
        display_name: "test_user_4".to_string(),
        email: "e@mail.dev".to_string(),
        project: String::new(),
    };
    let user_resp = db
        .register_user(user_req, "test_user_4_oidc".to_string())
        .unwrap();
    let user_id = diesel_ulid::DieselUlid::from_str(&user_resp.user_id).unwrap();

    // Activate the user
    let req = ActivateUserRequest {
        user_id: user_resp.user_id,
        project_perms: None,
    };
    db.activate_user(req).unwrap();

    // Add fresh pubkey
    let pubkey_result = db
        .get_or_add_pub_key(
            "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAnl3AKP1/g4qfy4UZH+MRxJC/C/mAuVVxwN+2zU99g54=\n-----END PUBLIC KEY-----\n".to_string(), None
        )
        .unwrap();

    // Create personal token for the user
    let req = CreateApiTokenRequest {
        project_id: String::new(),
        collection_id: String::new(),
        name: "personal_u3_token".to_string(),
        expires_at: None,
        permission: 1,
        is_session: false,
    };
    // Create a initial token
    let (_token_a, _, _) = db.create_api_token(req, user_id, pubkey_result).unwrap();

    // Create personal token for the user
    let req = CreateApiTokenRequest {
        project_id: String::new(),
        collection_id: String::new(),
        name: "personal_u4_token".to_string(),
        expires_at: None,
        permission: 2,
        is_session: false,
    };
    // Create a initial token
    let (_token_b, _, _) = db.create_api_token(req, user_id, pubkey_result).unwrap();

    // Get all tokens
    let request = GetApiTokensRequest {};
    let tokens = db.get_api_tokens(request, user_id).unwrap();

    // Should be 2
    assert!(tokens.token.len() == 2);
    // Iterate all tokens expect them to have either id_a or id_b
    for tok in tokens.token {
        if tok.id == _token_a.id || tok.id == _token_b.id {
            continue;
        } else {
            panic!("Unexpected token id: {:?}", tok.id);
        }
    }
}

#[test]
#[ignore]
#[serial(db)]
fn delete_api_token_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Create new user
    let user_req = RegisterUserRequest {
        display_name: "test_user_4".to_string(),
        email: "e@mail.dev".to_string(),
        project: String::new(),
    };
    let user_resp = db
        .register_user(user_req, "test_user_4_oidc".to_string())
        .unwrap();
    let user_id = diesel_ulid::DieselUlid::from_str(&user_resp.user_id).unwrap();

    // Activate the user
    let req = ActivateUserRequest {
        user_id: user_resp.user_id,
        project_perms: None,
    };
    db.activate_user(req).unwrap();

    // Add fresh pubkey
    let pubkey_result = db
        .get_or_add_pub_key(
            "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAnl3AKP1/g4qfy4UZH+MRxJC/C/mAuVVxwN+2zU99g54=\n-----END PUBLIC KEY-----\n".to_string(), None
        )
        .unwrap();

    // Create personal token for the user
    let req = CreateApiTokenRequest {
        project_id: String::new(),
        collection_id: String::new(),
        name: "personal_u3_token".to_string(),
        expires_at: None,
        permission: 1,
        is_session: false,
    };
    // Create a initial token
    let (token_a, _, _) = db.create_api_token(req, user_id, pubkey_result).unwrap();

    // Get all tokens
    let request = GetApiTokensRequest {};
    let tokens = db.get_api_tokens(request, user_id).unwrap();

    // Should be 2
    assert!(tokens.token.len() == 1);
    // Delete token
    let del_req = DeleteApiTokenRequest {
        token_id: token_a.id,
    };
    let _res = db.delete_api_token(del_req, user_id).unwrap();

    // Get all tokens
    let request = GetApiTokensRequest {};
    let tokens = db.get_api_tokens(request, user_id).unwrap();

    // Should be 2
    assert!(tokens.token.is_empty());
}

#[test]
#[ignore]
#[serial(db)]
fn delete_api_tokens_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Create new user
    let user_req = RegisterUserRequest {
        display_name: "test_user_4".to_string(),
        email: "e@mail.dev".to_string(),
        project: String::new(),
    };
    let user_resp = db
        .register_user(user_req, "test_user_4_oidc".to_string())
        .unwrap();
    let user_id = diesel_ulid::DieselUlid::from_str(&user_resp.user_id).unwrap();

    // Activate the user
    let req = ActivateUserRequest {
        user_id: user_resp.user_id,
        project_perms: None,
    };
    db.activate_user(req).unwrap();

    // Add fresh pubkey
    let pubkey_result = db
        .get_or_add_pub_key(
            "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAnl3AKP1/g4qfy4UZH+MRxJC/C/mAuVVxwN+2zU99g54=\n-----END PUBLIC KEY-----\n".to_string(), None
        )
        .unwrap();

    // Create personal token for the user
    let req = CreateApiTokenRequest {
        project_id: String::new(),
        collection_id: String::new(),
        name: "personal_u3_token".to_string(),
        expires_at: None,
        permission: 1,
        is_session: false,
    };
    // Create a initial token
    let _token_a = db.create_api_token(req, user_id, pubkey_result).unwrap();

    // Create personal token for the user
    let req = CreateApiTokenRequest {
        project_id: String::new(),
        collection_id: String::new(),
        name: "personal_u4_token".to_string(),
        expires_at: None,
        permission: 2,
        is_session: false,
    };
    // Create a initial token
    let _token_b = db.create_api_token(req, user_id, pubkey_result).unwrap();

    // Get all tokens
    let request = GetApiTokensRequest {};
    let tokens = db.get_api_tokens(request, user_id).unwrap();

    // Should be 2
    assert!(tokens.token.len() == 2);
    // Delete ALL tokens from this user
    let del_req = DeleteApiTokensRequest {
        user_id: user_id.to_string(),
    };
    let _ret = db.delete_api_tokens(del_req, user_id).unwrap();
    // Get all tokens
    let request = GetApiTokensRequest {};
    let tokens = db.get_api_tokens(request, user_id).unwrap();

    // Should be 0
    assert!(tokens.token.is_empty());
}

#[test]
#[ignore]
#[serial(db)]
fn get_user_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Create new user
    let user_req = RegisterUserRequest {
        display_name: "test_user_4".to_string(),
        email: "e@mail.dev".to_string(),
        project: String::new(),
    };
    let user_resp = db
        .register_user(user_req, "test_user_4_oidc".to_string())
        .unwrap();
    let user_id = diesel_ulid::DieselUlid::from_str(&user_resp.user_id).unwrap();

    // Activate the user
    let req = ActivateUserRequest {
        user_id: user_resp.user_id,
        project_perms: None,
    };
    db.activate_user(req).unwrap();

    // Test who am i
    let user_info = db.get_user(user_id).unwrap();

    println!("{:?}", user_info);

    assert!(user_info.clone().user.unwrap().active);
    assert_eq!(user_info.clone().user.unwrap().id, user_id.to_string());
    assert_eq!(
        user_info.clone().user.unwrap().external_id,
        "test_user_4_oidc".to_string()
    );
    assert_eq!(
        user_info.user.unwrap().display_name,
        "test_user_4".to_string()
    );

    // -------- FAILS ---------

    let user_info = db.get_user(diesel_ulid::DieselUlid::generate()).unwrap();

    assert!(user_info.user.is_none())
}

#[test]
#[ignore]
#[serial(db)]
fn get_not_activated_users_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    for x in 1337..1339 {
        // Add another user
        // Build request for new user
        let req_2 = RegisterUserRequest {
            display_name: format!("test_user_{}", x),
            email: "e@mail.dev".to_string(),
            project: String::new(),
        };
        // Create new user
        let _resp_2 = db
            .register_user(req_2, format!("external_id_{}", x))
            .unwrap();
    }

    // Build request for new user
    let req = GetNotActivatedUsersRequest {};

    let resp = db
        .get_not_activated_users(req, diesel_ulid::DieselUlid::default())
        .unwrap();

    println!("{:#?}", resp);

    let names = vec![
        "test_user_1".to_string(),
        "test_user_1337".to_string(),
        "test_user_1338".to_string(),
        "regular_user".to_string(),
    ];

    for u in resp.users {
        assert!(
            names.contains(&u.display_name),
            "Unknown displayname: {}",
            u.display_name
        );
    }
}

#[test]
#[ignore]
#[serial(db)]
fn update_user_display_name_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Create new user
    let user_req = RegisterUserRequest {
        display_name: "test_user_4".to_string(),
        email: "e@mail.dev".to_string(),
        project: String::new(),
    };
    let user_resp = db
        .register_user(user_req, "test_user_4_oidc".to_string())
        .unwrap();
    let user_id = diesel_ulid::DieselUlid::from_str(&user_resp.user_id).unwrap();

    // Activate the user
    let req = ActivateUserRequest {
        user_id: user_resp.user_id,
        project_perms: None,
    };
    db.activate_user(req).unwrap();
    let user_info = db.get_user(user_id).unwrap();
    assert_eq!(
        user_info.user.unwrap().display_name,
        "test_user_4".to_string()
    );

    let req = UpdateUserDisplayNameRequest {
        new_display_name: "new_name_1".to_string(),
    };
    db.update_user_display_name(req, user_id).unwrap();

    // Test who am i
    let user_info = db.get_user(user_id).unwrap();

    println!("{:?}", user_info);

    assert!(user_info.clone().user.unwrap().active);
    assert_eq!(user_info.clone().user.unwrap().id, user_id.to_string());
    assert_eq!(
        user_info.clone().user.unwrap().external_id,
        "test_user_4_oidc".to_string()
    );
    assert_eq!(
        user_info.user.unwrap().display_name,
        "new_name_1".to_string()
    );
}

#[test]
#[ignore]
#[serial(db)]
fn get_user_projects_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Create new user
    let user_req = RegisterUserRequest {
        display_name: "test_user_4".to_string(),
        email: "e@mail.dev".to_string(),
        project: String::new(),
    };
    let user_resp = db
        .register_user(user_req, "test_user_4_oidc".to_string())
        .unwrap();
    let user_id = diesel_ulid::DieselUlid::from_str(&user_resp.user_id).unwrap();

    // Activate the user
    let req = ActivateUserRequest {
        user_id: user_resp.user_id,
        project_perms: None,
    };
    db.activate_user(req).unwrap();

    // Create project as admin
    let crt_proj_req = CreateProjectRequest {
        name: "get-user-projects-test-project-001".to_string(),
        description: "First project created in get_user_projects_test()".to_string(),
    };
    let proj_1_ulid = db
        .create_project(crt_proj_req, common::functions::get_admin_user_ulid())
        .unwrap();
    // Add new user to the proj
    let add_user_req = AddUserToProjectRequest {
        project_id: proj_1_ulid.to_string(),
        user_permission: Some(ProjectPermission {
            user_id: user_id.to_string(),
            project_id: proj_1_ulid.to_string(),
            permission: 2,
            service_account: false,
        }),
    };
    db.add_user_to_project(add_user_req, user_id).unwrap();

    // Create project as user
    let crt_proj_req_2 = CreateProjectRequest {
        name: "get-user-projects-test-project-002".to_string(),
        description: "Second project created in get_user_projects_test()".to_string(),
    };
    // This should add the user automatically
    let _proj_2_ulid = db.create_project(crt_proj_req_2, user_id).unwrap();

    // Check the user_perms
    let perms = db.get_user(user_id).unwrap();
    // Should contain two permissions
    assert!(perms.project_permissions.len() == 2);

    for perm in perms.project_permissions {
        assert!(
            perm.project_id == proj_1_ulid.to_string()
                || perm.project_id == _proj_2_ulid.to_string()
        );
    }

    let get_user_projects = GetUserProjectsRequest {
        user_id: user_id.to_string(),
    };

    let get_user_proj = db.get_user_projects(get_user_projects, user_id).unwrap();

    assert!(get_user_proj.projects.len() == 2)
}

#[test]
#[ignore]
#[serial(db)]
fn get_checked_user_id_from_token_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Create new user
    let user_req = RegisterUserRequest {
        display_name: "test_user_4".to_string(),
        email: "e@mail.dev".to_string(),
        project: String::new(),
    };
    let user_resp = db
        .register_user(user_req, "test_user_4_oidc".to_string())
        .unwrap();
    let user_id = diesel_ulid::DieselUlid::from_str(&user_resp.user_id).unwrap();

    // Activate the user
    let req = ActivateUserRequest {
        user_id: user_resp.user_id,
        project_perms: None,
    };
    db.activate_user(req).unwrap();

    // Create project as admin
    let crt_proj_req = CreateProjectRequest {
        name: "testproj-1".to_string(),
        description: String::new(),
    };
    let proj_1_ulid = db
        .create_project(crt_proj_req, common::functions::get_admin_user_ulid())
        .unwrap();
    // Add new user to the proj with permissions "Read"
    let add_user_req = AddUserToProjectRequest {
        project_id: proj_1_ulid.to_string(),
        user_permission: Some(ProjectPermission {
            user_id: user_id.to_string(),
            project_id: proj_1_ulid.to_string(),
            permission: 2,
            service_account: false,
        }),
    };
    db.add_user_to_project(add_user_req, user_id).unwrap();

    // Create project as user -> Should be "admin"
    let crt_proj_req_2 = CreateProjectRequest {
        name: "testproj-2".to_string(),
        description: String::new(),
    };
    // This should add the user automatically
    let _proj_2_ulid = db.create_project(crt_proj_req_2, user_id).unwrap();

    // Create / Get tokens with differing permissions:

    // Add fresh pubkey
    let pubkey_result = db
        .get_or_add_pub_key(
            "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAnl3AKP1/g4qfy4UZH+MRxJC/C/mAuVVxwN+2zU99g54=\n-----END PUBLIC KEY-----\n".to_string(), None
        )
        .unwrap();

    // Create personal token for the user
    let req = CreateApiTokenRequest {
        project_id: String::new(),
        collection_id: String::new(),
        name: "personal_u2_token".to_string(),
        expires_at: None,
        permission: 3, // "APPEND permissions" -> Should be ignored
        is_session: false,
    };
    // Create a initial token
    let (regular_personal_token, _, _) = db.create_api_token(req, user_id, pubkey_result).unwrap();
    // Admin token
    let admin_token = diesel_ulid::DieselUlid::from(
        uuid::Uuid::parse_str("12345678-8888-8888-8888-999999999999").unwrap(),
    );
    // Personal token with perm = 3
    let regular_personal_token =
        diesel_ulid::DieselUlid::from_str(&regular_personal_token.id).unwrap();
    // Project scoped token with "READ" permissions
    let req = CreateApiTokenRequest {
        project_id: proj_1_ulid.to_string(),
        collection_id: String::new(),
        name: "personal_u3_token".to_string(),
        expires_at: None,
        permission: 2, // READ permissions
        is_session: false,
    };
    // Create a initial token
    let (project_token_with_read, _, _) = db.create_api_token(req, user_id, pubkey_result).unwrap();
    let project_token_with_read =
        diesel_ulid::DieselUlid::from_str(&project_token_with_read.id).unwrap();
    // Project scoped token with "ADMIN" permissions
    let req = CreateApiTokenRequest {
        project_id: _proj_2_ulid.to_string(),
        collection_id: String::new(),
        name: "personal_u4_token".to_string(),
        expires_at: None,
        permission: 5, // ADMIN permissions
        is_session: false,
    };
    // Create a initial token
    let (project_token_with_admin, _, _) =
        db.create_api_token(req, user_id, pubkey_result).unwrap();
    let project_token_with_admin =
        diesel_ulid::DieselUlid::from_str(&project_token_with_admin.id).unwrap();

    // Create collection in proj_1 --> Admin
    let ccoll_1_req = CreateNewCollectionRequest {
        name: "test-col-1".to_string(),
        description: String::new(),
        label_ontology: None,
        project_id: proj_1_ulid.to_string(),
        labels: Vec::new(),
        hooks: Vec::new(),
        dataclass: 0,
    };

    let col_1 = db.create_new_collection(ccoll_1_req, user_id).unwrap();
    // Create collection in proj_1 --> Admin
    let ccoll_2_req = CreateNewCollectionRequest {
        name: "test-col-2".to_string(),
        description: String::new(),
        label_ontology: None,
        project_id: _proj_2_ulid.to_string(),
        labels: Vec::new(),
        hooks: Vec::new(),
        dataclass: 0,
    };

    let col_2 = db.create_new_collection(ccoll_2_req, user_id).unwrap();

    // Collection scoped token with "READ" permissions
    let req = CreateApiTokenRequest {
        project_id: String::new(),
        collection_id: col_2.clone().0.collection_id,
        name: "personal_u5_token".to_string(),
        expires_at: None,
        permission: 2, // ADMIN permissions
        is_session: false,
    };
    // Create a initial token
    let (col_token_with_read, _, _) = db.create_api_token(req, user_id, pubkey_result).unwrap();
    let col_token_with_read = diesel_ulid::DieselUlid::from_str(&col_token_with_read.id).unwrap();
    // Collection scoped token with "ADMIN" permissions
    let req = CreateApiTokenRequest {
        project_id: String::new(),
        collection_id: col_1.0.collection_id,
        name: "personal_u5_token".to_string(),
        expires_at: None,
        permission: 5, // ADMIN permissions
        is_session: false,
    };
    // Create a initial token
    let (col_token_with_admin, _, _) = db.create_api_token(req, user_id, pubkey_result).unwrap();
    let col_token_with_admin = diesel_ulid::DieselUlid::from_str(&col_token_with_admin.id).unwrap();

    // TEST all tokens / cases
    // Case 1. Admin token / Admin context:
    let (token_user_uuid, _) = db
        .get_checked_user_id_from_token(
            &admin_token,
            &(Context {
                user_right: database::models::enums::UserRights::ADMIN,
                resource_type: database::models::enums::Resources::COLLECTION,
                resource_id: diesel_ulid::DieselUlid::default(),
                admin: true,
                personal: false,
                oidc_context: false,
                allow_service_accounts: false,
            }),
        )
        .unwrap();
    assert_eq!(
        token_user_uuid.to_string(),
        common::functions::get_admin_user_ulid().to_string()
    );
    // Case 2. Non admin token / Requested admin context: SHOULD fail
    let res = db.get_checked_user_id_from_token(
        &col_token_with_admin,
        &(Context {
            user_right: database::models::enums::UserRights::ADMIN,
            resource_type: database::models::enums::Resources::COLLECTION,
            resource_id: diesel_ulid::DieselUlid::default(),
            admin: true,
            personal: false,
            oidc_context: false,
            allow_service_accounts: false,
        }),
    );
    assert!(res.is_err());
    // Case 3. Personal token in "ADMIN" project
    let (token_user_uuid, _) = db
        .get_checked_user_id_from_token(
            &regular_personal_token,
            &(Context {
                user_right: database::models::enums::UserRights::ADMIN,
                resource_type: database::models::enums::Resources::PROJECT,
                resource_id: diesel_ulid::DieselUlid::from_str(&_proj_2_ulid.to_string()).unwrap(),
                admin: false,
                personal: false,
                oidc_context: false,
                allow_service_accounts: false,
            }),
        )
        .unwrap();
    assert_eq!(token_user_uuid, user_id);
    // READ project
    let (token_user_uuid, _) = db
        .get_checked_user_id_from_token(
            &regular_personal_token,
            &(Context {
                user_right: database::models::enums::UserRights::READ,
                resource_type: database::models::enums::Resources::PROJECT,
                resource_id: diesel_ulid::DieselUlid::from_str(&proj_1_ulid.to_string()).unwrap(),
                admin: false,
                personal: false,
                oidc_context: false,
                allow_service_accounts: false,
            }),
        )
        .unwrap();
    assert_eq!(token_user_uuid, user_id);
    // READ in ADMIN project
    let (token_user_uuid, _) = db
        .get_checked_user_id_from_token(
            &regular_personal_token,
            &(Context {
                user_right: database::models::enums::UserRights::READ,
                resource_type: database::models::enums::Resources::PROJECT,
                resource_id: diesel_ulid::DieselUlid::from_str(&_proj_2_ulid.to_string()).unwrap(),
                admin: false,
                personal: false,
                oidc_context: false,
                allow_service_accounts: false,
            }),
        )
        .unwrap();
    assert_eq!(token_user_uuid, user_id);
    // Personal only
    let (token_user_uuid, _) = db
        .get_checked_user_id_from_token(
            &regular_personal_token,
            &(Context {
                user_right: database::models::enums::UserRights::READ,
                resource_type: database::models::enums::Resources::PROJECT,
                resource_id: diesel_ulid::DieselUlid::from_str(&_proj_2_ulid.to_string()).unwrap(),
                admin: false,
                personal: true,
                oidc_context: false,
                allow_service_accounts: false,
            }),
        )
        .unwrap();
    assert_eq!(token_user_uuid, user_id);
    // Personal with unpersonal token user
    let res = db.get_checked_user_id_from_token(
        &project_token_with_admin,
        &(Context {
            user_right: database::models::enums::UserRights::READ,
            resource_type: database::models::enums::Resources::PROJECT,
            resource_id: diesel_ulid::DieselUlid::from_str(&_proj_2_ulid.to_string()).unwrap(),
            admin: false,
            personal: true,
            oidc_context: false,
            allow_service_accounts: false,
        }),
    );
    assert!(res.is_err());
    // Project token for collection
    // Personal with unpersonal token user
    let (token_user_uuid, _) = db
        .get_checked_user_id_from_token(
            &project_token_with_admin,
            &(Context {
                user_right: database::models::enums::UserRights::READ,
                resource_type: database::models::enums::Resources::COLLECTION,
                resource_id: diesel_ulid::DieselUlid::from_str(&col_2.0.collection_id).unwrap(),
                admin: false,
                personal: false,
                oidc_context: false,
                allow_service_accounts: false,
            }),
        )
        .unwrap();
    assert_eq!(token_user_uuid, user_id);
    // Collection with read with "higher" permissions -> Should fail
    let res = db.get_checked_user_id_from_token(
        &col_token_with_read,
        &(Context {
            user_right: database::models::enums::UserRights::ADMIN,
            resource_type: database::models::enums::Resources::COLLECTION,
            resource_id: diesel_ulid::DieselUlid::from_str(&col_2.0.collection_id).unwrap(),
            admin: false,
            personal: false,
            oidc_context: false,
            allow_service_accounts: false,
        }),
    );
    assert!(res.is_err());
    // Project with read with "higher" permissions -> Should fail
    let res = db.get_checked_user_id_from_token(
        &project_token_with_read,
        &(Context {
            user_right: database::models::enums::UserRights::ADMIN,
            resource_type: database::models::enums::Resources::COLLECTION,
            resource_id: diesel_ulid::DieselUlid::from_str(&col_2.0.collection_id).unwrap(),
            admin: false,
            personal: false,
            oidc_context: false,
            allow_service_accounts: false,
        }),
    );
    assert!(res.is_err());
}
