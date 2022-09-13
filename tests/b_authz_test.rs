use aruna_server::{
    api::aruna::api::storage::{
        models::v1::ProjectPermission,
        services::v1::{
            ActivateUserRequest,
            AddUserToProjectRequest,
            CreateApiTokenRequest,
            CreateProjectRequest,
            DeleteApiTokenRequest,
            DeleteApiTokensRequest,
            GetApiTokenRequest,
            GetApiTokensRequest,
            RegisterUserRequest,
            UpdateUserDisplayNameRequest,
            CreateNewCollectionRequest,
        },
    },
    database::{ self },
    server::services::authz::Context,
};
use serial_test::serial;

#[test]
#[ignore]
#[serial(db)]
fn get_or_add_pubkey_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Insert new element -> Create new serial number
    let result = db.get_or_add_pub_key("pubkey_test_1".to_string()).unwrap();
    // Insert a second "pubkey"
    let _result_2 = db.get_or_add_pub_key("pubkey_test_2".to_string()).unwrap();
    // Try to insert the first serial again -> should be the same as result
    let result_3 = db.get_or_add_pub_key("pubkey_test_1".to_string()).unwrap();
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
        if
            key.pubkey == *"pubkey_test_1" ||
            key.pubkey == *"pubkey_test_2" ||
            key.pubkey == *"admin_key"
        {
            continue;
            // Panic otherwise -> unknown pubkey in db
        } else {
            panic!("Expected pubkey_test_1 or pubkey_test_2, got: {:?}", key.pubkey);
        }
    }
}

#[test]
#[ignore]
#[serial(db)]
fn get_oidc_user_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Get admin user via (fake) oidc id
    let user_id = db.get_oidc_user("admin_test_oidc_id").unwrap().unwrap();

    // Expect the user to have the following uuid
    let parsed_uid = uuid::Uuid::parse_str("12345678-1234-1234-1234-111111111111").unwrap();
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
    };
    // Create new user
    let _resp = db.register_user(req, "test_user_1_oidc".to_string()).unwrap();
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
    };
    // Create new user
    let resp_2 = db.register_user(req_2, "test_user_2_oidc".to_string()).unwrap();

    // Build request for new user
    let req = ActivateUserRequest {
        user_id: resp_2.user_id,
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
    };
    let user_resp = db.register_user(user_req, "test_user_3_oidc".to_string()).unwrap();
    let user_id = uuid::Uuid::parse_str(&user_resp.user_id).unwrap();

    // Activate the user
    let req = ActivateUserRequest {
        user_id: user_resp.user_id,
    };
    db.activate_user(req).unwrap();

    // Add fresh pubkey
    let pubkey_result = db.get_or_add_pub_key("pubkey_test_2".to_string()).unwrap();

    // Create personal token for the user
    let req = CreateApiTokenRequest {
        project_id: "".to_string(),
        collection_id: "".to_string(),
        name: "personal_u1_token".to_string(),
        expires_at: None,
        permission: 1,
    };
    let _token = db.create_api_token(req, user_id, pubkey_result).unwrap();
}

#[test]
#[ignore]
#[serial(db)]
fn get_api_token_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Create new user
    let user_req = RegisterUserRequest {
        display_name: "test_user_4".to_string(),
    };
    let user_resp = db.register_user(user_req, "test_user_4_oidc".to_string()).unwrap();
    let user_id = uuid::Uuid::parse_str(&user_resp.user_id).unwrap();

    // Activate the user
    let req = ActivateUserRequest {
        user_id: user_resp.user_id,
    };
    db.activate_user(req).unwrap();

    // Add fresh pubkey
    let pubkey_result = db.get_or_add_pub_key("pubkey_test_1".to_string()).unwrap();

    // Create personal token for the user
    let req = CreateApiTokenRequest {
        project_id: "".to_string(),
        collection_id: "".to_string(),
        name: "personal_u2_token".to_string(),
        expires_at: None,
        permission: 1,
    };
    // Create a initial token
    let initial_token = db.create_api_token(req.clone(), user_id, pubkey_result).unwrap();

    // Get the token by id
    let get_api_token_req_id = GetApiTokenRequest {
        token_id: initial_token.id,
        name: "".to_string(),
    };
    let get_token_by_id = db.get_api_token(get_api_token_req_id, user_id).unwrap();
    assert_eq!(initial_token.name, get_token_by_id.token.unwrap().name);

    // Get the token by name
    let get_api_token_req_name = GetApiTokenRequest {
        token_id: "".to_string(),
        name: req.name,
    };
    let get_token_by_name = db.get_api_token(get_api_token_req_name, user_id).unwrap();
    assert_eq!(initial_token.name, get_token_by_name.token.unwrap().name);
}

#[test]
#[ignore]
#[serial(db)]
fn get_api_tokens_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Create new user
    let user_req = RegisterUserRequest {
        display_name: "test_user_4".to_string(),
    };
    let user_resp = db.register_user(user_req, "test_user_4_oidc".to_string()).unwrap();
    let user_id = uuid::Uuid::parse_str(&user_resp.user_id).unwrap();

    // Activate the user
    let req = ActivateUserRequest {
        user_id: user_resp.user_id,
    };
    db.activate_user(req).unwrap();

    // Add fresh pubkey
    let pubkey_result = db.get_or_add_pub_key("pubkey_test_1".to_string()).unwrap();

    // Create personal token for the user
    let req = CreateApiTokenRequest {
        project_id: "".to_string(),
        collection_id: "".to_string(),
        name: "personal_u3_token".to_string(),
        expires_at: None,
        permission: 1,
    };
    // Create a initial token
    let _token_a = db.create_api_token(req, user_id, pubkey_result).unwrap();

    // Create personal token for the user
    let req = CreateApiTokenRequest {
        project_id: "".to_string(),
        collection_id: "".to_string(),
        name: "personal_u4_token".to_string(),
        expires_at: None,
        permission: 2,
    };
    // Create a initial token
    let _token_b = db.create_api_token(req, user_id, pubkey_result).unwrap();

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
    };
    let user_resp = db.register_user(user_req, "test_user_4_oidc".to_string()).unwrap();
    let user_id = uuid::Uuid::parse_str(&user_resp.user_id).unwrap();

    // Activate the user
    let req = ActivateUserRequest {
        user_id: user_resp.user_id,
    };
    db.activate_user(req).unwrap();

    // Add fresh pubkey
    let pubkey_result = db.get_or_add_pub_key("pubkey_test_1".to_string()).unwrap();

    // Create personal token for the user
    let req = CreateApiTokenRequest {
        project_id: "".to_string(),
        collection_id: "".to_string(),
        name: "personal_u3_token".to_string(),
        expires_at: None,
        permission: 1,
    };
    // Create a initial token
    let token_a = db.create_api_token(req, user_id, pubkey_result).unwrap();

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
    };
    let user_resp = db.register_user(user_req, "test_user_4_oidc".to_string()).unwrap();
    let user_id = uuid::Uuid::parse_str(&user_resp.user_id).unwrap();

    // Activate the user
    let req = ActivateUserRequest {
        user_id: user_resp.user_id,
    };
    db.activate_user(req).unwrap();

    // Add fresh pubkey
    let pubkey_result = db.get_or_add_pub_key("pubkey_test_1".to_string()).unwrap();

    // Create personal token for the user
    let req = CreateApiTokenRequest {
        project_id: "".to_string(),
        collection_id: "".to_string(),
        name: "personal_u3_token".to_string(),
        expires_at: None,
        permission: 1,
    };
    // Create a initial token
    let _token_a = db.create_api_token(req, user_id, pubkey_result).unwrap();

    // Create personal token for the user
    let req = CreateApiTokenRequest {
        project_id: "".to_string(),
        collection_id: "".to_string(),
        name: "personal_u4_token".to_string(),
        expires_at: None,
        permission: 2,
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
    };
    let user_resp = db.register_user(user_req, "test_user_4_oidc".to_string()).unwrap();
    let user_id = uuid::Uuid::parse_str(&user_resp.user_id).unwrap();

    // Activate the user
    let req = ActivateUserRequest {
        user_id: user_resp.user_id,
    };
    db.activate_user(req).unwrap();

    // Test who am i
    let user_info = db.get_user(user_id).unwrap();

    println!("{:?}", user_info);

    assert!(user_info.clone().user.unwrap().active);
    assert_eq!(user_info.clone().user.unwrap().id, user_id.to_string());
    assert_eq!(user_info.clone().user.unwrap().external_id, "test_user_4_oidc".to_string());
    assert_eq!(user_info.user.unwrap().display_name, "test_user_4".to_string());
}

#[test]
#[ignore]
#[serial(db)]
fn update_user_display_name_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Create new user
    let user_req = RegisterUserRequest {
        display_name: "test_user_4".to_string(),
    };
    let user_resp = db.register_user(user_req, "test_user_4_oidc".to_string()).unwrap();
    let user_id = uuid::Uuid::parse_str(&user_resp.user_id).unwrap();

    // Activate the user
    let req = ActivateUserRequest {
        user_id: user_resp.user_id,
    };
    db.activate_user(req).unwrap();
    let user_info = db.get_user(user_id).unwrap();
    assert_eq!(user_info.user.unwrap().display_name, "test_user_4".to_string());

    let req = UpdateUserDisplayNameRequest {
        new_display_name: "new_name_1".to_string(),
    };
    db.update_user_display_name(req, user_id).unwrap();

    // Test who am i
    let user_info = db.get_user(user_id).unwrap();

    println!("{:?}", user_info);

    assert!(user_info.clone().user.unwrap().active);
    assert_eq!(user_info.clone().user.unwrap().id, user_id.to_string());
    assert_eq!(user_info.clone().user.unwrap().external_id, "test_user_4_oidc".to_string());
    assert_eq!(user_info.user.unwrap().display_name, "new_name_1".to_string());
}

#[test]
#[ignore]
#[serial(db)]
fn get_user_projects_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Create new user
    let user_req = RegisterUserRequest {
        display_name: "test_user_4".to_string(),
    };
    let user_resp = db.register_user(user_req, "test_user_4_oidc".to_string()).unwrap();
    let user_id = uuid::Uuid::parse_str(&user_resp.user_id).unwrap();

    // Activate the user
    let req = ActivateUserRequest {
        user_id: user_resp.user_id,
    };
    db.activate_user(req).unwrap();

    // Create project as admin
    let crt_proj_req = CreateProjectRequest {
        name: "testproj_1".to_string(),
        description: "".to_string(),
    };
    let proj_1 = db
        .create_project(
            crt_proj_req,
            uuid::Uuid::parse_str("12345678-1234-1234-1234-111111111111").unwrap()
        )
        .unwrap();
    // Add new user to the proj
    let add_user_req = AddUserToProjectRequest {
        project_id: proj_1.project_id.clone(),
        user_permission: Some(ProjectPermission {
            user_id: user_id.to_string(),
            project_id: proj_1.clone().project_id,
            permission: 2,
        }),
    };
    db.add_user_to_project(add_user_req, user_id).unwrap();

    // Create project as user
    let crt_proj_req_2 = CreateProjectRequest {
        name: "testproj_2".to_string(),
        description: "".to_string(),
    };
    // This should add the user automatically
    let _proj_2 = db.create_project(crt_proj_req_2, user_id).unwrap();

    // Check the user_perms
    let perms = db.get_user(user_id).unwrap();
    // Should contain two permissions
    assert!(perms.project_permissions.len() == 2);

    for perm in perms.project_permissions {
        assert!(perm.project_id == proj_1.project_id || perm.project_id == _proj_2.project_id);
    }
}

#[test]
#[ignore]
#[serial(db)]
fn get_checked_user_id_from_token_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Create new user
    let user_req = RegisterUserRequest {
        display_name: "test_user_4".to_string(),
    };
    let user_resp = db.register_user(user_req, "test_user_4_oidc".to_string()).unwrap();
    let user_id = uuid::Uuid::parse_str(&user_resp.user_id).unwrap();

    // Activate the user
    let req = ActivateUserRequest {
        user_id: user_resp.user_id,
    };
    db.activate_user(req).unwrap();

    // Create project as admin
    let crt_proj_req = CreateProjectRequest {
        name: "testproj_1".to_string(),
        description: "".to_string(),
    };
    let proj_1 = db
        .create_project(
            crt_proj_req,
            uuid::Uuid::parse_str("12345678-1234-1234-1234-111111111111").unwrap()
        )
        .unwrap();
    // Add new user to the proj with permissions "Read"
    let add_user_req = AddUserToProjectRequest {
        project_id: proj_1.project_id.clone(),
        user_permission: Some(ProjectPermission {
            user_id: user_id.to_string(),
            project_id: proj_1.clone().project_id,
            permission: 2,
        }),
    };
    db.add_user_to_project(add_user_req, user_id).unwrap();

    // Create project as user -> Should be "admin"
    let crt_proj_req_2 = CreateProjectRequest {
        name: "testproj_2".to_string(),
        description: "".to_string(),
    };
    // This should add the user automatically
    let _proj_2 = db.create_project(crt_proj_req_2, user_id).unwrap();

    // Create / Get tokens with differing permissions:

    // Add fresh pubkey
    let pubkey_result = db.get_or_add_pub_key("pubkey_test_1".to_string()).unwrap();

    // Create personal token for the user
    let req = CreateApiTokenRequest {
        project_id: "".to_string(),
        collection_id: "".to_string(),
        name: "personal_u2_token".to_string(),
        expires_at: None,
        permission: 3, // "APPEND permissions" -> Should be ignored
    };
    // Create a initial token
    let regular_personal_token = db.create_api_token(req.clone(), user_id, pubkey_result).unwrap();
    // Admin token
    let admin_token = uuid::Uuid::parse_str("12345678-8888-8888-8888-999999999999").unwrap();
    // Personal token with perm = 3
    let regular_personal_token = uuid::Uuid::parse_str(&regular_personal_token.id).unwrap();
    // Project scoped token with "READ" permissions
    let req = CreateApiTokenRequest {
        project_id: proj_1.project_id.clone(),
        collection_id: "".to_string(),
        name: "personal_u3_token".to_string(),
        expires_at: None,
        permission: 2, // READ permissions
    };
    // Create a initial token
    let project_token_with_read = db.create_api_token(req.clone(), user_id, pubkey_result).unwrap();
    let project_token_with_read = uuid::Uuid::parse_str(&project_token_with_read.id).unwrap();
    // Project scoped token with "ADMIN" permissions
    let req = CreateApiTokenRequest {
        project_id: _proj_2.project_id.clone(),
        collection_id: "".to_string(),
        name: "personal_u4_token".to_string(),
        expires_at: None,
        permission: 5, // ADMIN permissions
    };
    // Create a initial token
    let project_token_with_admin = db
        .create_api_token(req.clone(), user_id, pubkey_result)
        .unwrap();
    let project_token_with_admin = uuid::Uuid::parse_str(&project_token_with_admin.id).unwrap();

    // Create collection in proj_1 --> Admin
    let ccoll_1_req = CreateNewCollectionRequest {
        name: "test_col_1".to_string(),
        description: "".to_string(),
        project_id: proj_1.project_id.clone(),
        labels: Vec::new(),
        hooks: Vec::new(),
        dataclass: 0,
    };

    let col_1 = db.create_new_collection(ccoll_1_req, user_id).unwrap();
    // Create collection in proj_1 --> Admin
    let ccoll_2_req = CreateNewCollectionRequest {
        name: "test_col_2".to_string(),
        description: "".to_string(),
        project_id: _proj_2.clone().project_id,
        labels: Vec::new(),
        hooks: Vec::new(),
        dataclass: 0,
    };

    let col_2 = db.create_new_collection(ccoll_2_req, user_id).unwrap();

    // Collection scoped token with "READ" permissions
    let req = CreateApiTokenRequest {
        project_id: "".to_string(),
        collection_id: col_2.clone().collection_id,
        name: "personal_u5_token".to_string(),
        expires_at: None,
        permission: 2, // ADMIN permissions
    };
    // Create a initial token
    let col_token_with_read = db.create_api_token(req.clone(), user_id, pubkey_result).unwrap();
    let col_token_with_read = uuid::Uuid::parse_str(&col_token_with_read.id).unwrap();
    // Collection scoped token with "ADMIN" permissions
    let req = CreateApiTokenRequest {
        project_id: "".to_string(),
        collection_id: col_1.collection_id,
        name: "personal_u5_token".to_string(),
        expires_at: None,
        permission: 5, // ADMIN permissions
    };
    // Create a initial token
    let col_token_with_admin = db.create_api_token(req.clone(), user_id, pubkey_result).unwrap();
    let col_token_with_admin = uuid::Uuid::parse_str(&col_token_with_admin.id).unwrap();

    // TEST all tokens / cases
    // Case 1. Admin token / Admin context:
    let res = db
        .get_checked_user_id_from_token(
            &admin_token,
            &(Context {
                user_right: database::models::enums::UserRights::ADMIN,
                resource_type: database::models::enums::Resources::COLLECTION,
                resource_id: uuid::Uuid::default(),
                admin: true,
                personal: false,
                oidc_context: false,
            })
        )
        .unwrap();
    assert_eq!(res.to_string(), "12345678-1234-1234-1234-111111111111".to_string());
    // Case 2. Non admin token / Requested admin context: SHOULD fail
    let res = db.get_checked_user_id_from_token(
        &col_token_with_admin,
        &(Context {
            user_right: database::models::enums::UserRights::ADMIN,
            resource_type: database::models::enums::Resources::COLLECTION,
            resource_id: uuid::Uuid::default(),
            admin: true,
            personal: false,
            oidc_context: false,
        })
    );
    assert!(res.is_err());
    // Case 3. Personal token in "ADMIN" project
    let res = db
        .get_checked_user_id_from_token(
            &regular_personal_token,
            &(Context {
                user_right: database::models::enums::UserRights::ADMIN,
                resource_type: database::models::enums::Resources::PROJECT,
                resource_id: uuid::Uuid::parse_str(&_proj_2.project_id).unwrap(),
                admin: false,
                personal: false,
                oidc_context: false,
            })
        )
        .unwrap();
    assert_eq!(res, user_id);
    // READ project
    let res = db
        .get_checked_user_id_from_token(
            &regular_personal_token,
            &(Context {
                user_right: database::models::enums::UserRights::READ,
                resource_type: database::models::enums::Resources::PROJECT,
                resource_id: uuid::Uuid::parse_str(&proj_1.project_id).unwrap(),
                admin: false,
                personal: false,
                oidc_context: false,
            })
        )
        .unwrap();
    assert_eq!(res, user_id);
    // READ in ADMIN project
    let res = db
        .get_checked_user_id_from_token(
            &regular_personal_token,
            &(Context {
                user_right: database::models::enums::UserRights::READ,
                resource_type: database::models::enums::Resources::PROJECT,
                resource_id: uuid::Uuid::parse_str(&_proj_2.project_id).unwrap(),
                admin: false,
                personal: false,
                oidc_context: false,
            })
        )
        .unwrap();
    assert_eq!(res, user_id);
    // Personal only
    let res = db
        .get_checked_user_id_from_token(
            &regular_personal_token,
            &(Context {
                user_right: database::models::enums::UserRights::READ,
                resource_type: database::models::enums::Resources::PROJECT,
                resource_id: uuid::Uuid::parse_str(&_proj_2.project_id).unwrap(),
                admin: false,
                personal: true,
                oidc_context: false,
            })
        )
        .unwrap();
    assert_eq!(res, user_id);
    // Personal with unpersonal token user
    let res = db.get_checked_user_id_from_token(
        &project_token_with_admin,
        &(Context {
            user_right: database::models::enums::UserRights::READ,
            resource_type: database::models::enums::Resources::PROJECT,
            resource_id: uuid::Uuid::parse_str(&_proj_2.project_id).unwrap(),
            admin: false,
            personal: true,
            oidc_context: false,
        })
    );
    assert!(res.is_err());
    // Project token for collection
    // Personal with unpersonal token user
    let res = db
        .get_checked_user_id_from_token(
            &project_token_with_admin,
            &(Context {
                user_right: database::models::enums::UserRights::READ,
                resource_type: database::models::enums::Resources::COLLECTION,
                resource_id: uuid::Uuid::parse_str(&col_2.collection_id).unwrap(),
                admin: false,
                personal: false,
                oidc_context: false,
            })
        )
        .unwrap();
    assert_eq!(res, user_id);
    // Collection with read with "higher" permissions -> Should fail
    let res = db.get_checked_user_id_from_token(
        &col_token_with_read,
        &(Context {
            user_right: database::models::enums::UserRights::ADMIN,
            resource_type: database::models::enums::Resources::COLLECTION,
            resource_id: uuid::Uuid::parse_str(&col_2.collection_id).unwrap(),
            admin: false,
            personal: false,
            oidc_context: false,
        })
    );
    assert!(res.is_err());
    // Project with read with "higher" permissions -> Should fail
    let res = db.get_checked_user_id_from_token(
        &project_token_with_read,
        &(Context {
            user_right: database::models::enums::UserRights::ADMIN,
            resource_type: database::models::enums::Resources::COLLECTION,
            resource_id: uuid::Uuid::parse_str(&col_2.collection_id).unwrap(),
            admin: false,
            personal: false,
            oidc_context: false,
        })
    );
    assert!(res.is_err());
}