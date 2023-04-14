extern crate core;

mod common;

use aruna_rust_api::api::storage::services::v1::{
    DestroyProjectRequest, EditUserPermissionsForProjectRequest,
};
use aruna_rust_api::api::storage::{
    models::v1::{ProjectOverview, ProjectPermission},
    services::v1::{
        ActivateUserRequest, AddUserToProjectRequest, CreateProjectRequest, GetProjectRequest,
        RegisterUserRequest, RemoveUserFromProjectRequest, UpdateProjectRequest,
    },
};
use aruna_server::database;
use rand::seq::IteratorRandom;
use rand::Rng;
use serial_test::serial;
use std::io::{Error, ErrorKind};
use std::str::FromStr;

use crate::common::functions::{create_collection, TCreateCollection};

#[test]
#[ignore]
#[serial(db)]
fn create_project_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let creator = common::functions::get_admin_user_ulid();

    let request = CreateProjectRequest {
        name: "create-project-test-project".to_string(),
        description: "Project created in create_project_test()".to_string(),
    };

    let response = db.create_project(request, creator).unwrap();
    let id = diesel_ulid::DieselUlid::from_str(&response.project_id).unwrap();

    assert!(!id.to_string().is_empty())
}

#[test]
#[ignore]
#[serial(db)]
fn get_project_test() {
    // This function creates a project and returns an "project_overview"
    let _created_project = common::functions::create_project(None);
}

#[test]
#[ignore]
#[serial(db)]
fn update_project_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let creator = common::functions::get_admin_user_ulid();

    let project_id = common::functions::create_project(None).id;

    assert!(!project_id.to_string().is_empty());

    // Update empty project metadata
    let mut updated_name = "updated-project-name".to_string();
    let updated_description = "Updated project description".to_string();
    let mut update_request = UpdateProjectRequest {
        project_id: project_id.to_string(),
        name: updated_name.to_string(),
        description: updated_description.to_string(),
    };

    let update_response = db.update_project(update_request.clone(), creator).unwrap();
    let updated_project = update_response.project.unwrap();

    assert_eq!(project_id.to_string(), updated_project.id);
    assert_eq!(updated_name, updated_project.name);
    assert_eq!(updated_description, updated_project.description);

    // Create random collection in project
    let _random_collection = common::functions::create_collection(TCreateCollection {
        project_id: project_id.to_string(),
        creator_id: Some(creator.to_string()),
        ..Default::default()
    });

    // Try to update project name of non-empty project --> Error
    updated_name = "error-project".to_string();
    update_request.name = updated_name;

    let update_response = db.update_project(update_request, creator);

    assert!(update_response.is_err())
}

#[test]
#[ignore]
#[serial(db)]
fn destroy_empty_project_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let creator = common::functions::get_admin_user_ulid();

    let _created_project = common::functions::create_project(None);
    let project_id = diesel_ulid::DieselUlid::from_str(&_created_project.id).unwrap();
    assert!(!project_id.to_string().is_empty());

    // Destroy project
    let destroy_request = DestroyProjectRequest {
        project_id: project_id.to_string(),
    };
    db.destroy_project(destroy_request, creator).unwrap();

    // Validate project does not exist anymore
    let get_request = GetProjectRequest {
        project_id: project_id.to_string(),
    };
    let get_response = db.get_project(get_request, creator).unwrap();

    (match get_response.project {
        Some(_) => Err(Error::new(
            ErrorKind::Other,
            "Deleted project still exists.",
        )),
        None => Ok(()),
    })
    .unwrap();
}

#[test]
#[ignore]
#[should_panic]
#[serial(db)]
fn destroy_non_empty_project_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let creator = common::functions::get_admin_user_ulid();

    let created_project = common::functions::create_project(None);
    // Validate creation
    let project_id = diesel_ulid::DieselUlid::from_str(&created_project.id).unwrap();
    assert!(!project_id.to_string().is_empty());

    // Create collection in project
    create_collection(TCreateCollection {
        project_id: project_id.to_string(),
        ..Default::default()
    });

    // Try to destroy non-empty project which should fail
    let destroy_request = DestroyProjectRequest {
        project_id: project_id.to_string(),
    };
    db.destroy_project(destroy_request, creator).unwrap();
}

#[test]
#[ignore]
#[serial(db)]
fn add_remove_project_user_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let creator = common::functions::get_admin_user_ulid();

    // Register and activate some random users
    let mut rnd_user_ids: Vec<String> = Vec::new();
    for num in 1..5 {
        let register_user_request = RegisterUserRequest {
            display_name: format!("Random User {}", num),
            email: format!("randy{num}@mail.dev"),
            project: "Some really nice project".to_string(),
        };
        let user_id = db
            .register_user(
                register_user_request,
                "Yep. It is a random user.".to_string(),
            )
            .unwrap()
            .user_id;
        db.activate_user(ActivateUserRequest {
            user_id: user_id.clone(),
            project_perms: None,
        })
        .unwrap();
        rnd_user_ids.push(user_id);
    }

    // Create project
    let create_request = CreateProjectRequest {
        name: "add-remove-project-user-test-project".to_string(),
        description: "Project created for add_remove_project_user_test()".to_string(),
    };
    let create_response = db.create_project(create_request, creator).unwrap();

    // Validate project creation
    let project_id = diesel_ulid::DieselUlid::from_str(&create_response.project_id).unwrap();
    assert!(!project_id.to_string().is_empty());

    // Add several users to project
    let mut rng = rand::thread_rng();
    for user_id in &rnd_user_ids {
        let user_add_request = AddUserToProjectRequest {
            project_id: project_id.to_string(),
            user_permission: Some(ProjectPermission {
                user_id: user_id.to_string(),
                project_id: project_id.to_string(),
                permission: rng.gen_range(2..5),
                service_account: false,
            }),
        };

        db.add_user_to_project(user_add_request, creator).unwrap();
    }

    // Validate added users
    let get_request = GetProjectRequest {
        project_id: project_id.to_string(),
    };
    let get_response = db.get_project(get_request.clone(), creator).unwrap();
    let ProjectOverview { user_ids, .. } = get_response.project.unwrap();

    assert_eq!(5, user_ids.len());

    // Remove two random users from project
    let sample_user_ids = rnd_user_ids.iter().choose_multiple(&mut rng, 2);
    let mut removed_user_ids = Vec::new();
    for user_id in sample_user_ids {
        let remove_user_request = RemoveUserFromProjectRequest {
            project_id: project_id.to_string(),
            user_id: user_id.to_string(),
        };
        db.remove_user_from_project(remove_user_request, creator)
            .unwrap();
        removed_user_ids.push(user_id);
    }

    // Validate removed users
    let get_response = db.get_project(get_request, creator).unwrap();
    let ProjectOverview { user_ids, .. } = get_response.project.unwrap();

    assert_eq!(3, user_ids.len());
    for removed_id in removed_user_ids {
        assert!(!user_ids.contains(removed_id));
    }
}

#[test]
#[ignore]
#[serial(db)]
fn edit_project_user_permissions_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let creator = common::functions::get_admin_user_ulid();

    // Register and activate a random user
    let register_user_request = RegisterUserRequest {
        display_name: "Test".to_string(),
        email: "".to_string(),
        project: "whatever".to_string(),
    };
    let user_id = db
        .register_user(
            register_user_request,
            "Yep. It is a random user.".to_string(),
        )
        .unwrap()
        .user_id;
    db.activate_user(ActivateUserRequest {
        user_id: user_id.clone(),
        project_perms: None,
    })
    .unwrap();

    // Create project
    let create_request = CreateProjectRequest {
        name: "edit-project-user-permissions-test-project".to_string(),
        description: "Project created in edit_project_user_permissions_test()".to_string(),
    };
    let create_response = db.create_project(create_request, creator).unwrap();

    // Validate project creation
    let project_id = diesel_ulid::DieselUlid::from_str(&create_response.project_id).unwrap();
    assert!(!project_id.to_string().is_empty());

    // Add user to project as admin
    let admin_permission = ProjectPermission {
        user_id: user_id.to_string(),
        project_id: project_id.to_string(),
        permission: 5,
        service_account: false,
    };
    let user_add_request = AddUserToProjectRequest {
        project_id: project_id.to_string(),
        user_permission: Some(admin_permission.clone()),
    };
    db.add_user_to_project(user_add_request, creator).unwrap();

    // Validate users project permission
    let get_user_response = db
        .get_user(diesel_ulid::DieselUlid::from_str(user_id.as_str()).unwrap())
        .unwrap();
    assert!(get_user_response
        .project_permissions
        .contains(&admin_permission));

    // Update users project permission to read only
    let read_permission = ProjectPermission {
        user_id: user_id.to_string(),
        project_id: project_id.to_string(),
        permission: 2,
        service_account: false,
    };
    let edit_permission_request = EditUserPermissionsForProjectRequest {
        project_id: project_id.to_string(),
        user_permission: Some(read_permission.clone()),
    };

    db.edit_user_permissions_for_project(edit_permission_request, creator)
        .unwrap();

    // Validate users updated project permission
    let get_user_response = db
        .get_user(diesel_ulid::DieselUlid::from_str(user_id.as_str()).unwrap())
        .unwrap();
    assert!(get_user_response
        .project_permissions
        .contains(&read_permission));
}
