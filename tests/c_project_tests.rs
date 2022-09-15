extern crate core;

use aruna_server::api::aruna::api::storage::services::v1::{
    DestroyProjectRequest, EditUserPermissionsForProjectRequest,
};
use aruna_server::api::aruna::api::storage::{
    models::v1::{ProjectOverview, ProjectPermission},
    services::v1::{
        ActivateUserRequest, AddUserToProjectRequest, CreateNewCollectionRequest,
        CreateProjectRequest, GetProjectCollectionsRequest, GetProjectRequest, RegisterUserRequest,
        RemoveUserFromProjectRequest, UpdateProjectRequest,
    },
};
use aruna_server::database;
use rand::seq::IteratorRandom;
use rand::Rng;
use serial_test::serial;
use std::io::{Error, ErrorKind};

#[test]
#[ignore]
#[serial(db)]
fn create_project_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let creator = uuid::Uuid::parse_str("12345678-1234-1234-1234-111111111111").unwrap();

    let request = CreateProjectRequest {
        name: "".to_string(),
        description: "".to_string(),
    };

    let response = db.create_project(request, creator).unwrap();
    let id = uuid::Uuid::parse_str(&response.project_id).unwrap();

    assert!(!id.is_nil())
}

#[test]
#[ignore]
#[serial(db)]
fn get_project_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let creator = uuid::Uuid::parse_str("12345678-1234-1234-1234-111111111111").unwrap();

    let project_name = "Test Project 001";
    let project_description = "Lorem Ipsum Dolor Description"; //Note: Should also be tested with special characters
    let create_request = CreateProjectRequest {
        name: project_name.to_owned(),
        description: project_description.to_owned(),
    };

    let result = db.create_project(create_request, creator).unwrap();
    let project_id = uuid::Uuid::parse_str(&result.project_id).unwrap();

    assert!(!project_id.is_nil());

    let get_request = GetProjectRequest {
        project_id: project_id.to_string(),
    };
    let response = db.get_project(get_request, creator).unwrap();

    //Destructure response project
    let ProjectOverview {
        id,
        name,
        description,
        ..
    } = response.project.unwrap();

    assert_eq!(project_id.to_string(), id);
    assert_eq!(project_name, name);
    assert_eq!(project_description, description);
}

#[test]
#[ignore]
#[serial(db)]
fn get_project_collections_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let creator = uuid::Uuid::parse_str("12345678-1234-1234-1234-111111111111").unwrap();
    let reader = uuid::Uuid::parse_str("12345678-1234-1234-1234-111111111111").unwrap();

    // Create project
    let create_request = CreateProjectRequest {
        name: "".to_string(),
        description: "".to_string(),
    };
    let create_response = db.create_project(create_request, creator).unwrap();

    // Validate creation
    let project_id = uuid::Uuid::parse_str(&create_response.project_id).unwrap();
    assert!(!project_id.is_nil());

    // Get collections of empty project
    let get_request = GetProjectCollectionsRequest {
        project_id: project_id.to_string(),
        page_request: None,
    };
    let get_response = db
        .get_project_collections(get_request.clone(), reader)
        .unwrap();
    assert_eq!(0, get_response.collection.len());

    // Create collection in project
    let create_collection_request = CreateNewCollectionRequest {
        name: "Project Collection 001".to_string(),
        description: "Empty test collection.".to_string(),
        project_id: project_id.to_string(),
        labels: vec![],
        hooks: vec![],
        dataclass: 1,
    };
    db.create_new_collection(create_collection_request, creator)
        .unwrap();

    // Project contains one collection
    let get_response = db.get_project_collections(get_request, reader).unwrap();

    assert_eq!(1, get_response.collection.len());
}

#[test]
#[ignore]
#[serial(db)]
fn update_project_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let creator = uuid::Uuid::parse_str("12345678-1234-1234-1234-111111111111").unwrap();

    let request = CreateProjectRequest {
        name: "".to_string(),
        description: "".to_string(),
    };

    let result = db.create_project(request, creator).unwrap();
    let project_id = uuid::Uuid::parse_str(&result.project_id).unwrap();

    assert!(!project_id.is_nil());

    // Update empty project metadata
    let updated_name = "Updated Project Name".to_string();
    let updated_description = "Updated project description".to_string();
    let update_request = UpdateProjectRequest {
        project_id: project_id.to_string(),
        name: updated_name.to_string(),
        description: updated_description.to_string(),
    };

    let update_response = db.update_project(update_request, creator).unwrap();
    let updated_project = update_response.project.unwrap();

    assert_eq!(project_id.to_string(), updated_project.id);
    assert_eq!(updated_name, updated_project.name);
    assert_eq!(updated_description, updated_project.description);
}

#[test]
#[ignore]
#[serial(db)]
fn destroy_empty_project_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let creator = uuid::Uuid::parse_str("12345678-1234-1234-1234-111111111111").unwrap();

    let create_request = CreateProjectRequest {
        name: "".to_string(),
        description: "".to_string(),
    };

    let create_response = db.create_project(create_request, creator).unwrap();
    let project_id = uuid::Uuid::parse_str(&create_response.project_id).unwrap();

    assert!(!project_id.is_nil());

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

    match get_response.project {
        Some(_) => Err(Error::new(
            ErrorKind::Other,
            "Deleted project still exists.",
        )),
        None => Ok(()),
    }
    .unwrap();
}

#[test]
#[ignore]
#[should_panic]
#[serial(db)]
fn destroy_non_empty_project_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let creator = uuid::Uuid::parse_str("12345678-1234-1234-1234-111111111111").unwrap();

    let create_request = CreateProjectRequest {
        name: "".to_string(),
        description: "".to_string(),
    };

    let create_response = db.create_project(create_request, creator).unwrap();
    let project_id = uuid::Uuid::parse_str(&create_response.project_id).unwrap();

    assert!(!project_id.is_nil());

    // Create collection in project
    let create_collection_request = CreateNewCollectionRequest {
        name: "Project Collection 001".to_string(),
        description: "Empty test collection.".to_string(),
        project_id: project_id.to_string(),
        labels: vec![],
        hooks: vec![],
        dataclass: 1,
    };
    db.create_new_collection(create_collection_request, creator)
        .unwrap();

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
    let creator = uuid::Uuid::parse_str("12345678-1234-1234-1234-111111111111").unwrap();

    // Register and activate some random users
    let mut rnd_user_ids: Vec<String> = Vec::new();
    for num in 1..5 {
        let register_user_request = RegisterUserRequest {
            display_name: format!("Random User {}", num),
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
        })
        .unwrap();
        rnd_user_ids.push(user_id)
    }

    // Create project
    let create_request = CreateProjectRequest {
        name: "".to_string(),
        description: "".to_string(),
    };
    let create_response = db.create_project(create_request, creator).unwrap();

    // Validate project creation
    let project_id = uuid::Uuid::parse_str(&create_response.project_id).unwrap();
    assert!(!project_id.is_nil());

    // Add several users to project
    let mut rng = rand::thread_rng();
    for user_id in &rnd_user_ids {
        let user_add_request = AddUserToProjectRequest {
            project_id: project_id.to_string(),
            user_permission: Some(ProjectPermission {
                user_id: user_id.to_string(),
                project_id: project_id.to_string(),
                permission: rng.gen_range(2, 5),
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
        assert!(!user_ids.contains(removed_id))
    }
}

#[test]
#[ignore]
#[serial(db)]
fn edit_project_user_permissions_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let creator = uuid::Uuid::parse_str("12345678-1234-1234-1234-111111111111").unwrap();

    // Register and activate a random user
    let register_user_request = RegisterUserRequest {
        display_name: "Random User".to_string(),
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
    })
    .unwrap();

    // Create project
    let create_request = CreateProjectRequest {
        name: "".to_string(),
        description: "".to_string(),
    };
    let create_response = db.create_project(create_request, creator).unwrap();

    // Validate project creation
    let project_id = uuid::Uuid::parse_str(&create_response.project_id).unwrap();
    assert!(!project_id.is_nil());

    // Add user to project as admin
    let admin_permission = ProjectPermission {
        user_id: user_id.to_string(),
        project_id: project_id.to_string(),
        permission: 5,
    };
    let user_add_request = AddUserToProjectRequest {
        project_id: project_id.to_string(),
        user_permission: Some(admin_permission.clone()),
    };
    db.add_user_to_project(user_add_request, creator).unwrap();

    // Validate users project permission
    let get_user_response = db
        .get_user(uuid::Uuid::parse_str(user_id.as_str()).unwrap())
        .unwrap();
    assert!(get_user_response
        .project_permissions
        .contains(&admin_permission));

    // Update users project permission to read only
    let read_permission = ProjectPermission {
        user_id: user_id.to_string(),
        project_id: project_id.to_string(),
        permission: 2,
    };
    let edit_permission_request = EditUserPermissionsForProjectRequest {
        project_id: project_id.to_string(),
        user_permission: Some(read_permission.clone()),
    };

    db.edit_user_permissions_for_project(edit_permission_request, creator)
        .unwrap();

    // Validate users updated project permission
    let get_user_response = db
        .get_user(uuid::Uuid::parse_str(user_id.as_str()).unwrap())
        .unwrap();
    assert!(get_user_response
        .project_permissions
        .contains(&read_permission));
}
