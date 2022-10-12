mod common;

use crate::common::functions::{
    create_collection, create_object, create_project, TCreateCollection, TCreateObject,
};

use aruna_rust_api::api::storage::services::v1::{
    CreateObjectGroupRequest, GetObjectGroupByIdRequest, GetObjectGroupObjectsRequest,
};
use aruna_server::database;
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use serial_test::serial;

#[test]
#[ignore]
#[serial(db)]
fn create_object_group_test() {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let creator = uuid::Uuid::parse_str("12345678-1234-1234-1234-111111111111").unwrap();
    let endpoint_id = uuid::Uuid::parse_str("12345678-6666-6666-6666-999999999999").unwrap();

    // Create random project
    let random_project = create_project(None);

    // Create random collection
    let random_collection = create_collection(TCreateCollection {
        project_id: random_project.id,
        col_override: None,
        ..Default::default()
    });

    // Create 5 random objects
    let mut object_ids = (0..5)
        .map(|_| {
            create_object(&TCreateObject {
                creator_id: Some(creator.to_string()),
                collection_id: random_collection.id.to_string(),
                default_endpoint_id: Some(endpoint_id.to_string()),
                num_labels: thread_rng().gen_range(0, 4),
                num_hooks: thread_rng().gen_range(0, 4),
            })
            .id
        })
        .collect::<Vec<_>>();

    // Draw random sample from ids and remove element from vector
    let meta_object = object_ids.choose(&mut thread_rng()).unwrap().clone();
    object_ids.retain(|id| id != &meta_object);

    // Create ObjectGroup
    let object_group_name = "DummyGroup";
    let object_group_description = "Description of a dummy group.";
    let create_request = CreateObjectGroupRequest {
        name: object_group_name.to_string(),
        description: object_group_description.to_string(),
        collection_id: random_collection.id.to_string(),
        object_ids: object_ids.clone(),
        meta_object_ids: vec![meta_object.clone()],
        labels: vec![],
        hooks: vec![],
    };

    let create_response = db.create_object_group(&create_request, &creator).unwrap();
    let object_group = create_response.object_group.unwrap();

    // Validate Object Group Creation
    assert_eq!(object_group.name, object_group_name.to_string());
    assert_eq!(
        object_group.description,
        object_group_description.to_string()
    );
    assert_eq!(object_group.rev_number, 0);
    assert_eq!(object_group.labels, vec!());
    assert_eq!(object_group.hooks, vec!());

    // Get Object Group
    let get_group_request = GetObjectGroupByIdRequest {
        group_id: object_group.id.to_string(),
        collection_id: random_collection.id.to_string(),
    };

    let get_group_response = db.get_object_group_by_id(&get_group_request).unwrap();
    let group_overview = get_group_response.object_group.unwrap();

    assert_eq!(group_overview.name, object_group_name.to_string());
    assert_eq!(
        group_overview.description,
        object_group_description.to_string()
    );
    assert_eq!(group_overview.rev_number, 0);
    assert_eq!(group_overview.labels, vec!());
    assert_eq!(group_overview.hooks, vec!());

    // Get all Objects in ObjectGroup
    let get_all_request = GetObjectGroupObjectsRequest {
        collection_id: random_collection.id.to_string(),
        group_id: object_group.id.to_string(),
        page_request: None,
        meta_only: false,
    };
    let get_meta_request = GetObjectGroupObjectsRequest {
        collection_id: random_collection.id.to_string(),
        group_id: object_group.id.to_string(),
        page_request: None,
        meta_only: true,
    };

    let all_object_group_objects = db.get_object_group_objects(get_all_request).unwrap();
    let meta_object_group_objects = db.get_object_group_objects(get_meta_request).unwrap();

    assert_eq!(all_object_group_objects.object_group_objects.len(), 5);
    assert_eq!(meta_object_group_objects.object_group_objects.len(), 1);

    let all_object_ids = all_object_group_objects
        .object_group_objects
        .iter()
        .filter(|o| o.is_metadata == false)
        .map(|o| o.object.clone().unwrap().id)
        .collect::<Vec<_>>();

    for id in object_ids {
        assert!(all_object_ids.contains(&id));
    }

    for ogo in meta_object_group_objects.object_group_objects {
        let object = ogo.object.unwrap();
        assert_eq!(object.id, meta_object)
    }
}
