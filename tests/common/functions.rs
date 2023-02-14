use aruna_rust_api::api::internal::v1::{Location, LocationType};
use aruna_rust_api::api::storage::models::v1::{Permission, ProjectPermission};
use aruna_rust_api::api::storage::services::v1::{
    EditUserPermissionsForProjectRequest, GetObjectByIdRequest, GetReferencesRequest,
    ObjectReference, UpdateObjectRequest,
};
use aruna_rust_api::api::storage::{
    models::v1::{
        collection_overview, CollectionOverview, DataClass, Hash as ApiHash, Hashalgorithm,
        KeyValue, Object, ProjectOverview,
    },
    services::v1::{
        CreateNewCollectionRequest, CreateProjectRequest, FinishObjectStagingRequest,
        GetCollectionByIdRequest, GetProjectRequest, InitializeNewObjectRequest, StageObject,
    },
};
use aruna_server::database;
use aruna_server::database::crud::utils::grpc_to_db_object_status;
use aruna_server::database::models::enums::{EndpointType, ObjectStatus};
use diesel::{ExpressionMethods, QueryDsl, RunQueryDsl};
use rand::distributions::Uniform;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use std::collections::{hash_map::Entry, HashMap};
use std::hash::Hash;

pub fn rand_string(len: usize) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

fn rand_int(len: i64) -> i64 {
    let roll = Uniform::new_inclusive(0, len);
    thread_rng().sample_iter(roll).next().unwrap()
}

// Compare two vectors same elements ...
// Source: https://users.rust-lang.org/t/assert-vectors-equal-in-any-order/38716/11
#[allow(dead_code)]
pub fn compare_it<T: Eq + Hash>(
    i1: impl IntoIterator<Item = T>,
    i2: impl IntoIterator<Item = T>,
) -> bool {
    fn get_lookup<T: Eq + Hash>(iter: impl Iterator<Item = T>) -> HashMap<T, usize> {
        let mut lookup = HashMap::<T, usize>::new();
        for value in iter {
            match lookup.entry(value) {
                Entry::Occupied(entry) => {
                    *entry.into_mut() += 1;
                }
                Entry::Vacant(entry) => {
                    entry.insert(0);
                }
            }
        }
        lookup
    }
    get_lookup(i1.into_iter()) == get_lookup(i2.into_iter())
}

#[allow(dead_code)]
pub fn create_project(creator_id: Option<String>) -> ProjectOverview {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    let creator = if let Some(c_id) = creator_id {
        uuid::Uuid::parse_str(&c_id).unwrap()
    } else {
        uuid::Uuid::parse_str("12345678-1234-1234-1234-111111111111").unwrap()
    };

    let project_name = rand_string(30);
    let project_description = rand_string(30); //Note: Should also be tested with special characters
    let create_request = CreateProjectRequest {
        name: project_name.clone(),
        description: project_description.clone(),
    };

    let result = db.create_project(create_request, creator).unwrap();
    // Test if project_id is parseable
    let project_id = uuid::Uuid::parse_str(&result.project_id).unwrap();
    assert!(!project_id.is_nil());

    // Query project
    let response = get_project(&result.project_id);

    //Destructure response project
    let ProjectOverview {
        id,
        name,
        description,
        ..
    } = response.clone();

    // Check if get == created
    assert_eq!(project_id.to_string(), id);
    assert_eq!(project_name, name);
    assert_eq!(project_description, description);

    response
}

#[allow(dead_code)]
pub fn get_project(project_uuid: &str) -> ProjectOverview {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    let project_id = uuid::Uuid::parse_str(project_uuid).unwrap();

    let get_request = GetProjectRequest {
        project_id: project_id.to_string(),
    };
    let response = db.get_project(get_request, uuid::Uuid::default()).unwrap();

    response.project.unwrap()
}

/// Fast track permission edit.
#[allow(dead_code)]
pub fn update_project_permission(
    project_uuid: &str,
    user_uuid: &str,
    new_perm: Permission,
) -> bool {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Validate format of provided ids
    let project_id = uuid::Uuid::parse_str(project_uuid).unwrap();
    let user_id = uuid::Uuid::parse_str(user_uuid).unwrap();
    let creator_id = uuid::Uuid::parse_str("12345678-1234-1234-1234-111111111111").unwrap();

    let edit_perm_request = EditUserPermissionsForProjectRequest {
        project_id: project_id.to_string(),
        user_permission: Some(ProjectPermission {
            user_id: user_id.to_string(),
            project_id: project_id.to_string(),
            permission: new_perm as i32,
            service_account: false,
        }),
    };

    let edit_perm_response = db.edit_user_permissions_for_project(edit_perm_request, creator_id);

    edit_perm_response.is_ok()
}

/// This struct is used to simplify the function call
/// By deriving default you can just use the parameter as in
/// to auto populate all other values
/// ```
/// TCreateCollection {
///   project_id: "asdasdasdasd".to_string(),
///   ..Default::default()
/// };
/// ```
#[derive(Clone, Default)]
pub struct TCreateCollection {
    pub project_id: String,
    pub num_labels: i64,
    pub num_hooks: i64,
    pub col_override: Option<CreateNewCollectionRequest>,
    pub creator_id: Option<String>,
}

#[allow(dead_code)]
pub fn create_collection(tccol: TCreateCollection) -> CollectionOverview {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    #[derive(Clone)]
    struct CollectionTest {
        name: String,
        col_description: String,
        labels: Vec<KeyValue>,
        hooks: Vec<KeyValue>,
        request: CreateNewCollectionRequest,
    }
    let creator = if let Some(c_id) = tccol.creator_id {
        uuid::Uuid::parse_str(&c_id).unwrap()
    } else {
        uuid::Uuid::parse_str("12345678-1234-1234-1234-111111111111").unwrap()
    };
    // Create CollectionTest object containing the Request and expected values
    let create_collection_request_test = if let Some(col_req) = tccol.col_override {
        CollectionTest {
            name: col_req.name.clone(),
            col_description: col_req.description.clone(),
            labels: col_req.labels.clone(),
            hooks: col_req.hooks.clone(),
            request: col_req,
        }
    } else {
        let col_name = rand_string(30);
        let col_description = rand_string(30);
        let labels = (0..tccol.num_labels)
            .map(|num| KeyValue {
                key: format!("label_key_{:?}_{:?}", num, rand_string(5)),
                value: format!("label_value_{:?}_{:?}", num, rand_string(5)),
            })
            .collect::<Vec<_>>();
        let hooks = (0..tccol.num_hooks)
            .map(|num| KeyValue {
                key: format!("hook_key_{:?}_{:?}", num, rand_string(5)),
                value: format!("hook_value_{:?}_{:?}", num, rand_string(5)),
            })
            .collect::<Vec<_>>();
        let req = CreateNewCollectionRequest {
            name: col_name.clone(),
            description: col_description.clone(),
            label_ontology: None,
            project_id: tccol.project_id,
            labels: labels.clone(),
            hooks: hooks.clone(),
            dataclass: 1,
        };
        CollectionTest {
            name: col_name,
            col_description,
            labels,
            hooks,
            request: req,
        }
    };
    let res = db
        .create_new_collection(create_collection_request_test.clone().request, creator)
        .unwrap();

    let get_col_resp = get_collection(res.collection_id);
    // Collection should not be public
    assert_eq!(
        get_col_resp.is_public,
        create_collection_request_test.request.dataclass == 1
    );
    // Collection should have this description
    assert_eq!(
        get_col_resp.description,
        create_collection_request_test.col_description
    );
    // Collection should have the following name
    assert_eq!(get_col_resp.name, create_collection_request_test.name);
    // Collection should not have a version
    assert_eq!(
        get_col_resp.version.clone().unwrap(),
        collection_overview::Version::Latest(true)
    );
    assert!(
        // Should be empty vec
        get_col_resp
            .label_ontology
            .clone()
            .unwrap()
            .required_label_keys
            .is_empty()
    );
    // Labels / Hooks should be the same
    assert!({
        get_col_resp
            .labels
            .eq(&create_collection_request_test.labels)
    });
    assert!({ get_col_resp.hooks.eq(&create_collection_request_test.hooks) });

    get_col_resp
}

pub fn get_collection(col_id: String) -> CollectionOverview {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Get collection by ID
    let q_col_req = GetCollectionByIdRequest {
        collection_id: col_id,
    };
    let q_col = db.get_collection_by_id(q_col_req).unwrap();

    q_col.collection.unwrap()
}

#[derive(Default)]
pub struct TCreateObject {
    pub collection_id: String,
    pub num_labels: i64,
    pub num_hooks: i64,
    pub init_hash: Option<ApiHash>,
    pub sub_path: Option<String>,
    pub creator_id: Option<String>,
    pub default_endpoint_id: Option<String>,
}

/// Creates an Object in the specified Collection.
/// Fills everything with random values.
#[allow(dead_code)]
pub fn create_object(object_info: &TCreateObject) -> Object {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let creator_id = if let Some(c_id) = &object_info.creator_id {
        uuid::Uuid::parse_str(c_id).unwrap()
    } else {
        uuid::Uuid::parse_str("12345678-1234-1234-1234-111111111111").unwrap()
    };
    let collection_id = uuid::Uuid::parse_str(object_info.collection_id.as_str()).unwrap();
    let endpoint_id = if let Some(e_id) = &object_info.default_endpoint_id {
        uuid::Uuid::parse_str(e_id).unwrap()
    } else {
        uuid::Uuid::parse_str("12345678-6666-6666-6666-999999999999").unwrap()
    };
    let sub_path = &object_info.sub_path.unwrap_or_default();

    // Initialize Object with random values
    let object_id = uuid::Uuid::new_v4();
    let object_filename = format!("DummyFile.{}", rand_string(5));
    let object_length = thread_rng().gen_range(1..1073741824);
    let upload_id = uuid::Uuid::new_v4();
    let dummy_labels = (0..object_info.num_labels)
        .map(|num| KeyValue {
            key: format!("label_key_{:?}_{:?}", num, rand_string(5)),
            value: format!("label_value_{:?}_{:?}", num, rand_string(5)),
        })
        .collect::<Vec<_>>();
    let dummy_hooks = (0..object_info.num_hooks)
        .map(|num| KeyValue {
            key: format!("hook_key_{:?}_{:?}", num, rand_string(5)),
            value: format!("hook_value_{:?}_{:?}", num, rand_string(5)),
        })
        .collect::<Vec<_>>();

    let init_request = InitializeNewObjectRequest {
        object: Some(StageObject {
            filename: object_filename.to_string(),
            content_len: object_length,
            source: None,
            dataclass: DataClass::Private as i32,
            labels: dummy_labels,
            hooks: dummy_hooks,
            sub_path: sub_path.to_string(),
        }),
        collection_id: object_info.collection_id.to_string(),
        preferred_endpoint_id: endpoint_id.to_string(),
        multipart: false,
        is_specification: false,
        hash: *object_info.init_hash,
    };

    let dummy_location = Location {
        r#type: LocationType::S3 as i32,
        bucket: collection_id.to_string(),
        path: object_id.to_string(),
    };
    let _init_response = db
        .create_object(
            &init_request,
            &creator_id,
            &dummy_location,
            upload_id.to_string(),
            endpoint_id,
            object_id,
        )
        .unwrap();

    //Note: Skipping the data upload part.
    //      Maybe will be integrated later but is also tested in the data proxy repo.

    let dummy_hash = ApiHash {
        alg: Hashalgorithm::Sha256 as i32,
        hash: rand_string(64),
    };
    let finish_request = FinishObjectStagingRequest {
        object_id: object_id.to_string(),
        upload_id: upload_id.to_string(),
        collection_id: collection_id.to_string(),
        hash: Some(dummy_hash.clone()),
        no_upload: false,
        completed_parts: vec![],
        auto_update: true,
    };

    let finish_response = db
        .finish_object_staging(&finish_request, &creator_id)
        .unwrap();
    let finished_object = finish_response.object.unwrap();

    // Validate Object creation
    assert_eq!(finished_object.id, object_id.to_string());
    assert!(matches!(
        grpc_to_db_object_status(&finished_object.status),
        ObjectStatus::AVAILABLE
    ));
    assert_eq!(finished_object.rev_number, 0);
    assert_eq!(finished_object.filename, object_filename);
    assert_eq!(finished_object.content_len, object_length);
    assert_eq!(finished_object.hash.clone().unwrap(), dummy_hash);
    assert!(finished_object.auto_update);

    finished_object
}

/// GetObjectById wrapper for simplified use in tests.
#[allow(dead_code)]
pub fn get_object(collection_id: String, object_id: String) -> Object {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    // Get Object by its unique id
    let get_request = GetObjectByIdRequest {
        collection_id,
        object_id,
        with_url: false,
    };
    let object = db.get_object(&get_request).unwrap();

    object.unwrap()
}

/// GetReferences wrapper for simplified use in tests.
#[allow(dead_code)]
pub fn get_object_references(
    collection_id: String,
    object_id: String,
    with_revisions: bool,
) -> Vec<ObjectReference> {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    let get_request = GetReferencesRequest {
        collection_id,
        object_id,
        with_revisions,
    };

    db.get_references(&get_request).unwrap().references
}

/// Helper function to get the "raw" object from database without ID
#[allow(dead_code)]
pub fn get_object_status_raw(object_id: &str) -> aruna_server::database::models::object::Object {
    use database::schema::objects::dsl::*;
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    let mut conn = db.pg_connection.get().unwrap();

    let obj_id = uuid::Uuid::parse_str(object_id).unwrap();

    objects
        .filter(database::schema::objects::id.eq(&obj_id))
        .first::<aruna_server::database::models::object::Object>(&mut conn)
        .unwrap()
}

#[derive(Default)]
pub struct TCreateUpdate {
    pub original_object: Object,
    pub collection_id: String,
    pub new_name: String,
    pub new_sub_path: Option<String>,
    pub init_hash: Option<ApiHash>,
    pub content_len: i64,
    pub num_labels: i64,
    pub num_hooks: i64,
}

// Helper function to update an object
#[allow(dead_code)]
pub fn update_object(update: &TCreateUpdate) -> Object {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");
    let creator = uuid::Uuid::parse_str("12345678-1234-1234-1234-111111111111").unwrap();
    let endpoint_id = uuid::Uuid::parse_str("12345678-6666-6666-6666-999999999999").unwrap();

    // ParseTCreateUpdate
    let dummy_labels = (0..update.num_labels)
        .map(|num| KeyValue {
            key: format!("label_key_{:?}_{:?}", num, rand_string(5)),
            value: format!("label_value_{:?}_{:?}", num, rand_string(5)),
        })
        .collect::<Vec<_>>();
    let dummy_hooks = (0..update.num_hooks)
        .map(|num| KeyValue {
            key: format!("hook_key_{:?}_{:?}", num, rand_string(5)),
            value: format!("hook_value_{:?}_{:?}", num, rand_string(5)),
        })
        .collect::<Vec<_>>();

    let update_name = if update.new_name.is_empty() {
        "This is an updated object.name".to_string()
    } else {
        update.new_name.to_string()
    };

    let update_len = if update.content_len == 0 {
        rand_int(123555)
    } else {
        update.content_len
    };

    let sub_path = update.new_sub_path.unwrap_or_default();

    // Update Object
    let updated_object_id_001 = uuid::Uuid::new_v4();
    let updated_upload_id = uuid::Uuid::new_v4();
    let updated_location = Location {
        r#type: EndpointType::S3 as i32,
        bucket: update.collection_id.to_string(),
        path: updated_object_id_001.to_string(),
    };
    let update_request = UpdateObjectRequest {
        object_id: update.original_object.id.to_string(),
        collection_id: update.collection_id.to_string(),
        object: Some(StageObject {
            filename: update_name.to_string(),
            content_len: update_len,
            source: None,
            dataclass: 2,
            labels: dummy_labels.clone(),
            hooks: dummy_hooks.clone(),
            sub_path,
        }),
        force: true,
        reupload: true,
        preferred_endpoint_id: "".to_string(),
        multi_part: false,
        is_specification: false,
        hash: *update.init_hash,
    };

    let update_response = db
        .update_object(
            &update_request,
            &Some(updated_location),
            &creator,
            endpoint_id,
            updated_object_id_001,
        )
        .unwrap();

    // Finish updated Object
    let updated_hash = ApiHash {
        alg: Hashalgorithm::Sha256 as i32,
        hash: "90d1f400137575ed06a0200be160768f7e9aaa3da547f9e7e0722ee05457f7df".to_string(),
    };
    let updated_finish_request = FinishObjectStagingRequest {
        object_id: update_response.object_id.to_string(),
        upload_id: updated_upload_id.to_string(),
        collection_id: update_response.collection_id,
        hash: Some(updated_hash.clone()),
        no_upload: false,
        completed_parts: vec![],
        auto_update: true,
    };

    let finish_update_response = db
        .finish_object_staging(&updated_finish_request, &creator)
        .unwrap();
    let updated_object = finish_update_response.object.unwrap();

    // Validate update
    assert_eq!(updated_object.id, updated_object_id_001.to_string());
    assert!(matches!(
        grpc_to_db_object_status(&updated_object.status),
        ObjectStatus::AVAILABLE
    ));
    assert_eq!(
        updated_object.rev_number,
        update.original_object.rev_number + 1
    );
    assert_eq!(updated_object.filename, update_name);
    assert_eq!(updated_object.content_len, update_len);
    assert_eq!(updated_object.hash.as_ref().unwrap().clone(), updated_hash);
    assert_eq!(updated_object.hooks, dummy_hooks);
    assert_eq!(updated_object.labels, dummy_labels);
    assert!(updated_object.auto_update);

    updated_object
}
