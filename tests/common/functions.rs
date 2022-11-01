use aruna_rust_api::api::internal::v1::{Location, LocationType};
use aruna_rust_api::api::storage::services::v1::GetObjectByIdRequest;
use aruna_rust_api::api::storage::{
    models::v1::{
        collection_overview, CollectionOverview, DataClass, Hash, Hashalgorithm, KeyValue, Object,
        ProjectOverview,
    },
    services::v1::{
        CreateNewCollectionRequest, CreateProjectRequest, FinishObjectStagingRequest,
        GetCollectionByIdRequest, GetProjectRequest, InitializeNewObjectRequest, StageObject,
    },
};
use aruna_server::database;
use aruna_server::database::crud::utils::grpc_to_db_object_status;
use aruna_server::database::models::enums::ObjectStatus;
use diesel::{ExpressionMethods, QueryDsl, RunQueryDsl};
use rand::{distributions::Alphanumeric, thread_rng, Rng};

fn rand_string(len: usize) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

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

pub fn get_project(project_uuid: &str) -> ProjectOverview {
    let db = database::connection::Database::new("postgres://root:test123@localhost:26257/test");

    let project_id = uuid::Uuid::parse_str(project_uuid).unwrap();

    let get_request = GetProjectRequest {
        project_id: project_id.to_string(),
    };
    let response = db.get_project(get_request, uuid::Uuid::default()).unwrap();

    response.project.unwrap()
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
#[derive(Default)]
pub struct TCreateCollection {
    pub project_id: String,
    pub num_labels: i64,
    pub num_hooks: i64,
    pub col_override: Option<CreateNewCollectionRequest>,
    pub creator_id: Option<String>,
}

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
    assert!(get_col_resp.version.clone().unwrap() == collection_overview::Version::Latest(true));
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
    pub creator_id: Option<String>,
    pub collection_id: String,
    pub default_endpoint_id: Option<String>,
    pub num_labels: i64,
    pub num_hooks: i64,
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
        uuid::Uuid::parse_str("12345678-1234-1234-1234-111111111111").unwrap()
    };

    // Initialize Object with random values
    let object_id = uuid::Uuid::new_v4();
    let object_filename = format!("DummyFile.{}", rand_string(5));
    let object_description = rand_string(30);
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
            description: object_description,
            collection_id: collection_id.to_string(),
            content_len: object_length,
            source: None,
            dataclass: DataClass::Private as i32,
            labels: dummy_labels,
            hooks: dummy_hooks,
        }),
        collection_id: object_info.collection_id.to_string(),
        preferred_endpoint_id: endpoint_id.to_string(),
        multipart: false,
        is_specification: false,
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

    let dummy_hash = Hash {
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
pub fn _get_object(collection_id: String, object_id: String) -> Object {
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
