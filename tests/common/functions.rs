use aruna_rust_api::api::storage::{
    models::v1::{collection_overview, CollectionOverview, KeyValue, ProjectOverview},
    services::v1::{
        CreateNewCollectionRequest, CreateProjectRequest, GetCollectionByIdRequest,
        GetProjectRequest,
    },
};
use aruna_server::database;
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
/// TCreateCollection{project_id: "asdasdasdasd".to_string(), ..Default::default()};
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
