use crate::common::{init, test_utils};
use aruna_server::database::crud::CrudDb;
use aruna_server::database::dsls::workspaces_dsl::WorkspaceTemplate;
use diesel_ulid::DieselUlid;
use postgres_types::Json;
use tokio_postgres::GenericClient;

#[tokio::test]
async fn create_template() {
    // Init
    let db = init::init_database().await;
    let client = db.get_client().await.unwrap();
    let client = client.client();
    let mut user = test_utils::new_user(vec![]);
    user.create(client).await.unwrap();

    // Create template
    let id = DieselUlid::generate();
    let mut ws_template = WorkspaceTemplate {
        id,
        name: "create_template.test".to_string(),
        description: "test".to_string(),
        owner: user.id,
        prefix: "abc".to_string(),
        hook_ids: Json(vec![DieselUlid::generate()]),
        endpoint_ids: Json(vec![DieselUlid::generate()]),
    };
    ws_template.create(client).await.unwrap();

    let created_template = WorkspaceTemplate::get(id, client).await.unwrap().unwrap();
    assert_eq!(created_template.id, id);
    assert_eq!(created_template.name, ws_template.name);
    assert_eq!(created_template.description, ws_template.description);
    assert_eq!(created_template.owner, ws_template.owner);
    assert_eq!(created_template.prefix, ws_template.prefix);
    assert_eq!(created_template.hook_ids.0, ws_template.hook_ids.0);
    assert_eq!(created_template.endpoint_ids.0, ws_template.endpoint_ids.0);
}
