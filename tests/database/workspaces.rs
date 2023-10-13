use crate::common::{init, test_utils};
use aruna_server::database::crud::CrudDb;
use aruna_server::database::dsls::workspaces_dsl::WorkspaceTemplate;
use diesel_ulid::DieselUlid;
use postgres_types::Json;
use tokio_postgres::GenericClient;

#[tokio::test]
async fn test_db_calls() {
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

    // Get
    let created_template = WorkspaceTemplate::get(id, client).await.unwrap().unwrap();
    assert_eq!(created_template.id, id);
    assert_eq!(created_template.name, ws_template.name);
    assert_eq!(created_template.description, ws_template.description);
    assert_eq!(created_template.owner, ws_template.owner);
    assert_eq!(created_template.prefix, ws_template.prefix);
    assert_eq!(created_template.hook_ids.0, ws_template.hook_ids.0);
    assert_eq!(created_template.endpoint_ids.0, ws_template.endpoint_ids.0);
    let by_name = WorkspaceTemplate::get_by_name("create_template.test".to_string(), &client)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(by_name.id, id);
    assert_eq!(by_name.name, ws_template.name);
    assert_eq!(by_name.description, ws_template.description);
    assert_eq!(by_name.owner, ws_template.owner);
    assert_eq!(by_name.prefix, ws_template.prefix);
    assert_eq!(by_name.hook_ids.0, ws_template.hook_ids.0);
    assert_eq!(by_name.endpoint_ids.0, ws_template.endpoint_ids.0);
    let by_owner = WorkspaceTemplate::list_owned(&user.id, &client)
        .await
        .unwrap();
    assert_eq!(by_owner.len(), 1);

    // Delete
    ws_template.delete(&client).await.unwrap();

    assert!(WorkspaceTemplate::get(id, &client).await.unwrap().is_none());
}
