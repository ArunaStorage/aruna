use aruna_server::database::connection::Database;
use aruna_server::middlelayer::db_handler::DatabaseHandler;
use aruna_server::notification::natsio_handler::NatsIoHandler;
use std::sync::Arc;

#[allow(dead_code)]
pub async fn init_db() -> Database {
    let database_host = "localhost".to_string();
    let database_name = "test".to_string();
    let database_port = 5433;
    let database_user = "yugabyte".to_string();
    Database::new(database_host, database_port, database_name, database_user).unwrap()
    //db.initialize_db().await.unwrap();
}

#[allow(dead_code)]
pub async fn init_handler() -> DatabaseHandler {
    // init
    let nats_client = async_nats::connect("0.0.0.0:4222").await.unwrap();
    let nats_handler = NatsIoHandler::new(nats_client, "ThisIsASecretToken".to_string(), None)
        .await
        .unwrap();
    let database = Arc::new(init_db().await);
    DatabaseHandler {
        database,
        natsio_handler: Arc::new(nats_handler),
    }
}
