use deadpool_postgres::{Config, GenericClient, ManagerConfig, RecyclingMethod, Runtime};
use tokio_postgres::NoTls;

#[tokio::main]
pub async fn main() {
    let database_host = "localhost";
    let database_name = "test";
    let database_port = 5433;
    let database_user = "yugabyte";

    let mut cfg = Config::new();
    cfg.host = Some(database_host.to_string());
    cfg.port = Some(database_port);
    cfg.user = Some(database_user.to_string());
    cfg.dbname = Some(database_name.to_string());
    cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });
    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap();
    let client = pool.get().await.unwrap();
    let _ = client
        .execute("CREATE DATABASE $1", &[&database_name])
        .await;
    let initial = tokio::fs::read_to_string("./src/database/schema.sql")
        .await
        .unwrap();
    client.batch_execute(&initial).await.unwrap();
}
