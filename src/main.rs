use anyhow::Result;
use aruna_server::database;

#[tokio::main]
pub async fn main() -> Result<()> {
    let database_host = "localhost";
    let database_name = "test";
    let database_port = 5433;
    let database_user = "yugabyte";

    let db = database::connection::Database::new(
        database_host,
        database_port,
        database_name,
        database_user,
    );
    db.initialize_db().await?;
    Ok(())
}
