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
    )?;
    db.initialize_db().await?;

    // let notifications_cache = Arc::new(
    //     aruna_cache::notifications::NotificationCache::new(
    //         "a_token",
    //         "a_server",
    //         Box::new(aruna_server::middlelayer::query_handler::DBQueryHandler {}),
    //     )
    //     .await?,
    // );

    // let _authorizer =
    //     aruna_policy::ape::policy_evaluator::PolicyEvaluator::new("", notifications_cache.clone())
    //         .await?;

    Ok(())
}
