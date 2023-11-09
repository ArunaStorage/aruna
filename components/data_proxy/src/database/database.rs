use anyhow::Result;
use deadpool_postgres::{Client, Config, ManagerConfig, Pool, RecyclingMethod, Runtime};
use tokio_postgres::NoTls;

use crate::trace_err;
pub struct Database {
    connection_pool: Pool,
}

impl Database {
    #[tracing::instrument(level = "trace", skip())]
    pub async fn new() -> Result<Self> {
        let database_host = trace_err!(dotenvy::var("PERSISTENCE_DB_HOST"))?;
        let database_port = trace_err!(trace_err!(dotenvy::var("PERSISTENCE_DB_PORT"))?.parse())?;
        let database_name = trace_err!(dotenvy::var("PERSISTENCE_DB_NAME"))?;
        let database_user = trace_err!(dotenvy::var("PERSISTENCE_DB_USER"))?;
        let database_password = trace_err!(dotenvy::var("PERSISTENCE_DB_PASSWORD"))?;

        let mut cfg = Config::new();
        cfg.host = Some(database_host.to_string());
        cfg.port = Some(database_port);
        cfg.user = Some(database_user.to_string());
        cfg.password = Some(database_password.to_string());
        cfg.dbname = Some(database_name.to_string());
        cfg.manager = Some(ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        });
        let pool = trace_err!(cfg.create_pool(Some(Runtime::Tokio1), NoTls))?;

        trace_err!(Database::initialize_db(&trace_err!(pool.get().await)?).await)?;

        Ok(Database {
            connection_pool: pool,
        })
    }

    #[tracing::instrument(level = "trace", skip(client))]
    pub async fn initialize_db(client: &Client) -> Result<()> {
        trace_err!(dotenvy::from_filename(".env"))?;
        let initial = trace_err!(
            tokio::fs::read_to_string(trace_err!(dotenvy::var("PERSISTENCE_DB_SCHEMA"))?).await
        )?;
        trace_err!(client.batch_execute(&initial).await)?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn get_client(&self) -> Result<Client> {
        Ok(trace_err!(self.connection_pool.get().await)?)
    }
}
