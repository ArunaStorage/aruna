use anyhow::Result;
use deadpool_postgres::{Client, Config, ManagerConfig, Pool, RecyclingMethod, Runtime};
use tokio_postgres::NoTls;
pub struct Database {
    connection_pool: Pool,
}

impl Database {
    #[tracing::instrument(level = "trace", skip())]
    pub async fn new() -> Result<Self> {
        let database_host = dotenvy::var("PERSISTENCE_DB_HOST")?;
        let database_port = dotenvy::var("PERSISTENCE_DB_PORT")?.parse()?;
        let database_name = dotenvy::var("PERSISTENCE_DB_NAME")?;
        let database_user = dotenvy::var("PERSISTENCE_DB_USER")?;
        let database_password = dotenvy::var("PERSISTENCE_DB_PASSWORD")?;

        let mut cfg = Config::new();
        cfg.host = Some(database_host.to_string());
        cfg.port = Some(database_port);
        cfg.user = Some(database_user.to_string());
        cfg.password = Some(database_password.to_string());
        cfg.dbname = Some(database_name.to_string());
        cfg.manager = Some(ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        });
        let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls)?;

        Database::initialize_db(&pool.get().await?).await?;

        Ok(Database {
            connection_pool: pool,
        })
    }

    #[tracing::instrument(level = "trace", skip(client))]
    pub async fn initialize_db(client: &Client) -> Result<()> {
        dotenvy::from_filename(".env")?;
        let initial = tokio::fs::read_to_string(dotenvy::var("PERSISTENCE_DB_SCHEMA")?).await?;
        client.batch_execute(&initial).await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn get_client(&self) -> Result<Client> {
        Ok(self.connection_pool.get().await?)
    }
}
