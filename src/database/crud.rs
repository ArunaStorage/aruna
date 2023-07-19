use anyhow::Result;
use diesel_ulid::DieselUlid;
use postgres_types::ToSql;
use tokio_postgres::Client;

pub trait PrimaryKey: ToSql + Send + Sync {
    fn get_key(&self) -> &Self {
        &self
    }
}

impl PrimaryKey for DieselUlid {}
impl PrimaryKey for i32 {}

#[async_trait::async_trait]
pub trait CrudDb: Sized {
    async fn create(&self, client: &Client) -> Result<()>;
    async fn get(id: impl PrimaryKey, client: &Client) -> Result<Option<Self>>;
    async fn all(client: &Client) -> Result<Vec<Self>>;
    async fn delete(&self, client: &Client) -> Result<()>;
}
