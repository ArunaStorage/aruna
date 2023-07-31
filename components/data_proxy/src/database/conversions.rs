use anyhow::Result;
use deadpool_postgres::Client;

use super::persistence::{GenericBytes, Table};

#[async_trait::async_trait]
pub trait WithGenericBytes: TryFrom<GenericBytes> + TryInto<GenericBytes> + Sized + Clone {
    fn get_table() -> Table;
    async fn upsert(&self, client: &Client) -> Result<()>
    where
        <Self as TryInto<GenericBytes>>::Error: std::error::Error + Send + Sync + 'static,
    {
        let generic: GenericBytes = self.clone().try_into()?;

        let query = format!(
            "INSERT INTO {} (id, data) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET data = $2;",
            Self::get_table()
        );
        let prepared = client.prepare(&query).await?;
        client
            .query(&prepared, &[&generic.id, &generic.data.to_vec()])
            .await?;
        Ok(())
    }
}
