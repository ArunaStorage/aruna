use anyhow::anyhow;
use anyhow::Result;
use bytes::Bytes;
use deadpool_postgres::Client;
use diesel_ulid::DieselUlid;
use postgres_types::{FromSql, ToSql};
use std::fmt::{Debug, Display, Formatter};

#[derive(Debug, PartialEq, Eq)]
pub struct GenericBytes<X: ToSql + for<'a> FromSql<'a> + Send + Sync> {
    pub id: X,
    pub data: Bytes,
    pub table: Table,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Table {
    Objects,
    Users,
    PubKeys,
    ObjectLocations,
}

impl Display for Table {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Table::Objects => write!(f, "objects"),
            Table::Users => write!(f, "users"),
            Table::PubKeys => write!(f, "pubkeys"),
            Table::ObjectLocations => write!(f, "object_locations"),
        }
    }
}

#[async_trait::async_trait]
pub trait WithGenericBytes<X: ToSql + for<'a> FromSql<'a> + Send + Sync>:
    TryFrom<GenericBytes<X>> + TryInto<GenericBytes<X>> + Clone
where
    <Self as TryFrom<GenericBytes<X>>>::Error: Debug,
    <Self as TryInto<GenericBytes<X>>>::Error: Debug,
{
    fn get_table() -> Table;
    async fn upsert(&self, client: &Client) -> Result<()> {
        let generic: GenericBytes<X> = match self.clone().try_into() {
            Ok(generic) => generic,
            Err(e) => return Err(anyhow!("Failed to convert to GenericBytes: {:?}", e)),
        };

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

    async fn get_all(client: &Client) -> Result<Vec<Self>>
    where
        Self: WithGenericBytes<X>,
    {
        let query = format!("SELECT * FROM {};", Self::get_table());
        let prepared = client.prepare(&query).await?;
        let rows = client.query(&prepared, &[]).await?;
        Ok(rows
            .iter()
            .map(|row| {
                match Self::try_from(GenericBytes {
                    id: row.get(0),
                    data: Bytes::copy_from_slice(row.get(1)),
                    table: Self::get_table(),
                }) {
                    Ok(generic) => Ok(generic),
                    Err(e) => Err(anyhow!("Failed to convert to GenericBytes {:?}", e)),
                }
            })
            .collect::<Result<Vec<Self>>>()?)
    }
    async fn get(id: &X, client: &Client) -> Result<Self>
    where
        Self: WithGenericBytes<X>,
    {
        let query = format!("SELECT * FROM {} WHERE id = $1;", Self::get_table());
        let prepared = client.prepare(&query).await?;
        let row = client.query_one(&prepared, &[&id]).await?;
        match Self::try_from(GenericBytes {
            id: row.get(0),
            data: Bytes::copy_from_slice(row.get(1)),
            table: Self::get_table(),
        }) {
            Ok(generic) => Ok(generic),
            Err(e) => Err(anyhow!("Failed to convert to GenericBytes, {:?}", e)),
        }
    }
    async fn delete(id: &X, client: &Client) -> Result<()> {
        let query = format!("DELETE FROM {} WHERE id = $1;", Self::get_table());
        let prepared = client.prepare(&query).await?;
        client.execute(&prepared, &[&id]).await?;
        Ok(())
    }

    async fn delete_all(client: &Client) -> Result<()> {
        let query = format!("DELETE FROM {};", Self::get_table());
        let prepared = client.prepare(&query).await?;
        client.execute(&prepared, &[]).await?;
        Ok(())
    }
}
