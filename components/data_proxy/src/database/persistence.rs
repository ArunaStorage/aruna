use anyhow::Result;
use bytes::Bytes;
use deadpool_postgres::Client;
use diesel_ulid::DieselUlid;
use postgres_types::{FromSql, ToSql};
use std::fmt::{Display, Formatter};

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
    TryFrom<GenericBytes<X>> + TryInto<GenericBytes<X>> + Sized + Clone
{
    fn get_table() -> Table;
    async fn upsert(&self, client: &Client) -> Result<()>
    where
        <Self as TryInto<GenericBytes<X>>>::Error: std::error::Error + Send + Sync + 'static,
    {
        let generic: GenericBytes<X> = self.clone().try_into()?;

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

    async fn get_all<T: WithGenericBytes<X>>(client: &Client) -> Result<Vec<T>>
    where
        <T as TryFrom<GenericBytes<X>>>::Error: std::error::Error + Send + Sync + 'static,
    {
        let query = format!("SELECT * FROM {};", T::get_table());
        let prepared = client.prepare(&query).await?;
        let rows = client.query(&prepared, &[]).await?;
        rows.iter()
            .map(|row| {
                Ok(T::try_from(GenericBytes {
                    id: row.get(0),
                    data: Bytes::copy_from_slice(row.get(1)),
                    table: T::get_table(),
                })?)
            })
            .collect::<Result<Vec<T>>>()
    }
    async fn get<T: WithGenericBytes<X>>(id: DieselUlid, client: &Client) -> Result<T>
    where
        <T as TryFrom<GenericBytes<X>>>::Error: std::error::Error + Send + Sync + 'static,
    {
        let query = format!("SELECT * FROM {} WHERE id = $1;", T::get_table());
        let prepared = client.prepare(&query).await?;
        let row = client.query_one(&prepared, &[&id]).await?;
        Ok(T::try_from(GenericBytes {
            id: row.get(0),
            data: Bytes::copy_from_slice(row.get(1)),
            table: T::get_table(),
        })?)
    }
    async fn delete<T: WithGenericBytes<X>>(id: DieselUlid, client: &Client) -> Result<()>
    where
        <T as TryFrom<GenericBytes<X>>>::Error: std::error::Error + Send + Sync + 'static,
    {
        let query = format!("DELETE FROM {} WHERE id = $1;", T::get_table());
        let prepared = client.prepare(&query).await?;
        client.execute(&prepared, &[&id]).await?;
        Ok(())
    }
}
