use crate::trace_err;
use anyhow::anyhow;
use anyhow::Result;
use deadpool_postgres::Client;
use postgres_types::{FromSql, ToSql};
use std::fmt::{Debug, Display, Formatter};

#[derive(Debug, PartialEq, Eq)]
pub struct GenericBytes<X: ToSql + for<'a> FromSql<'a> + Send + Sync> {
    pub id: X,
    pub data: Vec<u8>,
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
    #[tracing::instrument(level = "trace", skip(self, f))]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Table::Objects => write!(f, "objects"),
            Table::Users => write!(f, "users"),
            Table::PubKeys => write!(f, "pub_keys"),
            Table::ObjectLocations => write!(f, "object_locations"),
        }
    }
}

#[async_trait::async_trait]
pub trait WithGenericBytes<X: ToSql + for<'a> FromSql<'a> + Send + Sync>:
    TryFrom<GenericBytes<X>> + TryInto<GenericBytes<X>> + Clone
where
    <Self as TryFrom<GenericBytes<X>>>::Error: Debug + Display,
    <Self as TryInto<GenericBytes<X>>>::Error: Debug + Display,
{
    fn get_table() -> Table;
    async fn upsert(&self, client: &Client) -> Result<()> {
        let generic: GenericBytes<X> = match trace_err!(self.clone().try_into()) {
            Ok(generic) => generic,
            Err(e) => return Err(anyhow!("Failed to convert to GenericBytes: {:?}", e)),
        };

        let query = format!(
            "INSERT INTO {} (id, data) VALUES ($1, $2::BYTEA) ON CONFLICT (id) DO UPDATE SET data = $2;",
            Self::get_table()
        );
        let prepared = trace_err!(client.prepare(&query).await)?;
        trace_err!(
            client
                .query(&prepared, &[&generic.id, &generic.data.to_vec().as_slice()])
                .await
        )?;
        Ok(())
    }

    async fn get_all(client: &Client) -> Result<Vec<Self>>
    where
        Self: WithGenericBytes<X>,
    {
        let query = format!("SELECT * FROM {};", Self::get_table());
        let prepared = trace_err!(client.prepare(&query).await)?;
        let rows = trace_err!(client.query(&prepared, &[]).await)?;
        Ok(rows
            .iter()
            .map(|row| {
                match trace_err!(Self::try_from(GenericBytes {
                    id: row.get::<&str, X>("id"),
                    data: row.get("data"),
                    table: Self::get_table(),
                })) {
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
        let prepared = trace_err!(client.prepare(&query).await)?;
        let row = trace_err!(client.query_one(&prepared, &[&id]).await)?;
        match trace_err!(Self::try_from(GenericBytes {
            id: row.get::<usize, X>(0),
            data: row.get(1),
            table: Self::get_table(),
        })) {
            Ok(generic) => Ok(generic),
            Err(e) => Err(anyhow!("Failed to convert to GenericBytes, {:?}", e)),
        }
    }

    async fn get_opt(id: &X, client: &Client) -> Result<Option<Self>>
    where
        Self: WithGenericBytes<X>,
    {
        let query = format!("SELECT * FROM {} WHERE id = $1;", Self::get_table());
        let prepared = trace_err!(client.prepare(&query).await)?;
        let row = trace_err!(client.query_opt(&prepared, &[&id]).await)?;

        match row {
            Some(row) => {
                match trace_err!(Self::try_from(GenericBytes {
                    id: row.get::<usize, X>(0),
                    data: row.get(1),
                    table: Self::get_table(),
                })) {
                    Ok(generic) => Ok(Some(generic)),
                    Err(e) => Err(anyhow!("Failed to convert to GenericBytes, {:?}", e)),
                }
            }
            None => Ok(None),
        }
    }

    async fn delete(id: &X, client: &Client) -> Result<()> {
        let query = format!("DELETE FROM {} WHERE id = $1;", Self::get_table());
        let prepared = trace_err!(client.prepare(&query).await)?;
        trace_err!(client.execute(&prepared, &[&id]).await)?;
        Ok(())
    }

    async fn delete_all(client: &Client) -> Result<()> {
        let query = format!("DELETE FROM {};", Self::get_table());
        let prepared = trace_err!(client.prepare(&query).await)?;
        trace_err!(client.execute(&prepared, &[]).await)?;
        Ok(())
    }
}
