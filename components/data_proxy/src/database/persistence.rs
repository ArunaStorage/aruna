use super::conversions::WithGenericBytes;
use anyhow::Result;
use bytes::Bytes;
use deadpool_postgres::Client;
use diesel_ulid::DieselUlid;
use std::fmt::{Display, Formatter};

#[derive(Debug, PartialEq, Eq)]
pub struct GenericBytes {
    pub id: DieselUlid,
    pub data: Bytes,
    pub table: Table,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Table {
    Objects,
    Users,
    PubKeys,
}

impl Display for Table {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Table::Objects => write!(f, "objects"),
            Table::Users => write!(f, "users"),
            Table::PubKeys => write!(f, "pubkeys"),
        }
    }
}

pub async fn get_all<T: WithGenericBytes>(client: &Client) -> Result<Vec<T>>
where
    <T as TryFrom<GenericBytes>>::Error: std::error::Error + Send + Sync + 'static,
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

pub async fn get<T: WithGenericBytes>(id: DieselUlid, client: &Client) -> Result<T>
where
    <T as TryFrom<GenericBytes>>::Error: std::error::Error + Send + Sync + 'static,
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
