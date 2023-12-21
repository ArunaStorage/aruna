use std::{sync::Arc, time::Duration};

use anyhow::{bail, Result};
use async_channel::Receiver;
use chrono::{NaiveDateTime, Utc};
use diesel_ulid::DieselUlid;
use itertools::Itertools;
use log::{debug, error};
use postgres_from_row::FromRow;
use tokio_postgres::Client;

use crate::{
    caching::cache::Cache,
    database::connection::Database,
    notification::natsio_handler::{NatsIoHandler, ServerEvents},
};

#[derive(Copy, Clone, FromRow, Hash, PartialEq, Eq)]
pub struct ObjectStats {
    pub origin_pid: DieselUlid,
    pub count: i64,
    pub size: i64,
    pub last_refresh: NaiveDateTime,
}

impl ObjectStats {
    pub async fn get_object_stats(id: DieselUlid, client: &Client) -> Result<Self> {
        let query = "SELECT * FROM object_stats WHERE id = $1;";
        let prepared = client.prepare(query).await?;

        let stats = match client.query_opt(&prepared, &[&id]).await? {
            Some(row) => ObjectStats::from_row(&row),
            None => ObjectStats {
                origin_pid: id,
                count: 0,
                size: 0,
                last_refresh: NaiveDateTime::default(),
            },
        };

        Ok(stats)
    }

    pub async fn get_all_stats(client: &Client) -> Result<Vec<Self>> {
        let query = "SELECT * FROM object_stats;";
        let prepared = client.prepare(query).await?;

        let rows = client.query(&prepared, &[]).await?;

        let stats = rows
            .iter()
            .map(|row| ObjectStats::from_row(&row))
            .collect_vec();

        Ok(stats)
    }
}

pub async fn refresh_stats_view(client: &Client) -> Result<()> {
    let query = "REFRESH MATERIALIZED VIEW CONCURRENTLY object_stats;";
    let prepared = client.prepare(query).await?;

    client.execute(&prepared, &[]).await?;

    Ok(())
}

pub async fn get_last_refresh(client: &Client) -> Result<NaiveDateTime> {
    let query = "SELECT last_refresh FROM object_stats LIMIT 1;";
    let prepared = client.prepare(query).await?;

    let last_refreshed: NaiveDateTime = match client.query_opt(&prepared, &[]).await? {
        Some(row) => row.get("last_refresh"),
        None => bail!("Object stats table is empty"),
    };

    Ok(last_refreshed)
}

