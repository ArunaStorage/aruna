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

#[derive(Copy, Clone, Debug, FromRow, Hash, Eq)]
pub struct ObjectStats {
    pub origin_pid: DieselUlid,
    pub count: i64,
    pub size: i64,
    pub last_refresh: NaiveDateTime,
}

impl Ord for ObjectStats {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Ignore timestamp in order
        (self.origin_pid, self.count, self.size).cmp(&(other.origin_pid, other.count, other.size))
    }
}

impl PartialOrd for ObjectStats {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ObjectStats {
    fn eq(&self, other: &Self) -> bool {
        // Ignore timestamp in equality comparison
        self.origin_pid == other.origin_pid && self.count == other.count && self.size == other.size
    }
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

pub async fn start_refresh_loop(
    database: Arc<Database>,
    cache: Arc<Cache>,
    refresh_interval: i64,
    natsio_handler: Arc<NatsIoHandler>,
    refresh_receiver: Receiver<i64>,
) {
    // Start loop
    tokio::spawn(async move {
        loop {
            // Try to get database connection
            let client = match database.get_client().await {
                Ok(client) => client,
                Err(err) => {
                    error!("Failed to get database client for MV refresh: {}", err);
                    tokio::time::sleep(Duration::from_secs(15)).await; // Wait 15s and try again
                    continue;
                }
            };

            // Save current timestamp to check if refresh is necessary
            let mut current_timestamp = Utc::now().timestamp_millis();

            // Collect latest timestamp of all other started refreshs
            let mut latest_refresh = None;
            while let Ok(refresh_timestamp) = refresh_receiver.try_recv() {
                log::info!("Received other refresh: {}", refresh_timestamp);
                if let Some(latest_timestamp) = latest_refresh {
                    if refresh_timestamp > latest_timestamp {
                        latest_refresh = Some(refresh_timestamp)
                    }
                } else {
                    latest_refresh = Some(refresh_timestamp)
                }
            }
            log::info!("Latest refresh: {:?}", latest_refresh);

            // Evaluate if refresh is necessary
            let start_refresh = if let Some(refresh_timestamp) = latest_refresh {
                log::info!(
                    "Difference to last refresh: {}",
                    current_timestamp - refresh_timestamp
                );
                let start_refresh = (current_timestamp - refresh_timestamp) > refresh_interval;
                if !start_refresh {
                    current_timestamp = refresh_timestamp
                }
                start_refresh
            } else {
                true
            };
            log::info!("Start refresh in this iteration: {}", start_refresh);

            // Start MV refresh if conditions are met
            if start_refresh {
                match refresh_stats_view(&client).await {
                    Ok(_) => {
                        // Send notification that MV refresh has been started
                        if let Err(err) = natsio_handler
                            .register_server_event(ServerEvents::MVREFRESH(current_timestamp))
                            .await
                        {
                            error!("Failed to send MV refresh notification: {}", err)
                        } else {
                            log::info!(
                                "Successfully send refresh timestamp to Nats: {}",
                                current_timestamp
                            )
                        }
                    }
                    Err(err) => error!("Start MV refresh failed: {}", err),
                }
            }

            // Wait in every case for refresh to finish for cache update
            loop {
                match get_last_refresh(&client).await {
                    Ok(last_refresh) => {
                        if last_refresh.timestamp_millis() >= current_timestamp {
                            // Update stats in cache if MV has been refreshed
                            if let Err(err) = cache.upsert_object_stats(database.clone()).await {
                                error!("Object stats cache update failed: {}", err)
                            } else {
                                log::info!("Object stats cache updated")
                            };
                            break;
                        } else {
                            debug!(
                                "Refresh still running. Wait 1 sec before re-try: ({}) {} > {}",
                                last_refresh,
                                last_refresh.timestamp_millis(),
                                current_timestamp
                            );
                            tokio::time::sleep(Duration::from_secs(1)).await
                        }
                    }
                    Err(err) => {
                        // On error break to continue outer loop
                        debug!("Skipped object stats cache update: {}", err);
                        break;
                    }
                }
            }

            // Sleep for refresh interval
            tokio::time::sleep(Duration::from_millis(
                refresh_interval.try_into().unwrap_or(30000),
            ))
            .await;
        }

        //Ok::<(), anyhow::Error>(())
    });
}
