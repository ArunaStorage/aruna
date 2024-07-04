use crate::database::crud::{CrudDb, PrimaryKey};
use crate::utils::database_utils::create_multi_query;
use aruna_rust_api::api::storage::models::v2::PageRequest;
use chrono::NaiveDateTime;
use diesel_ulid::DieselUlid;
use postgres_from_row::FromRow;
use postgres_types::ToSql;
use std::str::FromStr;
use tokio_postgres::Client;

#[derive(FromRow, Debug, Clone, PartialEq, Eq)]
pub struct Announcement {
    pub id: DieselUlid,
    pub announcement_type: String,
    pub title: String,
    pub teaser: String,
    pub image_url: String,
    pub content: String,
    pub created_by: String,
    pub created_at: NaiveDateTime,
    pub modified_by: String,
    pub modified_at: NaiveDateTime,
}

#[async_trait::async_trait]
impl CrudDb for Announcement {
    async fn create(&mut self, client: &Client) -> anyhow::Result<()> {
        let query = "INSERT INTO announcements 
            (id, announcement_type, title, teaser, image_url, content, created_by, created_at, modified_by, modified_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING *;";

        let prepared = client.prepare(query).await?;

        let row = client
            .query_one(
                &prepared,
                &[
                    &self.id,
                    &self.announcement_type,
                    &self.title,
                    &self.teaser,
                    &self.image_url,
                    &self.content,
                    &self.created_by,
                    &self.created_at,
                    &self.modified_by,
                    &self.modified_at,
                ],
            )
            .await?;

        *self = Announcement::from_row(&row);
        Ok(())
    }

    async fn get(id: impl PrimaryKey, client: &Client) -> anyhow::Result<Option<Self>> {
        let query = "SELECT * FROM announcements WHERE id = $1;";
        let prepared = client.prepare(query).await?;
        Ok(client
            .query_opt(&prepared, &[&&id])
            .await?
            .map(|e| Announcement::from_row(&e)))
    }

    async fn all(client: &Client) -> anyhow::Result<Vec<Self>> {
        let query = "SELECT * FROM announcements ORDER BY created_at;";
        let prepared = client.prepare(query).await?;
        let rows = client.query(&prepared, &[]).await?;
        Ok(rows.iter().map(Announcement::from_row).collect::<Vec<_>>())
    }

    async fn delete(&self, client: &Client) -> anyhow::Result<()> {
        let query = "DELETE FROM announcements WHERE id = $1;";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&self.id]).await?;
        Ok(())
    }
}

impl Announcement {
    pub async fn upsert(&self, client: &Client) -> anyhow::Result<Announcement> {
        let query = "INSERT INTO announcements 
              (id, announcement_type, title, teaser, image_url, content, created_by, created_at, modified_by, modified_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT(id) 
            DO UPDATE SET
              announcement_type = EXCLUDED.announcement_type,
              title = EXCLUDED.title,
              teaser = EXCLUDED.teaser,
              image_url = EXCLUDED.image_url,
              content = EXCLUDED.content,
              modified_by = $9,
              modified_at = $10
            RETURNING *;";

        let prepared = client.prepare(query).await?;

        let row = client
            .query_one(
                &prepared,
                &[
                    &self.id,
                    &self.announcement_type,
                    &self.title,
                    &self.teaser,
                    &self.image_url,
                    &self.content,
                    &self.created_by,
                    &self.created_at,
                    &self.modified_by,
                    &self.modified_at,
                ],
            )
            .await?;

        Ok(Announcement::from_row(&row))
    }

    pub async fn all_paginated(
        client: &Client,
        page: Option<PageRequest>,
    ) -> anyhow::Result<Vec<Announcement>> {
        let mut query = "SELECT * FROM announcements ".to_string();
        let mut inserts = Vec::<&(dyn ToSql + Sync)>::new();

        //Note: This is the verbose version as tokio_postgres query params are not std::marker::send
        let rows = if let Some(page) = page {
            if let Ok(start_after) = DieselUlid::from_str(&page.start_after) {
                inserts.push(&start_after);
                query.push_str(&format!("WHERE id > ${} ", inserts.len()));

                if page.page_size > 0 {
                    inserts.push(&page.page_size);
                    query.push_str(&format!(" ORDER BY created_at LIMIT ${};", inserts.len()));
                } else {
                    query.push_str(" ORDER BY created_at;");
                }

                let prepared = client.prepare(&query).await?;
                client.query(&prepared, &inserts).await?
            } else {
                if page.page_size > 0 {
                    inserts.push(&page.page_size);
                    query.push_str(&format!(" ORDER BY created_at LIMIT ${};", inserts.len()));
                } else {
                    query.push_str(" ORDER BY created_at;");
                }

                let prepared = client.prepare(&query).await?;
                client.query(&prepared, &inserts).await?
            }
        } else {
            query.push_str(" ORDER BY created_at;");
            let prepared = client.prepare(&query).await?;
            client.query(&prepared, &inserts).await?
        };

        Ok(rows.iter().map(Announcement::from_row).collect::<Vec<_>>())
    }

    pub async fn get_by_ids(
        client: &Client,
        ids: &Vec<DieselUlid>,
        page: Option<PageRequest>,
    ) -> anyhow::Result<Vec<Announcement>> {
        // Fast return if no ids are provided
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        // Prepare and execute query
        let mut query = "SELECT * FROM announcements WHERE id IN ".to_string();
        let mut inserts = Vec::<&(dyn ToSql + Sync)>::new();
        for id in ids {
            inserts.push(id);
        }
        query.push_str(&create_multi_query(&inserts));

        //Note: This is the verbose version as tokio_postgres query params are not std::marker::send
        let rows = if let Some(page) = page {
            if let Ok(start_after) = DieselUlid::from_str(&page.start_after) {
                inserts.push(&start_after);
                query.push_str(&format!(" AND id > ${}", inserts.len()));

                if page.page_size > 0 {
                    inserts.push(&page.page_size);
                    query.push_str(&format!(" ORDER BY created_at LIMIT ${};", inserts.len()));
                } else {
                    query.push_str(" ORDER BY created_at;");
                }

                let prepared = client.prepare(&query).await?;
                client.query(&prepared, &inserts).await?
            } else {
                if page.page_size > 0 {
                    inserts.push(&page.page_size);
                    query.push_str(&format!(" ORDER BY created_at LIMIT ${};", inserts.len()));
                } else {
                    query.push_str(" ORDER BY created_at;");
                }

                let prepared = client.prepare(&query).await?;
                client.query(&prepared, &inserts).await?
            }
        } else {
            query.push_str(" ORDER BY created_at;");
            let prepared = client.prepare(&query).await?;
            client.query(&prepared, &inserts).await?
        };

        /* Could be as compact as this
        let (query, params) = SelectQueryBuilder::new_with_table("announcements")
            .with_id_filter(ids)
            .with_pagination_opt(page)
            .build();

        let prepared = client.prepare(&query).await?;
        let param_slice = &params.iter().map(|x| x.as_ref()).collect::<Vec<_>>().as_slice();
        let rows = client.query(&prepared, &param_slice).await?
        */

        Ok(rows.iter().map(Announcement::from_row).collect())
    }

    pub async fn get_by_type(
        client: &Client,
        announcement_type: String,
        page: Option<PageRequest>,
    ) -> anyhow::Result<Vec<Announcement>> {
        // Fast return if no type is provided
        if announcement_type.is_empty() {
            return Ok(Vec::new());
        }

        // Prepare and execute query
        let mut query = "SELECT * FROM announcements WHERE announcement_type = $1".to_string();
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![&announcement_type];

        //Note: This is the verbose version as tokio_postgres query params are not std::marker::send
        let rows = if let Some(page) = page {
            if let Ok(start_after) = DieselUlid::from_str(&page.start_after) {
                params.push(&start_after);
                query.push_str(&format!(" AND id > ${}", params.len()));

                if page.page_size > 0 {
                    params.push(&page.page_size);
                    query.push_str(&format!(" ORDER BY created_at LIMIT ${};", params.len()));
                } else {
                    query.push_str(" ORDER BY created_at;");
                }

                let prepared = client.prepare(&query).await?;
                client.query(&prepared, &params).await?
            } else {
                if page.page_size > 0 {
                    params.push(&page.page_size);
                    query.push_str(&format!(" ORDER BY created_at LIMIT ${};", params.len()));
                } else {
                    query.push_str(" ORDER BY created_at;");
                }

                let prepared = client.prepare(&query).await?;
                client.query(&prepared, &params).await?
            }
        } else {
            query.push_str(" ORDER BY created_at;");
            let prepared = client.prepare(&query).await?;
            client.query(&prepared, &params).await?
        };

        Ok(rows.iter().map(Announcement::from_row).collect::<Vec<_>>())
    }

    pub async fn batch_delete(client: &Client, ids: &Vec<DieselUlid>) -> anyhow::Result<()> {
        // Fast return if no ids are provided
        if ids.is_empty() {
            return Ok(());
        }

        let base_query = "DELETE FROM announcements WHERE announcements.id IN ";
        let mut deletes = Vec::<&(dyn ToSql + Sync)>::new();
        for id in ids {
            deletes.push(id);
        }
        let included_deletions = create_multi_query(&deletes);
        let query = format!("{base_query}{included_deletions};");
        let prepared = client.prepare(&query).await?;

        client.execute(&prepared, &deletes).await?;
        Ok(())
    }
}
