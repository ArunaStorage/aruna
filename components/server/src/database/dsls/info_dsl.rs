use crate::database::crud::{CrudDb, PrimaryKey};
use crate::utils::database_utils::create_multi_query;
use chrono::NaiveDateTime;
use diesel_ulid::DieselUlid;
use postgres_from_row::FromRow;
use postgres_types::ToSql;
use tokio_postgres::Client;

#[derive(FromRow, Debug, Clone)]
pub struct Announcement {
    pub id: DieselUlid,
    pub announcement_type: String,
    pub title: String,
    pub content: String,
    pub created_by: DieselUlid,
    pub created_at: NaiveDateTime,
    pub last_modified_by: DieselUlid,
    pub last_modified_at: NaiveDateTime,
}

#[async_trait::async_trait]
impl CrudDb for Announcement {
    async fn create(&mut self, client: &Client) -> anyhow::Result<()> {
        let query = "INSERT INTO announcements (id, announcement_type, title, content, created_by, created_at, last_modified_by, last_modified_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING *;";

        let prepared = client.prepare(query).await?;

        let row = client
            .query_one(
                &prepared,
                &[
                    &self.id,
                    &self.announcement_type,
                    &self.title,
                    &self.content,
                    &self.created_by,
                    &self.created_at,
                    &self.last_modified_by,
                    &self.last_modified_at,
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
            .query_opt(&prepared, &[&id])
            .await?
            .map(|e| Announcement::from_row(&e)))
    }

    async fn all(client: &Client) -> anyhow::Result<Vec<Self>> {
        let query = "SELECT * FROM announcements;";
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
        let query = "INSERT INTO announcements (id, announcement_type, title, content, created_by, created_at, last_modified_by, last_modified_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT(id) 
            DO UPDATE SET
              announcement_type = EXCLUDED.announcement_type,
              title = EXCLUDED.title,
              content = EXCLUDED.content,
              last_modified_by = $7,
              last_modified_at = $6
            RETURNING *;";

        let prepared = client.prepare(&query).await?;

        let row = client
            .query_one(
                &prepared,
                &[
                    &self.id,
                    &self.announcement_type,
                    &self.title,
                    &self.content,
                    &self.created_by,
                    &self.created_at,
                    &self.last_modified_by,
                    &self.last_modified_at,
                ],
            )
            .await?;

        Ok(Announcement::from_row(&row))
    }

    pub async fn get_by_ids(
        client: &Client,
        ids: &Vec<DieselUlid>,
    ) -> anyhow::Result<Vec<Announcement>> {
        // Fast return if no ids are provided
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let query_one = "SELECT * FROM announcements WHERE announcements.id IN ";
        let mut inserts = Vec::<&(dyn ToSql + Sync)>::new();
        for id in ids {
            inserts.push(id);
        }
        let query_insert = create_multi_query(&inserts);
        let query = format!("{query_one}{query_insert};");
        let prepared = client.prepare(&query).await?;
        Ok(client
            .query(&prepared, &inserts)
            .await?
            .iter()
            .map(Announcement::from_row)
            .collect())
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
