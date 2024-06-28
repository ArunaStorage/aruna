use crate::database::crud::CrudDb;
use crate::database::dsls::info_dsl::Announcement as DbAnnouncement;
use crate::middlelayer::db_handler::DatabaseHandler;
use anyhow::anyhow;
use anyhow::Result;
use aruna_rust_api::api::storage::services::v2::{Announcement, SetAnnouncementsRequest};
use diesel_ulid::DieselUlid;
use itertools::Itertools;
use std::str::FromStr;

impl DatabaseHandler {
    pub async fn get_announcements(&self) -> Result<Vec<Announcement>> {
        let client = self.database.get_client().await?;
        Ok(DbAnnouncement::all(&client)
            .await?
            .into_iter()
            .map(|a| a.into())
            .collect_vec())
    }

    pub async fn get_announcement(&self, id: DieselUlid) -> Result<Announcement> {
        let client = self.database.get_client().await?;

        let announcement = DbAnnouncement::get(id, &client)
            .await?
            .ok_or_else(|| anyhow!("Announcement not found"))?;

        Ok(announcement.try_into()?)
    }

    pub async fn set_announcements(
        &self,
        user_id: DieselUlid,
        request: SetAnnouncementsRequest,
    ) -> Result<Vec<Announcement>> {
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client(); // Necessary?

        // Upserts
        let user_name = self
            .cache
            .get_user(&user_id)
            .ok_or_else(|| anyhow!("User not found"))?
            .display_name;
        let mut upserted = vec![];
        for announcement in request.announcements_upsert {
            let mut db_announcement: DbAnnouncement = announcement.try_into()?;

            // Set display name/id of request user if no creator/modifier is provided
            if db_announcement.created_by.is_empty() {
                db_announcement.created_by = if user_name.is_empty() {
                    user_id.to_string()
                } else {
                    user_name.to_string()
                };
            }
            if db_announcement.modified_by.is_empty() {
                db_announcement.modified_by = if user_name.is_empty() {
                    user_id.to_string()
                } else {
                    user_name.to_string()
                };
            }

            upserted.push(db_announcement.upsert(transaction_client).await?);
        }

        // Deletions
        if !request.announcements_delete.is_empty() {
            let deletions: Result<Vec<_>, _> = request
                .announcements_delete
                .into_iter()
                .map(|id| DieselUlid::from_str(&id))
                .collect();
            DbAnnouncement::batch_delete(transaction_client, &deletions?).await?;
        }

        transaction.commit().await?;

        Ok(upserted.into_iter().map(|u| u.into()).collect_vec())
    }
}
