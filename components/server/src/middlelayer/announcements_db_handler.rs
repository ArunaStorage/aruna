use crate::database::crud::CrudDb;
use crate::database::dsls::info_dsl::Announcement as DbAnnouncement;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::utils::conversions::announcements::announcement_type_to_string;
use anyhow::anyhow;
use anyhow::Result;
use aruna_rust_api::api::storage::services::v2::GetAnnouncementsByTypeRequest;
use aruna_rust_api::api::storage::services::v2::GetAnnouncementsRequest;
use aruna_rust_api::api::storage::services::v2::{Announcement, SetAnnouncementsRequest};
use diesel_ulid::DieselUlid;
use itertools::Itertools;
use std::str::FromStr;

impl DatabaseHandler {
    pub async fn get_announcements(
        &self,
        request: GetAnnouncementsRequest,
    ) -> Result<Vec<Announcement>> {
        // Create database client
        let client = self.database.get_client().await?;

        Ok(if request.announcement_ids.len() > 0 {
            // Parse the ids
            let mapped_ids: Result<Vec<_>, _> = request
                .announcement_ids
                .iter()
                .map(|id| DieselUlid::from_str(id))
                .collect();

            DbAnnouncement::get_by_ids(
                &client,
                &tonic_invalid!(mapped_ids, "Invalid announcement id format"),
                request.page,
            )
            .await?
        } else {
            DbAnnouncement::all_paginated(&client, request.page).await?
        }
        .into_iter()
        .map(|a| a.into())
        .collect_vec())
    }

    pub async fn get_announcements_by_type(
        &self,
        request: GetAnnouncementsByTypeRequest,
    ) -> Result<Vec<Announcement>> {
        // Parse the provided type
        let announcement_type = tonic_invalid!(
            announcement_type_to_string(request.announcement_type()),
            "Invalid announcement type"
        );

        // Create database client and query announcements
        let client = self.database.get_client().await?;
        Ok(
            DbAnnouncement::get_by_type(&client, announcement_type, request.page)
                .await?
                .into_iter()
                .map(|a| a.into())
                .collect_vec(),
        )
    }

    pub async fn get_announcement(&self, id: DieselUlid) -> Result<Announcement> {
        // Create database client and query announcements
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
