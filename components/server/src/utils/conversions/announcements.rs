use crate::{
    database::dsls::info_dsl::Announcement as DbAnnouncement, utils::grpc_utils::from_prost_time,
};
use anyhow::{anyhow, bail, Result};
use aruna_rust_api::api::storage::{models::v2::AnnouncementType, services::v2::Announcement};
use chrono::DateTime;
use diesel_ulid::DieselUlid;
use std::str::FromStr;

pub fn string_to_announcement_type(input: String) -> AnnouncementType {
    match input.as_str() {
        "ORGA" => AnnouncementType::Orga,
        "RELEASE" => AnnouncementType::Release,
        "UPDATE" => AnnouncementType::Update,
        "MAINTENANCE" => AnnouncementType::Maintenance,
        "BLOG" => AnnouncementType::Blog,
        _ => AnnouncementType::Unspecified,
    }
}

pub fn announcement_type_to_string(input: AnnouncementType) -> Result<String> {
    Ok(match input {
        AnnouncementType::Unspecified => bail!("Unspecified Announcement Type"),
        AnnouncementType::Orga => "ORGA".to_string(),
        AnnouncementType::Release => "RELEASE".to_string(),
        AnnouncementType::Update => "UPDATE".to_string(),
        AnnouncementType::Maintenance => "MAINTENANCE".to_string(),
        AnnouncementType::Blog => "BLOG".to_string(),
    })
}

impl From<DbAnnouncement> for Announcement {
    fn from(value: DbAnnouncement) -> Self {
        Announcement {
            announcement_id: value.id.to_string(),
            announcement_type: string_to_announcement_type(value.announcement_type) as i32,
            title: value.title,
            teaser: value.teaser,
            image_url: value.image_url,
            content: value.content,
            created_by: value.created_by,
            created_at: Some(value.created_at.into()),
            modified_by: value.modified_by,
            modified_at: Some(value.modified_at.into()),
        }
    }
}

impl TryFrom<Announcement> for DbAnnouncement {
    type Error = anyhow::Error;

    fn try_from(value: Announcement) -> Result<Self, Self::Error> {
        // Evaluate timestamps dependending on presence of an id or created_at timestamp
        // Pagination would break at specific cases if the id and creation timestamp are decoupled
        let id = if value.announcement_id.is_empty() {
            if let Some(created_at) = from_prost_time(value.created_at.clone()) {
                DieselUlid::from_timestamp_millis(
                    created_at.and_utc().timestamp_millis().try_into()?,
                )
                .map_err(|_| anyhow!("Invalid ULID timestamp provided"))?
            } else {
                DieselUlid::generate()
            }
        } else {
            DieselUlid::from_str(&value.announcement_id)?
        };
        let current_timestamp = DateTime::from_timestamp_millis(id.timestamp() as i64)
            .map(|e| e.naive_utc())
            .unwrap_or_default();

        Ok(DbAnnouncement {
            id,
            announcement_type: announcement_type_to_string(value.announcement_type())?,
            title: value.title,
            teaser: value.teaser,
            image_url: value.image_url,
            content: value.content,
            created_by: value.created_by, // Will be replaced afterward if empty
            created_at: current_timestamp,
            modified_by: value.modified_by, // Will be replaced afterward if empty
            modified_at: current_timestamp,
        })
    }
}
