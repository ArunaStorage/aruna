use crate::database::dsls::info_dsl::Announcement as DbAnnouncement;
use anyhow::{bail, Result};
use aruna_rust_api::api::storage::services::v2::{Announcement, AnnouncementType};
use chrono::{DateTime, Utc};
use diesel_ulid::DieselUlid;
use std::str::FromStr;

fn string_to_announcement_type(input: String) -> AnnouncementType {
    match input.as_str() {
        "MISC" => return AnnouncementType::Misc,
        "RELEASE" => return AnnouncementType::Release,
        "UPDATE" => return AnnouncementType::Update,
        "MAINTENANCE" => return AnnouncementType::Maintenance,
        _ => return AnnouncementType::Unspecified
    }
}

fn announcement_type_to_string(input: AnnouncementType) -> Result<String> {
    Ok(match input {
        AnnouncementType::Unspecified => bail!("Unspecified Announcement Type"),
        AnnouncementType::Misc => "MISC".to_string(),
        AnnouncementType::Release => "RELEASE".to_string(),
        AnnouncementType::Update => "UPDATE".to_string(),
        AnnouncementType::Maintenance => "MAINTENANCE".to_string(),
    })
}

impl From<DbAnnouncement> for Announcement {
    fn from(value: DbAnnouncement) -> Self {
        Announcement {
            id: value.id.to_string(),
            announcement_type: string_to_announcement_type(value.announcement_type) as i32,
            title: value.title,
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
        Ok(DbAnnouncement {
            id: DieselUlid::from_str(&value.id)?,
            announcement_type: announcement_type_to_string(value.announcement_type())?,
            title: value.title,
            content: value.content,
            created_by: value.created_by, // Will be replaced afterward if empty
            created_at: if let Some(timestamp) = value.created_at {
                DateTime::from_timestamp(timestamp.seconds, timestamp.nanos.try_into()?)
                    .map(|e| e.naive_utc())
                    .unwrap_or_default()
            } else {
                Utc::now().naive_utc()
            },
            modified_by: value.modified_by, // Will be replaced afterward if empty
            modified_at: if let Some(timestamp) = value.modified_at {
                DateTime::from_timestamp(timestamp.seconds, timestamp.nanos.try_into()?)
                    .map(|e| e.naive_utc())
                    .unwrap_or_default()
            } else {
                Utc::now().naive_utc()
            },
        })
    }
}
