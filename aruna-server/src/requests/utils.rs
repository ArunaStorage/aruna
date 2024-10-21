use chrono::{DateTime, Utc};
use ulid::Ulid;

use crate::error::ArunaError;

use super::transaction::Fields;

pub(crate) fn get_resource_field(fields: &Option<Vec<Fields>>) -> Result<Ulid, ArunaError> {
    if let Fields::ResourceId(resource_id) =
        get_field(fields, Fields::ResourceId(Default::default()))?
    {
        Ok(resource_id)
    } else {
        tracing::error!("Field response invalid");
        Err(ArunaError::TransactionFailure(
            "Field response invalid".to_string(),
        ))
    }
}

pub(crate) fn get_realm_field(fields: &Option<Vec<Fields>>) -> Result<Ulid, ArunaError> {
    if let Fields::RealmId(realm_id) = get_field(fields, Fields::RealmId(Default::default()))? {
        Ok(realm_id)
    } else {
        tracing::error!("Field response invalid");
        Err(ArunaError::TransactionFailure(
            "Field response invalid".to_string(),
        ))
    }
}

pub(crate) fn get_group_field(fields: &Option<Vec<Fields>>) -> Result<Ulid, ArunaError> {
    if let Fields::GroupId(group_id) = get_field(fields, Fields::GroupId(Default::default()))? {
        Ok(group_id)
    } else {
        tracing::error!("Field response invalid");
        Err(ArunaError::TransactionFailure(
            "Field response invalid".to_string(),
        ))
    }
}

pub(crate) fn get_created_at_field(
    fields: &Option<Vec<Fields>>,
) -> Result<DateTime<Utc>, ArunaError> {
    if let Fields::CreatedAt(created_at) = get_field(fields, Fields::CreatedAt(Default::default()))?
    {
        DateTime::from_timestamp_millis(created_at).ok_or_else(|| {
            tracing::error!("Unexpected timestamp format");
            ArunaError::TransactionFailure("Invalid timestamp: created_at".to_string())
        })
    } else {
        tracing::error!("Field response invalid");
        Err(ArunaError::TransactionFailure(
            "Field response invalid".to_string(),
        ))
    }
}

fn get_field(fields: &Option<Vec<Fields>>, expected: Fields) -> Result<Fields, ArunaError> {
    for field in fields
        .as_ref()
        .ok_or_else(|| {
            tracing::error!("No generated fields");
            ArunaError::InvalidParameter {
                name: "generated_fields".to_string(),
                error: "No generated fields".to_string(),
            }
        })?
        .iter()
    {
        match (&expected, field) {
            (Fields::CreatedAt(_), got @ Fields::CreatedAt(_)) => return Ok(got.clone()),
            (Fields::UserId(_), got @ Fields::UserId(_)) => return Ok(got.clone()),
            (Fields::GroupId(_), got @ Fields::GroupId(_)) => return Ok(got.clone()),
            (Fields::ParentId(_), got @ Fields::ParentId(_)) => return Ok(got.clone()),
            (Fields::ResourceId(_), got @ Fields::ResourceId(_)) => return Ok(got.clone()),
            (Fields::RealmId(_), got @ Fields::RealmId(_)) => return Ok(got.clone()),
            _ => {
                continue;
            }
        }
    }
    tracing::error!("Invalid parameter generated_fields: Could not find expected field");
    Err(ArunaError::InvalidParameter {
        name: "generated_fields".to_string(),
        error: "Could not find expected field".to_string(),
    })
}
