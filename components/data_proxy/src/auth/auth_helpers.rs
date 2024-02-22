use diesel_ulid::DieselUlid;
use s3s::{s3_error, S3Error};
use tracing::error;

use crate::structs::{AccessKeyPermissions, DbPermissionLevel, Object};

/// Creates a list of tuples with the prefix and the object name
#[tracing::instrument(level = "trace", skip(key))]
pub(super) fn key_into_prefix(key: &str) -> Result<Vec<(String, String)>, S3Error> {
    let mut parts = Vec::new();
    let mut prefix = String::new();
    for s in key.splitn(4, "/") {
        prefix.push_str(s);
        parts.push((prefix.clone(), s.to_string()));
        prefix.push('/');
    }
    if parts.len() > 4 {
        error!("This should not happen: Detected more than 4 Objects in the path");
        return Err(s3_error!(InternalError, "Invalid key parsing"));
    }
    Ok(parts)
}

#[tracing::instrument(level = "trace", skip(key_info, resource_id, perm))]
pub(super) fn check_permissions(
    key_info: &AccessKeyPermissions,
    resource_id: &DieselUlid,
    perm: DbPermissionLevel,
) -> Result<(), S3Error> {
    if key_info.permissions.get(&resource_id).ok_or_else(|| {
        error!("No permissions found");
        s3_error!(AccessDenied, "Access Denied")
    })? < &perm
    {
        error!("Insufficient permissions");
        return Err(s3_error!(AccessDenied, "Access Denied"));
    }
    Ok(())
}

#[tracing::instrument(level = "trace", skip(key_info, resource_ids, perm))]
pub(super) fn check_multi_permissions(
    key_info: &AccessKeyPermissions,
    resource_ids: &[Object],
    perm: DbPermissionLevel,
) -> Result<(), S3Error> {
    for id in resource_ids {
        if let Some(perm) = key_info.permissions.get(&id.id) {
            if perm >= &perm {
                return Ok(());
            }
        }
    }
    error!("Insufficient permissions");
    Err(s3_error!(AccessDenied, "Access Denied"))
}
