use crate::database::enums::PermissionLevels;
use anyhow::Result;
use diesel_ulid::DieselUlid;

pub struct ResourcePermission {
    pub id: DieselUlid,
    pub level: PermissionLevels,
    pub allow_sa: bool,
}
impl ResourcePermission {
    pub fn new(id: DieselUlid, level: PermissionLevels, allow_sa: bool) -> Self {
        ResourcePermission {
            id,
            level,
            allow_sa,
        }
    }
}

pub enum Context {
    Project(ResourcePermission),
    Collection(ResourcePermission),
    Dataset(ResourcePermission),
    Object(ResourcePermission),
    User(DieselUlid),
    GlobalAdmin,
}

pub struct Authorizer {}

impl Authorizer {
    pub fn new() -> Self {
        Authorizer {}
    }

    pub fn check_permissions(&self, _token: &str, _ctx: Context) -> Result<bool> {
        // Should return user_id
        Ok(false)
    }
}
