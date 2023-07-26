use crate::database::{dsls::user_dsl::User, enums::DbPermissionLevel};
use anyhow::{bail, Result};
use diesel_ulid::DieselUlid;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub enum Context {
    Activated,
    ResourceContext((DieselUlid, DbPermissionLevel)),
    User(DieselUlid),
    GlobalAdmin,
}

impl User {
    pub fn get_permissions(
        &self,
        token: Option<DieselUlid>,
    ) -> Result<Vec<(DieselUlid, DbPermissionLevel)>> {
        if let Some(token) = token {
            if let Some(token) = self.attributes.0.tokens.get(&token) {
                return Ok(vec![(token.object_id, token.user_rights)]);
            } else {
                bail!("Token not found")
            }
        } else {
            Ok(self
                .attributes
                .0
                .permissions
                .iter()
                .map(|(k, v)| (*k, *v))
                .collect())
        }
    }
}
