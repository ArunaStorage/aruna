use crate::auth::structs::Context;
use crate::database::enums::DbPermissionLevel;
use anyhow::Result;
use aruna_rust_api::api::storage::services::v2::{
    ActivateUserRequest, DeactivateUserRequest, GetUserRedactedRequest, GetUserRequest,
    RegisterUserRequest, UpdateUserDisplayNameRequest, UpdateUserEmailRequest,
};
use diesel_ulid::DieselUlid;
use std::str::FromStr;

pub struct RegisterUser(pub RegisterUserRequest);
pub struct DeactivateUser(pub DeactivateUserRequest);
pub struct ActivateUser(pub ActivateUserRequest);
pub struct UpdateUserName(pub UpdateUserDisplayNameRequest);
pub struct UpdateUserEmail(pub UpdateUserEmailRequest);
pub enum GetUser {
    GetUser(GetUserRequest),
    GetUserRedacted(GetUserRedactedRequest),
}

impl RegisterUser {
    pub fn get_display_name(&self) -> String {
        self.0.display_name.clone()
    }
    pub fn get_email(&self) -> String {
        self.0.email.clone()
    }
    pub fn get_project_hint(&self) -> String {
        self.0.project.clone()
    }
}

impl DeactivateUser {
    pub fn get_id(&self) -> Result<DieselUlid> {
        Ok(DieselUlid::from_str(&self.0.user_id)?)
    }
}

impl ActivateUser {
    pub fn get_id(&self) -> Result<DieselUlid> {
        Ok(DieselUlid::from_str(&self.0.user_id)?)
    }
}

impl UpdateUserName {
    pub fn get_name(&self) -> String {
        self.0.new_display_name.clone()
    }
}
impl UpdateUserEmail {
    pub fn get_user(&self) -> Result<DieselUlid> {
        Ok(DieselUlid::from_str(&self.0.user_id)?)
    }
    pub fn get_email(&self) -> String {
        self.0.new_email.clone()
    }
}

impl GetUser {
    pub fn get_user(&self) -> Result<(Option<DieselUlid>, Context)> {
        let (id, ctx) = match self {
            GetUser::GetUser(req) => {
                if req.user_id.is_empty() {
                    (None, Context::self_ctx())
                } else {
                    (Some(DieselUlid::from_str(&req.user_id)?), Context::admin())
                }
            }
            GetUser::GetUserRedacted(req) => {
                if req.user_id.is_empty() {
                    (None, Context::self_ctx())
                } else {
                    (Some(DieselUlid::from_str(&req.user_id)?), Context::admin())
                }
            }
        };
        Ok((id, ctx))
    }
}
