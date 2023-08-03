use anyhow::Result;
use aruna_rust_api::api::storage::services::v2::{
    ActivateUserRequest, DeactivateUserRequest, RegisterUserRequest, UpdateUserDisplayNameRequest,
    UpdateUserEmailRequest,
};
use diesel_ulid::DieselUlid;
use std::str::FromStr;

pub struct RegisterUser(pub RegisterUserRequest);
pub struct DeactivateUser(pub DeactivateUserRequest);
pub struct ActivateUser(pub ActivateUserRequest);
pub struct UpdateUserName(pub UpdateUserDisplayNameRequest);
pub struct UpdateUserEmail(pub UpdateUserEmailRequest);

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
