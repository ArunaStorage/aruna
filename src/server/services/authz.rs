use std::{
    io::{Error, ErrorKind},
    sync::Arc,
};

use tonic::metadata::MetadataMap;

use crate::database::{
    connection::Database,
    models::enums::{Resources, UserRights},
};

pub struct Authz {}

pub struct Context {
    pub user_right: UserRights,
    pub resource_type: Resources,
    pub resource_id: uuid::Uuid,
    pub admin: bool,
}

impl Authz {
    pub fn authorize(
        db: Arc<Database>,
        metadata: &MetadataMap,
        context: Context,
    ) -> Result<uuid::Uuid, Box<dyn std::error::Error>> {
        let token = metadata
            .get("Bearer")
            .ok_or(Error::new(ErrorKind::Other, "Token not found"))?
            .to_str()?;

        let user_uid = match context.resource_type {
            Resources::COLLECTION => {
                db.get_user_right_from_token(token, Some(context.resource_id), None, context.admin)?
            }
            Resources::PROJECT => {
                db.get_user_right_from_token(token, None, Some(context.resource_id), context.admin)?
            }
            _ => {
                return Err(Box::new(Error::new(
                    ErrorKind::Other,
                    "Forbidden resource type",
                )))
            }
        };
        return Ok(user_uid);
    }
}
