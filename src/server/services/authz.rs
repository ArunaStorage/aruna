use std::{
    io::{Error, ErrorKind},
    sync::Arc,
};

use tonic::metadata::MetadataMap;

use crate::database::{
    connection::Database,
    models::{
        auth::{ApiToken, UserPermission},
        enums::{Resources, UserRights},
    },
};

pub struct Authz {}

pub struct Context {
    pub user_right: UserRights,
    pub resource_type: Resources,
    pub resource_id: uuid::Uuid,
}

impl Authz {
    pub fn authorize(
        db: Arc<Database>,
        metadata: &MetadataMap,
        context: Context,
    ) -> Result<ApiToken, Box<dyn std::error::Error>> {
        let token = metadata
            .get("Bearer")
            .ok_or(Error::new(ErrorKind::Other, "Token not found"))?
            .to_str()?;

        let (token, permissions) = db.get_user_permissions_from_token(token.to_string())?;

        Authz::validate(&token, &context, &permissions)?;
        todo!()
    }

    /// This method validates the "requested" context against the permissions returned by the db
    pub fn validate(
        token: &ApiToken,
        context: &Context,
        perms: &Option<Vec<UserPermission>>,
    ) -> Result<(), Error> {
        // If perm is none -> The token must be project or collection scoped

        match context.resource_type {
            Resources::COLLECTION => (),
            Resources::PROJECT => (),
            _ => (),
        }

        todo!()
    }

    pub fn check<T: std::cmp::PartialEq, U: std::cmp::PartialEq>(
        context_uuid: T,
        token_uuid: T,
        context_resource: U,
        token_resource: U,
    ) -> Option<()> {
        if context_resource == token_resource && context_uuid == token_uuid {
            return Some(());
        }

        None
    }
}
