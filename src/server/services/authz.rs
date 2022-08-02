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
    user_right: UserRights,
    resource_type: Resources,
    uid: uuid::Uuid,
}

impl Authz {
    pub fn new() -> Self {
        Authz {}
    }

    pub fn authorize(
        db: Arc<Database>,
        metadata: &MetadataMap,
        context: Context,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let creator = uuid::Uuid::parse_str(
            metadata
                .get("UserId")
                .ok_or(Error::new(ErrorKind::Other, "oh no!"))?
                .to_str()?,
        )?;

        todo!()
    }
}
