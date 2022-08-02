use std::{
    io::{Error, ErrorKind},
    sync::Arc,
};

use tonic::metadata::MetadataMap;

use crate::database::{
    connection::Database,
    models::{
        auth::ApiToken,
        enums::{Resources, UserRights},
    },
};

pub struct Authz {}

pub struct Context {
    pub user_right: UserRights,
    pub resource_type: Resources,
    pub uid: uuid::Uuid,
}

impl Authz {
    pub fn authorize(
        db: Arc<Database>,
        metadata: &MetadataMap,
        _context: Context,
    ) -> Result<ApiToken, Box<dyn std::error::Error>> {
        let token_id = uuid::Uuid::parse_str(
            metadata
                .get("UserId")
                .ok_or(Error::new(ErrorKind::Other, "oh no!"))?
                .to_str()?,
        )?;

        let _token = db.get_api_token(token_id);

        todo!()
    }
}
