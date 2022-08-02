use crate::database::{
    connection::Database,
    models::{self, auth::ApiToken},
};
use diesel::prelude::*;

impl Database {
    pub fn get_api_token(
        &self,
        token_id: uuid::Uuid,
    ) -> Result<ApiToken, Box<dyn std::error::Error>> {
        use crate::database::schema::api_tokens::dsl::*;
        //use crate::database::schema::identity_providers::dsl::*;
        //use crate::database::schema::notification_stream_groups::dsl::*;
        //use crate::database::schema::users::dsl::*;
        use diesel::result::Error;

        let token_value = self
            .pg_connection
            .get()?
            .transaction::<models::auth::ApiToken, Error, _>(|conn| {
                let user_token = api_tokens.filter(id.eq(token_id)).first::<ApiToken>(conn)?;
                Ok(user_token)
            })?;

        Ok(token_value)
    }
}
