use aruna_rust_api::api::storage::models::v1::Permission;
use aruna_rust_api::api::storage::services::v1::{
    GetResourceHierarchyRequest, GetResourceHierarchyResponse,
};
use diesel::{prelude::*, sql_query, QueryDsl, RunQueryDsl};

use crate::database;
use crate::database::connection::Database;
use crate::database::models::auth::{ApiToken, UserPermission};
use crate::error::ArunaError;

impl Database {
    pub fn validate_and_query_hierarchy(
        &self,
        request: GetResourceHierarchyRequest,
        token_id: uuid::Uuid,
    ) -> Result<GetResourceHierarchyResponse, ArunaError> {
        use crate::database::schema::api_tokens::dsl::*;
        // Check if endpoint defined in the config already exists in the database
        let hierarchy = self
            .pg_connection
            .get()?
            .transaction::<_, ArunaError, _>(|conn| {
                enum Scoped {
                    COLLECTIONID(uuid::Uuid),
                    PROJECTID(uuid::Uuid),
                    NONE,
                }

                let api_token = api_tokens
                    .filter(database::schema::api_tokens::id.eq(token_id))
                    .first::<ApiToken>(conn)?;

                let project_ids = Vec::new();

                // Check if token is scoped and use this scope
                let scope = match api_token.collection_id {
                    Some(coll) => Scoped::COLLECTIONID(coll),
                    None => match api_token.project_id {
                        Some(proj) => Scoped::PROJECTID(proj),
                        None => Scoped::NONE,
                    },
                };

                let permissions = match scope {
                    Scoped::COLLECTIONID(coll) => {

                    }
                    Scoped::PROJECTID(proj) => {}
                    Scoped::NONE => {}
                }

                Ok(())
            });

        todo!()
    }
}
