use aruna_rust_api::api::storage::models::v1::{Permission, ResourceType};
use aruna_rust_api::api::storage::services::v1::{
    GetResourceHierarchyRequest, GetResourceHierarchyResponse, Hierarchy,
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
            .transaction::<Vec<Hierarchy>, ArunaError, _>(|conn| {
                #[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
                enum Scoped {
                    COLLECTIONID(uuid::Uuid),
                    PROJECTID(uuid::Uuid),
                    NONE,
                }

                let api_token = api_tokens
                    .filter(database::schema::api_tokens::id.eq(token_id))
                    .first::<ApiToken>(conn)?;

                // Check if token is scoped and use this scope
                let scope = match api_token.collection_id {
                    Some(coll) => Scoped::COLLECTIONID(coll),
                    None => match api_token.project_id {
                        Some(proj) => Scoped::PROJECTID(proj),
                        None => Scoped::NONE,
                    },
                };

                match request.resource_type() {
                    ResourceType::Unspecified | ResourceType::All => {
                        return Err(ArunaError::InvalidRequest(
                            "ResourceType must be specified".to_string(),
                        ))
                    }
                    ResourceType::Project => {
                        if scope == Scoped::PROJECTID(uuid::Uuid::parse_str(&request.resource_id)?)
                        {
                            return Ok(vec![Hierarchy {
                                object_id: "".to_string(),
                                object_group_ids: vec!["".to_string()],
                                collection_id: "".to_string(),
                                project_id: request.resource_id,
                            }]);
                        } else {
                            
                        }
                    }
                    ResourceType::Collection => {}
                    ResourceType::Object => {}
                    ResourceType::ObjectGroup => {}
                }

                Ok(())
            });

        todo!()
    }
}
