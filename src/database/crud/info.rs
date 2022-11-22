use aruna_rust_api::api::storage::models::v1::{Permission, ResourceType};
use aruna_rust_api::api::storage::services::v1::{
    GetResourceHierarchyRequest, GetResourceHierarchyResponse, Hierarchy,
};
use diesel::dsl::date;
use diesel::{prelude::*, sql_query, QueryDsl, RunQueryDsl};

use crate::database;
use crate::database::connection::Database;
use crate::database::models::auth::{ApiToken, UserPermission};
use crate::database::models::collection::{Collection, CollectionObject, CollectionObjectGroup};
use crate::database::models::object_group::ObjectGroupObject;
use crate::error::ArunaError;
use itertools::Itertools;

impl Database {
    pub fn validate_and_query_hierarchy(
        &self,
        request: GetResourceHierarchyRequest,
        token_id: uuid::Uuid,
    ) -> Result<GetResourceHierarchyResponse, ArunaError> {
        use crate::database::schema::api_tokens::dsl::*;
        use crate::database::schema::collection_object_groups::dsl::*;
        use crate::database::schema::collection_objects::dsl::*;
        use crate::database::schema::collections::dsl::*;
        use crate::database::schema::object_group_objects::dsl::*;

        let parsed_resource = uuid::Uuid::parse_str(&request.resource_id)?;

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

                let hierarchies = match request.resource_type() {
                    ResourceType::Unspecified | ResourceType::All => {
                        return Err(ArunaError::InvalidRequest(
                            "ResourceType must be specified".to_string(),
                        ))
                    }
                    ResourceType::Project => {
                        vec![Hierarchy {
                            object_id: "".to_string(),
                            object_group_ids: vec!["".to_string()],
                            collection_id: "".to_string(),
                            project_id: parsed_resource.to_string(),
                        }]
                    }
                    ResourceType::Collection => {
                        let get_coll = collections
                            .filter(database::schema::collections::id.eq(&parsed_resource))
                            .first::<Collection>(conn)?;
                        vec![Hierarchy {
                            object_id: "".to_string(),
                            object_group_ids: vec!["".to_string()],
                            collection_id: get_coll.id.to_string(),
                            project_id: get_coll.project_id.to_string(),
                        }]
                    }
                    ResourceType::Object => {
                        let coll_objects = collection_objects
                            .filter(
                                database::schema::collection_objects::object_id
                                    .eq(&parsed_resource),
                            )
                            .load::<CollectionObject>(conn)?;
                        let object_grp_objects = object_group_objects
                            .filter(
                                database::schema::object_group_objects::object_id
                                    .eq(&parsed_resource),
                            )
                            .load::<ObjectGroupObject>(conn)
                            .optional()?;

                        // Vec with object_group id / object_id
                        let obj_group_ids = object_grp_objects.map(|grps| {
                            grps.iter()
                                .map(|grp| (grp.object_group_id, grp.object_id))
                                .collect::<Vec<(uuid::Uuid, uuid::Uuid)>>()
                        });

                        // Vec with coll_id & object_group_id
                        let coll_obj_ids = match obj_group_ids {
                            Some(ogroup_tuple) => {
                                let ogroup_ids = ogroup_tuple
                                    .iter()
                                    .map(|(og, _)| og.to_owned())
                                    .collect::<Vec<uuid::Uuid>>();

                                match collection_object_groups
                                    .filter(
                                        database::schema::collection_object_groups::object_group_id
                                            .eq_any(&ogroup_ids),
                                    )
                                    .load::<CollectionObjectGroup>(conn)
                                    .optional()?
                                {
                                    Some(grp) => grp
                                        .iter()
                                        .map(|grp| (grp.collection_id, grp.object_group_id))
                                        .collect::<Vec<(uuid::Uuid, uuid::Uuid)>>(),
                                    None => Vec::new(),
                                }
                            }
                            None => Vec::new(),
                        };

                        // Get collections
                        let coll_ids = coll_obj_ids
                            .iter()
                            .map(|(coll, _)| coll.to_owned())
                            .unique()
                            .collect::<Vec<uuid::Uuid>>();

                        let colls = collections
                            .filter(database::schema::collections::id.eq_any(&coll_ids))
                            .load::<Collection>(conn)?;

                        let col_projs = colls
                            .iter()
                            .map(|col| (col.project_id, col.id))
                            .collect::<Vec<(uuid::Uuid, uuid::Uuid)>>();

                        todo!()
                    }
                    ResourceType::ObjectGroup => {
                        todo!()
                    }
                };

                Ok(hierarchies)
            });

        todo!()
    }
}
