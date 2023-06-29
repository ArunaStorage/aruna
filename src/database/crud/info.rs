use std::collections::HashMap;

use aruna_rust_api::api::storage::models::v1::ResourceType;
use aruna_rust_api::api::storage::services::v1::{GetResourceHierarchyResponse, Hierarchy};
use diesel::{prelude::*, QueryDsl, RunQueryDsl};

use crate::database;
use crate::database::connection::Database;
use crate::database::models::auth::ApiToken;
use crate::database::models::collection::{Collection, CollectionObjectGroup};
use crate::error::ArunaError;

impl Database {
    pub fn validate_and_query_hierarchy(
        &self,
        token_id: diesel_ulid::DieselUlid,
        resource_ulid: &diesel_ulid::DieselUlid,
        resource_type: ResourceType,
    ) -> Result<GetResourceHierarchyResponse, ArunaError> {
        use crate::database::schema::api_tokens::dsl::*;
        use crate::database::schema::collection_object_groups::dsl::*;
        use crate::database::schema::collections::dsl::*;

        // Transaction time
        let _hierarchy = self
            .pg_connection
            .get()?
            .transaction::<Vec<Hierarchy>, ArunaError, _>(|conn| {
                #[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
                enum Scoped {
                    Collectionid(diesel_ulid::DieselUlid),
                    Projectid(diesel_ulid::DieselUlid),
                    None,
                }

                // Fetch api token meta from database
                let api_token = api_tokens
                    .filter(database::schema::api_tokens::id.eq(token_id))
                    .first::<ApiToken>(conn)?;

                // Check if token is scoped and use this scope
                let _scope = match api_token.collection_id {
                    Some(coll) => Scoped::Collectionid(coll),
                    None => match api_token.project_id {
                        Some(proj) => Scoped::Projectid(proj),
                        None => Scoped::None,
                    },
                };

                // Query hierarchy info of resource
                let hierarchies = match resource_type {
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
                            project_id: resource_ulid.to_string(),
                        }]
                    }
                    ResourceType::Collection => {
                        let get_coll = collections
                            .filter(database::schema::collections::id.eq(&resource_ulid))
                            .first::<Collection>(conn)?;

                        vec![Hierarchy {
                            object_id: "".to_string(),
                            object_group_ids: vec!["".to_string()],
                            collection_id: get_coll.id.to_string(),
                            project_id: get_coll.project_id.to_string(),
                        }]
                    }
                    ResourceType::Object => {
                        // Note: Should all references (explicit and implicit) of the object be considered?
                        //  - Explicit: All references of specific object_id
                        //  - Implicit: All auto_update references of shared_revision_id or even all other references?

                        // Fetch all collection ids the object is explicitly member of
                        let all_object_refs = crate::database::crud::object::get_all_references(conn, resource_ulid, &false)?;

                        // Filter all relevant coll ids from references
                        let obj_coll_ids = all_object_refs
                            .into_iter()
                            .map(|obj_ref| obj_ref.collection_id)
                            .collect::<Vec<_>>();

                        // Fetch collection for project_id
                        let colls: Vec<Collection> = collections
                            .filter(database::schema::collections::id.eq_any(&obj_coll_ids))
                            .load::<Collection>(conn)?;

                        // Create (collection_id, project_id) HashMap
                        let col_projs = colls
                            .iter()
                            .map(|col| (col.id, col.project_id))
                            .collect::<HashMap<diesel_ulid::DieselUlid, diesel_ulid::DieselUlid>>();

                        // Fetch all Collection <-> ObjectGroup rerferences filtered by collection_id and object_id 
                        let object_collection_object_groups: Vec<CollectionObjectGroup> = collection_object_groups
                            .inner_join(database::schema::object_group_objects::dsl::object_group_objects
                                .on(database::schema::collection_object_groups::dsl::object_group_id.eq(
                                    database::schema::object_group_objects::dsl::object_group_id)
                                )
                            )
                            .filter(database::schema::collection_object_groups::collection_id.eq_any(&obj_coll_ids))
                            .filter(database::schema::object_group_objects::object_id.eq(&resource_ulid))
                            .select(CollectionObjectGroup::as_select())
                            .load::<CollectionObjectGroup>(conn)?;

                        // Create collection_id <-> Vec<object_group_id> HashMap
                        let col_obj_grps = obj_coll_ids
                        .iter()
                        .map(|coll_id|
                            (coll_id.to_owned(),
                            object_collection_object_groups
                                .iter()
                                .filter(|reference| reference.collection_id == *coll_id)
                                .map(|reference| reference.object_group_id.to_string())
                                .collect())
                        )
                        .collect::<HashMap<diesel_ulid::DieselUlid, Vec<String>>>();

                        // Loop hierarchy creation
                        let mut obj_hierarchies = vec![];
                        for (again_coll_id, object_group_ulids) in col_obj_grps {
                            obj_hierarchies.push(Hierarchy {
                                object_id: resource_ulid.to_string(),
                                object_group_ids: object_group_ulids,
                                collection_id: again_coll_id.to_string(),
                                project_id: match col_projs.get(&again_coll_id) {
                                    Some(project_ulid) => project_ulid.to_string(),
                                    None => return Err(ArunaError::InvalidRequest("I fucked up.".to_string())),
                                }
                            });
                        }

                        obj_hierarchies
                    }
                    ResourceType::ObjectGroup => {
                        // Fetch CollectionObjectGroup Reference
                        let reference = collection_object_groups
                        .filter(crate::database::schema::collection_object_groups::object_group_id.eq(&resource_ulid))
                        .first::<CollectionObjectGroup>(conn)?;

                        // Fetch collection
                        let og_collection = collections
                        .filter(crate::database::schema::collections::id.eq(&reference.collection_id))
                        .first::<Collection>(conn)?;

                        // Return hierarchy with project_ulid, collection_ulid and single object_group_ulid
                        vec![Hierarchy {
                            object_id: "".to_string(),
                            object_group_ids: vec![reference.object_group_id.to_string()],
                            collection_id: og_collection.id.to_string(),
                            project_id: og_collection.project_id.to_string(),
                        }]
                    }
                };

                Ok(hierarchies)
            });

        todo!()
    }
}
