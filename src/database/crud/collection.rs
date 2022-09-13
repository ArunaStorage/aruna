//! This file contains all database methods that handle collection specific actions
//!
//! Mainly this is used to:
//!
//! - Create a new collection
//! - GetCollectionById
//! - GetCollections
//! - UpdateCollections
//! - PinCollectionVersion
//! - DeleteCollection
use super::utils::*;
use crate::api::aruna::api::storage::models::v1::DataClass;
use crate::api::aruna::api::storage::models::v1::{
    collection_overview, collection_overview::Version as CollectionVersiongRPC, CollectionOverview,
    CollectionOverviews, CollectionStats, LabelOntology, Stats, Version,
};
use crate::api::aruna::api::storage::services::v1::{
    CreateNewCollectionRequest, CreateNewCollectionResponse, DeleteCollectionRequest,
    DeleteCollectionResponse, GetCollectionByIdRequest, GetCollectionByIdResponse,
    GetCollectionsRequest, GetCollectionsResponse, PinCollectionVersionRequest,
    PinCollectionVersionResponse, UpdateCollectionRequest, UpdateCollectionResponse,
};
use crate::database::connection::Database;
use crate::database::crud::object::clone_object;
use crate::database::models;
use crate::database::models::collection::{
    Collection, CollectionKeyValue, CollectionObject, CollectionObjectGroup, CollectionVersion,
    RequiredLabel,
};
use crate::database::models::enums::{Dataclass as DBDataclass, ObjectStatus};
use crate::database::models::object::Object;
use crate::database::models::object_group::{ObjectGroup, ObjectGroupKeyValue, ObjectGroupObject};
use crate::database::models::views::CollectionStat;
use crate::error::ArunaError;
use chrono::Local;
use diesel::r2d2::ConnectionManager;
use diesel::result::Error;
use diesel::{delete, prelude::*};
use diesel::{insert_into, update};
use r2d2::PooledConnection;
use std::collections::HashMap;

/// Helper struct that contains a `CollectionOverviewDb`
///
/// Fields:
/// - coll -> CollectionOverview
/// - coll_key_value -> Labels / Hooks
/// - coll_req_labels -> Required Labels / LabelOntology
/// - coll_stats -> Statisticts from materialized view
/// - coll_version -> Version / None if "latest"
#[derive(Debug, Clone)]
struct CollectionOverviewDb {
    coll: Collection,
    coll_key_value: Option<Vec<CollectionKeyValue>>,
    coll_req_labels: Option<Vec<RequiredLabel>>,
    coll_stats: Option<CollectionStat>,
    coll_version: Option<CollectionVersion>,
}

impl Database {
    /// Create_new_collection request cretes a new collection based on user request
    ///
    /// ## Behaviour
    ///
    /// A new collection is created when this request is made, it needs project-level "WRITE" permissions to succeed.
    ///
    /// ## Arguments
    ///
    /// `CreateNewCollectionRequest` - Basic information about the freshly created collection like name description etc.
    ///
    /// ## Results
    ///
    /// `CreateNewCollectionResponse` - Overview of the new created collection.
    ///
    /// TODO: LabelOntology ?
    pub fn create_new_collection(
        &self,
        request: CreateNewCollectionRequest,
        creator: uuid::Uuid,
    ) -> Result<CreateNewCollectionResponse, ArunaError> {
        use crate::database::schema::collection_key_value::dsl::*;
        use crate::database::schema::collections::dsl::*;

        // Create new collection uuid
        let collection_uuid = uuid::Uuid::new_v4();
        // Create new "shared_version_uuid"
        let shared_version_uuid = uuid::Uuid::new_v4();
        // Convert request key_values to DB Keyvalue list
        let key_values = to_key_values::<CollectionKeyValue>(
            request.labels.clone(),
            request.hooks.clone(),
            collection_uuid,
        );
        // Create collection DB struct
        let db_collection = models::collection::Collection {
            id: collection_uuid,
            shared_version_id: shared_version_uuid,
            name: request.name.clone(),
            description: request.description.clone(),
            created_by: creator,
            created_at: Local::now().naive_local(),
            version_id: None,
            dataclass: Some(request.dataclass()).map(DBDataclass::from),
            project_id: uuid::Uuid::parse_str(&request.project_id)?,
        };

        // Insert in transaction
        self.pg_connection
            .get()?
            .transaction::<_, Error, _>(|conn| {
                // Insert collection
                insert_into(collections)
                    .values(db_collection)
                    .execute(conn)?;
                // Insert collection key values
                insert_into(collection_key_value)
                    .values(key_values)
                    .execute(conn)?;
                Ok(())
            })?;
        // Create response and return
        Ok(CreateNewCollectionResponse {
            collection_id: collection_uuid.to_string(),
        })
    }

    /// GetCollectionById queries a single collection via its uuid.
    ///
    /// ## Behaviour
    ///
    /// This returns a single collection by id with all available information excluding all informations about objects / objectgroups etc.
    /// For this the associated methods in objects/objectgroups should be used. This needs collection-level read permissions.
    ///
    /// ## Arguments
    ///
    /// `GetCollectionByIdRequest` - Contains the requested collection_id
    ///
    /// ## Results
    ///
    /// `GetCollectionByIdResponse` - Overview of the new created collection.
    ///
    pub fn get_collection_by_id(
        &self,
        request: GetCollectionByIdRequest,
    ) -> Result<GetCollectionByIdResponse, ArunaError> {
        use crate::database::schema::collections::dsl as col;
        use diesel::prelude::*;
        // Parse the collection_id to uuid
        let collection_id = uuid::Uuid::parse_str(&request.collection_id)?;
        // Execute request and return CollectionOverviewDb
        let ret_collection = self
            .pg_connection
            .get()?
            .transaction::<Option<CollectionOverviewDb>, Error, _>(|conn| {
                // Query the collection overview
                let collection_info = col::collections
                    .filter(col::id.eq(collection_id))
                    .first::<models::collection::Collection>(conn)
                    .optional()?;
                // Check if collection_info is Some()
                match collection_info {
                    Some(coll_info) => Ok(Some(query_overview(conn, coll_info)?)), // This will query all associated collectiondata
                    None => Ok(None),
                }
            })?;
        // Build response and return
        Ok(GetCollectionByIdResponse {
            collection: map_to_collection_overview(ret_collection)?,
        })
    }

    /// GetCollections queries multiple collections via either multiple uuids or a specific set of label filters.
    ///
    /// ## Behaviour
    ///
    /// Parses the query / pagerequest and tries to return the requested subset of collections
    ///
    /// ## Arguments
    ///
    /// `GetCollectionsRequest` - Contains a list of collectionids or a label_filter and an optional pagination information
    ///
    /// ## Results
    ///
    /// `GetCollectionsResponse` - Contains a list with collection_overviews that match the request.
    ///
    pub fn get_collections(
        &self,
        request: GetCollectionsRequest,
    ) -> Result<GetCollectionsResponse, ArunaError> {
        use crate::database::schema::collection_key_value::dsl as ckv;
        use crate::database::schema::collections::dsl as col;
        use diesel::prelude::*;
        // Parse the page_request and get pagesize / lastuuid
        let (pagesize, last_uuid) = parse_page_request(request.page_request, 20)?;
        // Parse the query to a `ParsedQuery`
        let parsed_query = parse_query(request.label_or_id_filter)?;
        // Get the project_id
        let project_id = uuid::Uuid::parse_str(&request.project_id)?;
        // Execute request
        let ret_collections = self
            .pg_connection
            .get()?
            .transaction::<Option<Vec<CollectionOverviewDb>>, Error, _>(|conn| {
                // First build a "boxed" base request to which additional parameters can be added later
                let mut base_request = col::collections
                    .filter(col::project_id.eq(project_id))
                    .into_boxed();
                // Create returnvector of CollectionOverviewsDb
                let mut return_vec: Vec<CollectionOverviewDb> = Vec::new();
                // If pagesize is not unlimited set it to pagesize or default = 20
                if let Some(pg_size) = pagesize {
                    base_request = base_request.limit(pg_size);
                }
                // Add "last_uuid" filter if it is specified
                if let Some(l_uid) = last_uuid {
                    base_request = base_request.filter(col::id.ge(l_uid));
                }
                // Add query if it exists
                if let Some(p_query) = parsed_query {
                    // Check if query exists
                    match p_query {
                        // This is a label query request
                        ParsedQuery::LabelQuery(l_query) => {
                            // Create key value boxed request
                            let mut ckv_query = ckv::collection_key_value.into_boxed();
                            // Create vector with "matching" collections
                            let found_cols: Option<Vec<uuid::Uuid>>;
                            // Is "and"
                            if l_query.1 {
                                // Add each key / value to label query
                                for (key, value) in l_query.0.clone() {
                                    // Always add keys
                                    ckv_query = ckv_query.filter(ckv::key.eq(key));
                                    // Will be Some if keys only == false
                                    if let Some(val) = value {
                                        ckv_query = ckv_query.filter(ckv::value.eq(val));
                                    }
                                }
                                // Execute request and get a list with all found key values
                                let found_cols_key_values: Option<Vec<CollectionKeyValue>> =
                                    ckv_query.load::<CollectionKeyValue>(conn).optional()?;
                                // Parse the returned key_values for the "all" constraint
                                // and only return matching collection ids
                                found_cols = check_all_for_db_kv(found_cols_key_values, l_query.0);
                                // If the query is "or"
                            } else {
                                // Query all key / values
                                for (key, value) in l_query.0 {
                                    ckv_query = ckv_query.or_filter(ckv::key.eq(key));
                                    // Only Some() if key_only is false
                                    if let Some(val) = value {
                                        ckv_query = ckv_query.filter(ckv::value.eq(val));
                                    }
                                }
                                // Can query the matches collections directly
                                found_cols = ckv_query
                                    .select(ckv::collection_id)
                                    .distinct()
                                    .load::<uuid::Uuid>(conn)
                                    .optional()?;
                            }
                            // Add to query if something was found otherwise return Only
                            if let Some(fcolls) = found_cols {
                                base_request = base_request.filter(col::id.eq_any(fcolls));
                            } else {
                                return Ok(None);
                            }
                        }
                        // If the request was an ID request, just filter for all ids
                        // And for uuids makes no sense
                        ParsedQuery::IdsQuery(ids) => {
                            base_request = base_request.filter(col::id.eq_any(ids));
                        }
                    }
                }

                // Execute the preconfigured query
                let query_collections: Option<Vec<Collection>> =
                    base_request.load::<Collection>(conn).optional()?;
                // Query overviews for each collection
                // TODO: This might be inefficient and can be optimized later
                if let Some(q_colls) = query_collections {
                    for col in q_colls {
                        return_vec.push(query_overview(conn, col)?);
                    }
                    Ok(Some(return_vec))
                } else {
                    Ok(None)
                }
            })?;
        // Map the collectionoveviewDbs to gRPC collectionoverviews
        let coll_overviews = match ret_collections {
            Some(colls) => {
                let mut coll_overviews: Vec<CollectionOverview> = Vec::new();

                for col in colls {
                    if let Some(coll_overv) = map_to_collection_overview(Some(col))? {
                        coll_overviews.push(coll_overv);
                    }
                }

                Some(CollectionOverviews {
                    collection_overviews: coll_overviews,
                })
            }
            None => None,
        };
        // Return the collection overviews
        Ok(GetCollectionsResponse {
            collections: coll_overviews,
        })
    }

    /// UpdateCollection updates a specific collection and optional pins a new version.
    ///
    /// ## Arguments
    ///
    /// `UpdateCollectionRequest` - Contains the information the collection should be updated to.
    ///
    /// ## Results
    ///
    /// `GetCollectionsResponse` - Responds with an collection_overview for the updated or newly created collection.
    ///
    pub fn update_collection(
        &self,
        request: UpdateCollectionRequest,
        user_id: uuid::Uuid,
    ) -> Result<UpdateCollectionResponse, ArunaError> {
        use crate::database::schema::collections::dsl::*;
        // Query the old collection id that should be updated
        let old_collection_id = uuid::Uuid::parse_str(&request.collection_id)?;
        // Execute request in transaction
        let ret_collections = self
            .pg_connection
            .get()?
            .transaction::<Option<CollectionOverview>, ArunaError, _>(|conn| {
                // Query the old collection
                let old_collection = collections
                    .filter(id.eq(old_collection_id))
                    .first::<Collection>(conn)?;

                // If the old collection or the update creates a new "versioned" collection
                // -> This needs to perform a pin
                if old_collection.version_id.is_some() || request.version.is_some() {
                    let old_overview = query_overview(conn, old_collection.clone())?;
                    // Return error if old_version is >= new_version
                    // Updates must increase the semver
                    // Updates for "historic" versions are not allowed
                    if let Some(v) = &old_overview.coll_version {
                        if Version::from(v.clone()) >= request.version.clone().unwrap() {
                            return Err(ArunaError::InvalidRequest(
                                "New version must be greater than old one".to_string(),
                            ));
                        }
                    }
                    // Create new "Version" database struct
                    let new_version = request
                        .version
                        .clone()
                        .map(|v| v.into_collection_version(uuid::Uuid::new_v4()))
                        .unwrap();

                    // Put together the new collection info
                    let new_coll = Collection {
                        id: uuid::Uuid::new_v4(),
                        shared_version_id: old_collection.shared_version_id,
                        name: request.name.clone(),
                        description: request.description.clone(),
                        created_at: chrono::Utc::now().naive_utc(),
                        created_by: user_id,
                        version_id: Some(new_version.id),
                        dataclass: Some(request.dataclass()).map(DBDataclass::from),
                        project_id: uuid::Uuid::parse_str(&request.project_id)?,
                    };
                    // Execute the pin request and return the collection overview
                    Ok(Some(pin_collection_to_version(
                        old_collection_id,
                        user_id,
                        transform_collection_overviewdb(new_coll, new_version, Some(old_overview))?,
                        conn,
                    )?))
                    // This is the update "in place" for collections without versions
                } else {
                    // Create new collection info
                    let update_col = Collection {
                        id: old_collection.id,
                        shared_version_id: old_collection.shared_version_id,
                        name: request.name,
                        description: request.description,
                        created_at: old_collection.created_at,
                        created_by: user_id,
                        version_id: None,
                        dataclass: old_collection.dataclass,
                        project_id: old_collection.project_id,
                    };
                    // Update the collection "in place"
                    update(collections).set(&update_col).execute(conn)?;

                    // return the new collectionoverview
                    Ok(map_to_collection_overview(Some(query_overview(
                        conn, update_col,
                    )?))?)
                }
            })?;

        // Return the collectionoverview returned from transaction
        Ok(UpdateCollectionResponse {
            collection: ret_collections,
        })
    }

    /// PinCollectionVersion creates a copy of the collection with a specific "pinned" version
    ///
    /// ## Arguments
    ///
    /// `PinCollectionVersionRequest` - Contains the collection_id and the new version.
    ///
    /// ## Results
    ///
    /// `PinCollectionVersionResponse` - Responds with an collection_overview for the new versioned collection.
    ///
    pub fn pin_collection_version(
        &self,
        request: PinCollectionVersionRequest,
        user_id: uuid::Uuid,
    ) -> Result<PinCollectionVersionResponse, ArunaError> {
        use crate::database::schema::collections::dsl::*;
        // Parse the old collection id
        let old_collection_id = uuid::Uuid::parse_str(&request.collection_id)?;
        // Execute the database transaction
        let ret_collections = self
            .pg_connection
            .get()?
            .transaction::<Option<CollectionOverview>, ArunaError, _>(|conn| {
                // Query the old collection
                let old_collection = collections
                    .filter(id.eq(old_collection_id))
                    .first::<Collection>(conn)?;
                // Get the old collection overview with labels, hooks etc.
                let old_overview = query_overview(conn, old_collection.clone())?;
                // If the old collection or the update creates a new "versioned" collection
                // -> This needs to perform a pin
                if old_collection.version_id.is_some() {
                    // Return error if old_version is >= new_version
                    // Updates must increase the semver
                    // Updates for "historic" versions are not allowed
                    if let Some(v) = &old_overview.coll_version {
                        if Version::from(v.clone()) >= request.version.clone().unwrap() {
                            return Err(ArunaError::InvalidRequest(
                                "New version must be greater than old one".to_string(),
                            ));
                        }
                    }
                }
                // Build the new version
                let new_version = request
                    .version
                    .clone()
                    .map(|v| v.into_collection_version(uuid::Uuid::new_v4()))
                    .unwrap();
                // Create new collection database struct
                let new_coll = Collection {
                    id: uuid::Uuid::new_v4(),
                    shared_version_id: old_collection.shared_version_id,
                    name: old_collection.name,
                    description: old_collection.description,
                    created_at: chrono::Utc::now().naive_utc(),
                    created_by: user_id,
                    version_id: Some(new_version.id),
                    dataclass: old_collection.dataclass,
                    project_id: old_collection.project_id,
                };
                // Pin the collection and return the overview
                Ok(Some(pin_collection_to_version(
                    old_collection_id,
                    user_id,
                    transform_collection_overviewdb(new_coll, new_version, Some(old_overview))?,
                    conn,
                )?))
            })?;
        // Return the collectionoverview to the grpc function
        Ok(PinCollectionVersionResponse {
            collection: ret_collections,
        })
    }

    /// DeleteCollection will delete a specific collection including its contents.
    ///
    /// Note: This does not delete the objects permanently it adds them just to a "trashable" status
    ///       Might in the future implement a rollback function
    ///
    /// ## Arguments
    ///
    /// `DeleteCollectionRequest` - Collection_id, project_id and force bool
    ///
    /// ## Results
    ///
    /// `DeleteCollectionResponse` - Placeholder this response is currently empty, which means success.
    ///
    pub fn delete_collection(
        &self,
        request: DeleteCollectionRequest,
        user_id: uuid::Uuid,
    ) -> Result<DeleteCollectionResponse, ArunaError> {
        // Import of database structures
        use crate::database::schema::collection_key_value::dsl as colkv;
        use crate::database::schema::collection_object_groups::dsl as colobjgrp;
        use crate::database::schema::collection_objects::dsl as colobj;
        use crate::database::schema::collections::dsl as col;
        use crate::database::schema::object_group_key_value::dsl as objgrpkv;
        use crate::database::schema::object_group_objects::dsl as objgrpobj;
        use crate::database::schema::object_groups::dsl as objgrp;
        use crate::database::schema::objects::dsl as obj;
        // Parse the collection_id string to uuid
        let collection_id = uuid::Uuid::parse_str(&request.collection_id)?;
        // Execute the request in transaction
        self.pg_connection.get()?.transaction::<_, ArunaError, _>(|conn| {
            // Query all object references
            let all_obj_references = colobj::collection_objects
                .filter(colobj::collection_id.eq(collection_id))
                .load::<CollectionObject>(conn)?;
            // Query the object_ids of all writeable references
            let all_obj_ids = all_obj_references
                .iter()
                .map(|elem| elem.object_id)
                .collect::<Vec<_>>();
            // Query all other object references for the specified object ids
            let all_other_references = colobj::collection_objects
                .filter(colobj::object_id.eq_any(all_obj_ids))
                .filter(colobj::collection_id.ne(collection_id))
                .load::<CollectionObject>(conn)?;

            // Create hashmap with key = object_uuid and value all associated mappings
            // This is needed to check if the object is referenced somewhere else
            let mut objects_referenced = HashMap::new();
            // Iterate through all returned references
            for specific_ref in all_obj_references {
                // Temporary vec that contains all references beginning with the reference for this collection
                let mut shared = vec![specific_ref.clone()];
                // Iterate all other references
                for all_ref in &all_other_references {
                    // If both object_ids are the same -> they reference the same object
                    if specific_ref.object_id == all_ref.object_id {
                        // Append to reference vector
                        shared.push(all_ref.to_owned());
                        // If the reference vector is greater than 1 and force is not used
                        // This means the data is owned by this collection and referenced somewhere else
                        if shared.len() > 1 && !request.force && specific_ref.writeable {
                            return Err(
                                ArunaError::InvalidRequest(
                                    "Can not delete collection because of dangling objects. Transfer ownership manually or use force".to_string()
                                )
                            );
                        }
                    }
                }
                // Insert the object_id and vector with associated references to the hashmap
                // Each value vector should contain at least one element -> the associated reference from this collection
                objects_referenced.insert(specific_ref.object_id, shared);
            }
            // Create a list of all "trashable" objects
            let trash = objects_referenced.iter().fold(Vec::new(), |mut trash, (k, v)| {
                // If the object is writeable by this collection
                // Append it to the list of trashable objects
                if v[0].writeable {
                    trash.push(k.to_owned());
                }
                trash
            });
            // Delete all references
            delete(
                colobj::collection_objects.filter(colobj::collection_id.eq(collection_id))
            ).execute(conn)?;
            // Delete all collection object groups belonging to this collection
            let deleted_objgrp = delete(
                colobjgrp::collection_object_groups.filter(
                    colobjgrp::collection_id.eq(collection_id)
                )
            ).load::<CollectionObjectGroup>(conn)?;
            // Filter out the objgrpids
            let objgrpids = deleted_objgrp
                .iter()
                .map(|elem| elem.object_group_id)
                .collect::<Vec<_>>();
            // Delete objgrpobjects
            delete(
                objgrpobj::object_group_objects.filter(
                    objgrpobj::object_group_id.eq_any(&objgrpids)
                )
            ).execute(conn)?;
            // Delete objgrp_kv
            delete(
                objgrpkv::object_group_key_value.filter(
                    objgrpkv::object_group_id.eq_any(&objgrpids)
                )
            ).execute(conn)?;
            // Delete all objgrps -> Objgrps for now are bound to a collection
            delete(objgrp::object_groups.filter(objgrp::id.eq_any(&objgrpids))).execute(conn)?;
            // Update all "owned/writeable" objects to be "TRASH" -> The trash routine should update all other objectgroups
            update(obj::objects.filter(obj::id.eq_any(&trash)))
                .set((obj::object_status.eq(ObjectStatus::TRASH), obj::created_by.eq(user_id)))
                .execute(conn)?;
            // Delete all collection_key_values
            delete(
                colkv::collection_key_value.filter(colkv::collection_id.eq(collection_id))
            ).execute(conn)?;
            // Delete the collection
            delete(col::collections.filter(col::id.eq(collection_id))).execute(conn)?;
            Ok(())
        })?;
        Ok(DeleteCollectionResponse {})
    }
}

/* ----------------- Section for collection specific helper functions ------------------- */

/// This is a helper function that queries and builds a CollectionOverviewDb based on an existing collection query
/// Used to query all associated Tables and map them to one consistent struct.
/// TODO: Optimize with belonging to
///
/// ## Arguments
///
/// *  conn: &mut PooledConnection<ConnectionManager<PgConnection>>, Database connection
/// *  col: Collection, collection information
///
/// ## Returns
///
/// * Result<CollectionOverviewDb, Error>: Returns an CollectionOverviewDb based on all query results
///
fn query_overview(
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
    col: Collection,
) -> Result<CollectionOverviewDb, Error> {
    // Database imports
    use crate::database::schema::collection_key_value::dsl as ckv;
    use crate::database::schema::collection_stats::dsl as clstats;
    use crate::database::schema::collection_version::dsl as clversion;
    use crate::database::schema::required_labels::dsl as rlbl;
    use diesel::prelude::*;
    // Query collection key_values
    let collection_key_values = ckv::collection_key_value
        .filter(ckv::collection_id.eq(col.id))
        .load::<models::collection::CollectionKeyValue>(conn)
        .optional()?;
    // Query required labels
    let req_labels = rlbl::required_labels
        .filter(rlbl::collection_id.eq(col.id))
        .load::<models::collection::RequiredLabel>(conn)
        .optional()?;
    // Query stats
    let stats = clstats::collection_stats
        .filter(clstats::id.eq(col.id))
        .first::<CollectionStat>(conn)
        .optional()?;
    // Query the version if a foreign key exists
    if let Some(cl_version) = col.version_id {
        let version = clversion::collection_version
            .filter(clversion::id.eq(cl_version))
            .first::<CollectionVersion>(conn)?;
        // Return CollectionOverviewdb
        Ok(CollectionOverviewDb {
            coll: col,
            coll_key_value: collection_key_values,
            coll_req_labels: req_labels,
            coll_stats: stats,
            coll_version: Some(version),
        })
    } else {
        // Return CollectionOverviewDb
        Ok(CollectionOverviewDb {
            coll: col,
            coll_key_value: collection_key_values,
            coll_req_labels: req_labels,
            coll_stats: stats,
            coll_version: None,
        })
    }
}

/// This is a helper function that maps different database information to a grpc collection_overview
///
/// ## Arguments
///
/// * coll_infos:
/// Option<CollectionOverviewDb> All collection associated tables (key_values, version, ontology etc.) in one struct.
///
/// ## Returns
///
/// * Result<Option<CollectionOverview>, ArunaError>: Returns an Option<CollectionOverview> (gRPC) or ArunaError
///
fn map_to_collection_overview(
    coll_infos: Option<CollectionOverviewDb>,
) -> Result<Option<CollectionOverview>, ArunaError> {
    // If coll_infos is Some()
    if let Some(ret_coll) = coll_infos {
        // Map database key values to two lists for labels and hooks
        let (labels, hooks) = if let Some(ret_kv) = ret_coll.coll_key_value {
            from_key_values(ret_kv)
        } else {
            (Vec::new(), Vec::new())
        };
        // Map the LabelOntology
        let label_ont = ret_coll.coll_req_labels.map(|req_labels| LabelOntology {
            required_label_keys: req_labels
                .iter()
                .map(|val| val.label_key.to_string())
                .collect::<Vec<String>>(),
        });
        // Parse the timestamp
        let tstmpt = naivedatetime_to_prost_time(ret_coll.coll.created_at)?;
        // Parse the stats to gRPC format
        let stats: Result<Option<CollectionStats>, ArunaError> =
            if let Some(sts) = ret_coll.coll_stats {
                let obj_stats = Stats {
                    count: sts.object_count,
                    acc_size: sts.size,
                };

                let coll_stats = CollectionStats {
                    object_stats: Some(obj_stats),
                    object_group_count: sts.object_group_count,
                    last_updated: Some(naivedatetime_to_prost_time(sts.last_updated)?),
                };

                Ok(Some(coll_stats))
            } else {
                Ok(None)
            };
        // Check if collection is public
        let is_public = matches!(
            ret_coll.coll.dataclass,
            Some(models::enums::Dataclass::PUBLIC)
        );
        // Map the collectionversion
        let mapped_version = match ret_coll.coll_version {
            Some(vers) => Some(collection_overview::Version::SemanticVersion(vers.into())),
            None => Some(collection_overview::Version::Latest(true)),
        };
        // Return gRPC collectionoverview
        Ok(Some(CollectionOverview {
            id: ret_coll.coll.id.to_string(),
            name: ret_coll.coll.name,
            description: ret_coll.coll.description,
            labels,
            hooks,
            label_ontology: label_ont,
            created: Some(tstmpt),
            stats: stats?,
            is_public,
            version: mapped_version,
        }))
        // Return none if input was None
    } else {
        Ok(None)
    }
}

/// Helper function that transforms an "old" CollectionOverviewDB to a "new" one. It creates new uuids for each label / hook / requiredlabel
/// and changes the association the the "new" collection uuid. This request is intended to be used if a new version should be created.
/// Therefore a "new_version" is required this "version" must be a higher number than the previous version.
///
/// ## Arguments
///
/// * new_coll: Collection, Collection stats for the "new" collection
/// * new_version: CollectionVersion, New semver for collection
/// * coll_infos: Option<CollectionOverviewDb>, "old" collection overview if this is None the function will Error
///
/// ## Returns
///
/// * Result<CollectionOverviewDb>, ArunaError>: Returns a CollectionOverviewDb or an Error
fn transform_collection_overviewdb(
    new_coll: Collection,
    new_version: CollectionVersion,
    coll_infos: Option<CollectionOverviewDb>,
) -> Result<CollectionOverviewDb, ArunaError> {
    // If coll_infos is Some
    if let Some(cinfos) = coll_infos {
        // Transform collectionoverview db to new ids
        Ok(CollectionOverviewDb {
            coll: new_coll.clone(),
            // Modify key_values
            coll_key_value: cinfos.coll_key_value.map(|mut collinfo_kv| {
                collinfo_kv
                    .iter_mut()
                    .map(|elem| {
                        elem.id = uuid::Uuid::new_v4();
                        elem.collection_id = new_coll.id;
                        elem.to_owned()
                    })
                    .collect::<Vec<_>>()
            }),
            // Modify required labels
            coll_req_labels: cinfos.coll_req_labels.map(|mut collreq_label| {
                collreq_label
                    .iter_mut()
                    .map(|elem| {
                        elem.id = uuid::Uuid::new_v4();
                        elem.collection_id = new_coll.id;
                        elem.to_owned()
                    })
                    .collect::<Vec<_>>()
            }),
            // Stats are not needed
            coll_stats: None,
            // Version should be transformed beforehand
            coll_version: Some(new_version),
        })
        // Return error if transformation failed
    } else {
        Err(ArunaError::InvalidRequest(
            "CollectionOverviewDb failed".to_string(),
        ))
    }
}

/// Helper function that pins a specific collection to a specific version (or creates a new one). Pinning is a quite complex process that might take a while.
///
/// ## Behaviour
/// Sucessfully pinning a specific collection requires the following steps.
/// WARNING: This process is quite compute heavy and must make several database requests.
/// This might take a significant time for large collections.
///
/// - Query all information from "origin" collection including (KeyValues, Objects, Objectgroups etc.)
/// - Create a new collection as copy from the original one.
/// - Clone all objects from old to new collection.
/// - Clone all objectgroups to new collection
/// - Add correct mappings for collection objectgroups etc.
///
/// ## Arguments
///
/// * origin_collection: uuid::Uuid, The original collection that should be cloned and pinned to a specific version
/// * creator_user: uuid::Uuid, The user that initiated the pin
/// * new_collection_overview: CollectionOverviewDb, The CollectionOverviewDb that describes the basic attributes of the new collection
///
/// ## Returns
///
/// * Result<Option<CollectionOverview>, ArunaError>: Returns a CollectionOverview or an Error
///
fn pin_collection_to_version(
    origin_collection: uuid::Uuid,
    creator_user: uuid::Uuid,
    new_collection_overview: CollectionOverviewDb,
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
) -> Result<CollectionOverview, ArunaError> {
    // Imports for all collection related tables
    use crate::database::schema::collection_key_value::dsl as ckv;
    use crate::database::schema::collection_object_groups::dsl as colobjgrp;
    use crate::database::schema::collection_objects::dsl as clobj;
    use crate::database::schema::collection_version::dsl as clversion;
    use crate::database::schema::collections::dsl as col;
    use crate::database::schema::object_group_key_value::dsl as objgrpkv;
    use crate::database::schema::object_group_objects::dsl as objgrpobj;
    use crate::database::schema::object_groups::dsl as objgrp;
    use crate::database::schema::objects::dsl as obj;
    use crate::database::schema::required_labels::dsl as rlbl;
    use diesel::prelude::*;
    // Query the original objects from the origin collection
    let original_objects: Vec<Object> = clobj::collection_objects
        .filter(clobj::collection_id.eq(origin_collection))
        .inner_join(obj::objects)
        .select(Object::as_select())
        .load::<Object>(conn)?;
    // Get ids of original objects
    let original_objects_ids = original_objects
        .iter()
        .map(|elem| elem.id)
        .collect::<Vec<_>>();
    // Get all original object_groups
    let original_object_groups: Vec<ObjectGroup> = colobjgrp::collection_object_groups
        .inner_join(objgrp::object_groups)
        .filter(colobjgrp::collection_id.eq(origin_collection))
        .select(ObjectGroup::as_select())
        .load::<ObjectGroup>(conn)?;
    // Map full object_groups to list of uuids
    let original_object_group_ids = original_object_groups
        .iter()
        .map(|elem| elem.id)
        .collect::<Vec<_>>();
    // Get all associations between objects <-> objectgroups
    let objectgrp_associations = objgrpobj::object_group_objects
        .filter(objgrpobj::object_group_id.eq_any(original_object_group_ids.clone()))
        .load::<ObjectGroupObject>(conn)?;
    // Get objectgroup key values
    let original_obj_grp_kv = objgrpkv::object_group_key_value
        .filter(objgrpkv::object_group_id.eq_any(original_object_group_ids))
        .load::<ObjectGroupKeyValue>(conn)?;
    // Inserts for collection
    // First collection
    insert_into(col::collections)
        .values(&new_collection_overview.coll)
        .execute(conn)?;
    // Collection_key_values
    if let Some(kv) = &new_collection_overview.coll_key_value {
        insert_into(ckv::collection_key_value)
            .values(kv)
            .execute(conn)?;
    }
    // Insert new version
    if let Some(vers) = &new_collection_overview.coll_version {
        insert_into(clversion::collection_version)
            .values(vers)
            .execute(conn)?;
    }
    // Insert required labels
    if let Some(req_labels) = &new_collection_overview.coll_req_labels {
        insert_into(rlbl::required_labels)
            .values(req_labels)
            .execute(conn)?;
    }
    // List with new objectgroups
    let mut new_obj_groups = Vec::new();
    // List with new objects
    // Only used as information inserts are done via clone_obj request
    let mut new_objects = Vec::new();
    // List with new object_group_key_values
    let mut new_object_group_kv = Vec::new();
    // List with new collection_object_groups
    let mut new_coll_obj_groups = Vec::new();
    // List with new object_group_objects
    let mut new_obj_grp_objs = Vec::new();
    // Mapping table with key = original_uuid and value = new_uuid
    let mut object_mapping_table = HashMap::new();
    // Mapping table for objectgroups with key = original_uuid and value = new_uuid
    let mut object_group_mappings = HashMap::new();
    // Clone each object from old to new collection
    for obj_id in original_objects_ids {
        let new_obj = clone_object(
            conn,
            obj_id,
            origin_collection,
            new_collection_overview.coll.id,
        )?;
        object_mapping_table.insert(obj_id, uuid::Uuid::parse_str(&new_obj.id)?);
        new_objects.push(new_obj);
    }
    // Iterate through objectgroups
    for obj_grp in original_object_groups {
        // Create copy of objectgroup
        let new_uuid = uuid::Uuid::new_v4();
        let new_objgrp = ObjectGroup {
            id: new_uuid,
            shared_revision_id: uuid::Uuid::new_v4(),
            revision_number: 0,
            name: obj_grp.name,
            description: obj_grp.description,
            created_at: chrono::Utc::now().naive_utc(),
            created_by: creator_user,
        };
        // Add coll_obj_group to vec
        new_coll_obj_groups.push(CollectionObjectGroup {
            id: uuid::Uuid::new_v4(),
            collection_id: new_collection_overview.coll.id,
            object_group_id: new_uuid,
            writeable: false, // Always not writeable -> version collections are immutable
        });

        object_group_mappings.insert(obj_grp.id, new_uuid);
        new_obj_groups.push(new_objgrp);
    }
    // Copy key values of objectgroup
    for obj_grp_kv in original_obj_grp_kv {
        new_object_group_kv.push(ObjectGroupKeyValue {
            id: uuid::Uuid::new_v4(),
            object_group_id: *object_group_mappings
                .get(&obj_grp_kv.object_group_id)
                .unwrap(),
            key: obj_grp_kv.key,
            value: obj_grp_kv.value,
            key_value_type: obj_grp_kv.key_value_type,
        });
    }
    // Copy associations based on mappings old_uuid <-> new_uuid
    for association in objectgrp_associations {
        new_obj_grp_objs.push(ObjectGroupObject {
            id: uuid::Uuid::new_v4(),
            object_group_id: *object_group_mappings
                .get(&association.object_group_id)
                .unwrap(),
            object_id: *object_mapping_table.get(&association.object_id).unwrap(),
            is_meta: association.is_meta,
        });
    }
    // Insert objectgroups
    if !new_obj_groups.is_empty() {
        insert_into(objgrp::object_groups)
            .values(&new_obj_groups)
            .execute(conn)?;
    }
    // Insert object_group key values
    if !new_object_group_kv.is_empty() {
        insert_into(objgrpkv::object_group_key_value)
            .values(&new_object_group_kv)
            .execute(conn)?;
    }
    // Insert collectionobjectgroups
    if !new_coll_obj_groups.is_empty() {
        insert_into(colobjgrp::collection_object_groups)
            .values(&new_coll_obj_groups)
            .execute(conn)?;
    }
    // Insert objectgroupobjects
    if !new_obj_grp_objs.is_empty() {
        insert_into(objgrpobj::object_group_objects)
            .values(&new_obj_grp_objs)
            .execute(conn)?;
    }
    // Parse labels / hooks for return
    let (labels, hooks) = if let Some(kv) = new_collection_overview.coll_key_value {
        from_key_values(kv)
    } else {
        (Vec::new(), Vec::new())
    };
    // Map required labels for return
    let mapped_req_labels =
        new_collection_overview
            .coll_req_labels
            .map(|req_labels| LabelOntology {
                required_label_keys: req_labels
                    .iter()
                    .map(|elem| elem.label_key.clone())
                    .collect::<Vec<_>>(),
            });
    // Map version for return
    let mapped_version = new_collection_overview
        .coll_version
        .map(|v| CollectionVersiongRPC::SemanticVersion(v.into()));
    // Construct collectionoverview and return it
    Ok(CollectionOverview {
        id: new_collection_overview.coll.id.to_string(),
        name: new_collection_overview.coll.name,
        description: new_collection_overview.coll.description,
        labels,
        hooks,
        label_ontology: mapped_req_labels,
        created: None,
        stats: None,
        is_public: matches!(
            new_collection_overview.coll.dataclass,
            Some(models::enums::Dataclass::PUBLIC)
        ),
        version: mapped_version,
    })
}

/// Implement `from` CollectionVersion for gRPC "Version"
/// This makes it easier to convert gRPC to database version types
impl From<CollectionVersion> for Version {
    fn from(cver: CollectionVersion) -> Self {
        Version {
            major: cver.major as i32,
            minor: cver.minor as i32,
            patch: cver.patch as i32,
        }
    }
}
/// Implement partial ord for gRPC version
/// This allows us to compare versions with `<`, `>` etc.
impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.major.partial_cmp(&other.major) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => {
                return ord;
            }
        }
        match self.minor.partial_cmp(&other.minor) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => {
                return ord;
            }
        }
        self.patch.partial_cmp(&other.patch)
    }
}

/// Function that converts a gRPC version to a database collection version
/// This function needs a uuid to correctly map
impl Version {
    fn into_collection_version(self, version_id: uuid::Uuid) -> CollectionVersion {
        CollectionVersion {
            id: version_id,
            major: self.major.into(),
            minor: self.minor.into(),
            patch: self.patch.into(),
        }
    }
}

// Convert dataclass from gRPC to DBDataclass
impl From<DataClass> for DBDataclass {
    fn from(grpc: DataClass) -> Self {
        match grpc {
            DataClass::Unspecified => DBDataclass::PRIVATE,
            DataClass::Public => DBDataclass::PUBLIC,
            DataClass::Private => DBDataclass::PRIVATE,
            DataClass::Confidential => DBDataclass::CONFIDENTIAL,
            DataClass::Protected => DBDataclass::PROTECTED,
        }
    }
}
