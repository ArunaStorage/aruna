use super::utils::*;
use crate::api::aruna::api::storage::models::v1::DataClass;
use crate::api::aruna::api::storage::models::v1::{
    collection_overview, collection_overview::Version as CollectionVersiongRPC, CollectionOverview,
    CollectionOverviews, CollectionStats, LabelOntology, Stats, Version,
};
use crate::api::aruna::api::storage::services::v1::{
    CreateNewCollectionRequest, CreateNewCollectionResponse, GetCollectionByIdRequest,
    GetCollectionByIdResponse, GetCollectionsRequest, GetCollectionsResponse,
    PinCollectionVersionRequest, PinCollectionVersionResponse, UpdateCollectionRequest,
    UpdateCollectionResponse,
};
use crate::database::connection::Database;
use crate::database::crud::object::clone_object;
use crate::database::models;
use crate::database::models::collection::{
    Collection, CollectionKeyValue, CollectionObjectGroup, CollectionVersion, RequiredLabel,
};
use crate::database::models::enums::Dataclass as DBDataclass;
use crate::database::models::object::Object;
use crate::database::models::object_group::{ObjectGroup, ObjectGroupKeyValue, ObjectGroupObject};
use crate::database::models::views::CollectionStat;
use crate::error::ArunaError;
use chrono::Local;
use diesel::prelude::*;
use diesel::r2d2::ConnectionManager;
use diesel::result::Error;
use diesel::{insert_into, update};
use r2d2::PooledConnection;
use std::collections::HashMap;

#[derive(Debug, Clone)]
struct CollectionOverviewDb {
    coll: Collection,
    coll_key_value: Option<Vec<CollectionKeyValue>>,
    coll_req_labels: Option<Vec<RequiredLabel>>,
    coll_stats: Option<CollectionStat>,
    coll_version: Option<CollectionVersion>,
}

impl Database {
    pub fn create_new_collection(
        &self,
        request: CreateNewCollectionRequest,
        creator: uuid::Uuid,
    ) -> Result<CreateNewCollectionResponse, ArunaError> {
        use crate::database::schema::collection_key_value::dsl::*;
        use crate::database::schema::collections::dsl::*;

        let collection_uuid = uuid::Uuid::new_v4();

        let shared_version_uuid = uuid::Uuid::new_v4();

        let key_values = to_collection_key_values(request.labels, request.hooks, collection_uuid);

        let db_collection = models::collection::Collection {
            id: collection_uuid,
            shared_version_id: shared_version_uuid,
            name: request.name,
            description: request.description,
            created_by: creator,
            created_at: Local::now().naive_local(),
            version_id: None,
            dataclass: None,
            project_id: uuid::Uuid::parse_str(&request.project_id)?,
        };

        self.pg_connection
            .get()?
            .transaction::<_, Error, _>(|conn| {
                insert_into(collections)
                    .values(db_collection)
                    .execute(conn)?;
                // Get the API token, if this errors -> no corresponding database token object could be found
                insert_into(collection_key_value)
                    .values(key_values)
                    .execute(conn)?;

                Ok(())
            })?;

        Ok(CreateNewCollectionResponse {
            collection_id: collection_uuid.to_string(),
        })
    }

    pub fn get_collection_by_id(
        &self,
        request: GetCollectionByIdRequest,
    ) -> Result<GetCollectionByIdResponse, ArunaError> {
        use crate::database::schema::collections::dsl as col;
        use diesel::prelude::*;

        let collection_id = uuid::Uuid::parse_str(&request.collection_id)?;

        let ret_collection = self
            .pg_connection
            .get()?
            .transaction::<Option<CollectionOverviewDb>, Error, _>(|conn| {
                let collection_info = col::collections
                    .filter(col::id.eq(collection_id))
                    .first::<models::collection::Collection>(conn)
                    .optional()?;

                match collection_info {
                    Some(coll_info) => Ok(Some(query_overview(conn, coll_info)?)),
                    None => Ok(None),
                }
            })?;

        Ok(GetCollectionByIdResponse {
            collection: map_to_collection_overview(ret_collection)?,
        })
    }

    pub fn get_collections(
        &self,
        request: GetCollectionsRequest,
    ) -> Result<GetCollectionsResponse, ArunaError> {
        use crate::database::schema::collection_key_value::dsl as ckv;
        use crate::database::schema::collections::dsl as col;
        use diesel::prelude::*;

        let (pagesize, last_uuid) = parse_page_request(request.page_request, 20)?;
        let parsed_query = parse_query(request.label_or_id_filter)?;

        let project_id = uuid::Uuid::parse_str(&request.project_id)?;

        let ret_collections = self
            .pg_connection
            .get()?
            .transaction::<Option<Vec<CollectionOverviewDb>>, Error, _>(|conn| {
                let mut base_request = col::collections
                    .filter(col::project_id.eq(project_id))
                    .into_boxed();

                let mut return_vec: Vec<CollectionOverviewDb> = Vec::new();

                if let Some(pg_size) = pagesize {
                    base_request = base_request.limit(pg_size);
                }

                if let Some(l_uid) = last_uuid {
                    base_request = base_request.filter(col::id.ge(l_uid));
                }

                if let Some(p_query) = parsed_query {
                    match p_query {
                        ParsedQuery::LabelQuery(l_query) => {
                            let mut ckv_query = ckv::collection_key_value.into_boxed();

                            let found_cols: Option<Vec<uuid::Uuid>>;
                            // Is "and"
                            if l_query.1 {
                                for (key, value) in l_query.0.clone() {
                                    ckv_query = ckv_query.filter(ckv::key.eq(key));
                                    if let Some(val) = value {
                                        ckv_query = ckv_query.filter(ckv::value.eq(val))
                                    };
                                }

                                let found_cols_key_values: Option<Vec<CollectionKeyValue>> =
                                    ckv_query.load::<CollectionKeyValue>(conn).optional()?;

                                found_cols = check_all_for_db_kv(found_cols_key_values, l_query.0)
                            } else {
                                for (key, value) in l_query.0 {
                                    ckv_query = ckv_query.or_filter(ckv::key.eq(key));
                                    if let Some(val) = value {
                                        ckv_query = ckv_query.filter(ckv::value.eq(val))
                                    };
                                }
                                found_cols = ckv_query
                                    .select(ckv::collection_id)
                                    .load::<uuid::Uuid>(conn)
                                    .optional()?;
                            }

                            if let Some(fcolls) = found_cols {
                                base_request = base_request.filter(col::id.eq_any(fcolls))
                            } else {
                                return Ok(None);
                            }
                        }
                        ParsedQuery::IdsQuery(ids) => {
                            base_request = base_request.filter(col::id.eq_any(ids));
                        }
                    };
                };

                let query_collections: Option<Vec<Collection>> =
                    base_request.load::<Collection>(conn).optional()?;

                if let Some(q_colls) = query_collections {
                    for col in q_colls {
                        return_vec.push(query_overview(conn, col)?);
                    }
                    Ok(Some(return_vec))
                } else {
                    Ok(None)
                }
            })?;

        let coll_overviews = match ret_collections {
            Some(colls) => {
                let mut coll_overviews: Vec<CollectionOverview> = Vec::new();

                for col in colls {
                    if let Some(coll_overv) = map_to_collection_overview(Some(col))? {
                        coll_overviews.push(coll_overv);
                    };
                }

                Some(CollectionOverviews {
                    collection_overviews: coll_overviews,
                })
            }
            None => None,
        };

        Ok(GetCollectionsResponse {
            collections: coll_overviews,
        })
    }

    pub fn update_collection(
        &self,
        request: UpdateCollectionRequest,
        user_id: uuid::Uuid,
    ) -> Result<UpdateCollectionResponse, ArunaError> {
        // Todo: What needs to be done to update a collection ?
        //
        // 1. Get the existing collection.
        // 2. Is versioned or will be versioned ?
        //    Yes: Execute "Pin" routing
        //    No: Update collection in place
        //
        // (3.) Pin Routine:
        //   - Create new collection with identical infos
        //   - Clone all Objects & ObjectGroups including key-values (reference them immutably) set origin to old key/value id
        //   -
        //
        use crate::database::schema::collections::dsl::*;

        let old_collection_id = uuid::Uuid::parse_str(&request.collection_id)?;

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

                    let new_version =
                        option_new_version_grpc_to_db(request.version.clone()).unwrap();

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

                    Ok(Some(pin_collection_to_version(
                        old_collection_id,
                        user_id,
                        transform_collection_overviewdb(new_coll, new_version, Some(old_overview))?,
                        conn,
                    )?))
                } else {
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

                    update(collections).set(&update_col).execute(conn)?;

                    Ok(map_to_collection_overview(Some(query_overview(
                        conn, update_col,
                    )?))?)
                }
            })?;

        Ok(UpdateCollectionResponse {
            collection: ret_collections,
        })
    }
    pub fn pin_collection_version(
        &self,
        request: PinCollectionVersionRequest,
        user_id: uuid::Uuid,
    ) -> Result<PinCollectionVersionResponse, ArunaError> {
        use crate::database::schema::collections::dsl::*;

        let old_collection_id = uuid::Uuid::parse_str(&request.collection_id)?;
        let ret_collections = self
            .pg_connection
            .get()?
            .transaction::<Option<CollectionOverview>, ArunaError, _>(|conn| {
                // Query the old collection
                let old_collection = collections
                    .filter(id.eq(old_collection_id))
                    .first::<Collection>(conn)?;
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
                let new_version = option_new_version_grpc_to_db(request.version.clone()).unwrap();

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

                Ok(Some(pin_collection_to_version(
                    old_collection_id,
                    user_id,
                    transform_collection_overviewdb(new_coll, new_version, Some(old_overview))?,
                    conn,
                )?))
            })?;
        Ok(PinCollectionVersionResponse {
            collection: ret_collections,
        })
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
    use crate::database::schema::collection_key_value::dsl as ckv;
    use crate::database::schema::collection_stats::dsl as clstats;
    use crate::database::schema::collection_version::dsl as clversion;
    use crate::database::schema::required_labels::dsl as rlbl;
    use diesel::prelude::*;
    let collection_key_values = ckv::collection_key_value
        .filter(ckv::collection_id.eq(col.id))
        .load::<models::collection::CollectionKeyValue>(conn)
        .optional()?;

    let req_labels = rlbl::required_labels
        .filter(rlbl::collection_id.eq(col.id))
        .load::<models::collection::RequiredLabel>(conn)
        .optional()?;

    let stats = clstats::collection_stats
        .filter(clstats::id.eq(col.id))
        .first::<CollectionStat>(conn)
        .optional()?;

    if let Some(cl_version) = col.version_id {
        let version = clversion::collection_version
            .filter(clversion::id.eq(cl_version))
            .first::<CollectionVersion>(conn)?;

        Ok(CollectionOverviewDb {
            coll: col,
            coll_key_value: collection_key_values,
            coll_req_labels: req_labels,
            coll_stats: stats,
            coll_version: Some(version),
        })
    } else {
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
    if let Some(ret_coll) = coll_infos {
        let (labels, hooks) = if let Some(ret_kv) = ret_coll.coll_key_value {
            from_collection_key_values(ret_kv)
        } else {
            (Vec::new(), Vec::new())
        };

        let label_ont = ret_coll.coll_req_labels.map(|req_labels| LabelOntology {
            required_label_keys: req_labels
                .iter()
                .map(|val| val.label_key.to_string())
                .collect::<Vec<String>>(),
        });

        let tstmpt = naivedatetime_to_prost_time(ret_coll.coll.created_at)?;

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

        let is_public = matches!(
            ret_coll.coll.dataclass,
            Some(models::enums::Dataclass::PUBLIC)
        );

        let mapped_version = match ret_coll.coll_version {
            Some(vers) => Some(collection_overview::Version::SemanticVersion(vers.into())),
            None => Some(collection_overview::Version::Latest(true)),
        };

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
    if let Some(cinfos) = coll_infos {
        Ok(CollectionOverviewDb {
            coll: new_coll.clone(),
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
            coll_stats: None,
            coll_version: Some(new_version),
        })
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
    // Todo:
    //
    // Query all objects and objectgroups from the "origin collection". Create mapping table with
    // old object <-> objectgroup associations
    //
    // - Create the new collection.
    // - Clone all objects to new collection
    // - Clone all object_groups from old collection to new collection (without objects)
    // - Map object <-> object_group associations to new collection
    //
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
    let original_objects: Vec<Object> = clobj::collection_objects
        .filter(clobj::collection_id.eq(origin_collection))
        .inner_join(obj::objects)
        .select(Object::as_select())
        .load::<Object>(conn)?;
    let original_objects_ids = original_objects
        .iter()
        .map(|elem| elem.id)
        .collect::<Vec<_>>();

    let original_object_groups: Vec<ObjectGroup> = colobjgrp::collection_object_groups
        .inner_join(objgrp::object_groups)
        .filter(colobjgrp::collection_id.eq(origin_collection))
        .select(ObjectGroup::as_select())
        .load::<ObjectGroup>(conn)?;

    let original_object_group_ids = original_object_groups
        .iter()
        .map(|elem| elem.id)
        .collect::<Vec<_>>();

    let objectgrp_associations = objgrpobj::object_group_objects
        .filter(objgrpobj::object_group_id.eq_any(original_object_group_ids.clone()))
        .load::<ObjectGroupObject>(conn)?;

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
    };
    if let Some(vers) = &new_collection_overview.coll_version {
        insert_into(clversion::collection_version)
            .values(vers)
            .execute(conn)?;
    };
    if let Some(req_labels) = &new_collection_overview.coll_req_labels {
        insert_into(rlbl::required_labels)
            .values(req_labels)
            .execute(conn)?;
    };

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

    for obj_grp in original_object_groups {
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

    if !new_obj_groups.is_empty() {
        insert_into(objgrp::object_groups)
            .values(&new_obj_groups)
            .execute(conn)?;
    }

    if !new_object_group_kv.is_empty() {
        insert_into(objgrpkv::object_group_key_value)
            .values(&new_object_group_kv)
            .execute(conn)?;
    }

    if !new_coll_obj_groups.is_empty() {
        insert_into(colobjgrp::collection_object_groups)
            .values(&new_coll_obj_groups)
            .execute(conn)?;
    }
    if !new_obj_grp_objs.is_empty() {
        insert_into(objgrpobj::object_group_objects)
            .values(&new_obj_grp_objs)
            .execute(conn)?;
    }

    let (labels, hooks) = if let Some(kv) = new_collection_overview.coll_key_value {
        from_collection_key_values(kv)
    } else {
        (Vec::new(), Vec::new())
    };

    let mapped_req_labels =
        new_collection_overview
            .coll_req_labels
            .map(|req_labels| LabelOntology {
                required_label_keys: req_labels
                    .iter()
                    .map(|elem| elem.label_key.clone())
                    .collect::<Vec<_>>(),
            });

    let mapped_version = new_collection_overview
        .coll_version
        .map(|v| CollectionVersiongRPC::SemanticVersion(v.into()));

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

impl From<CollectionVersion> for Version {
    fn from(cver: CollectionVersion) -> Self {
        Version {
            major: cver.major as i32,
            minor: cver.minor as i32,
            patch: cver.patch as i32,
        }
    }
}

impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.major.partial_cmp(&other.major) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.minor.partial_cmp(&other.minor) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.patch.partial_cmp(&other.patch)
    }
}

impl Version {
    fn new_coll_db_version(&self) -> CollectionVersion {
        CollectionVersion {
            id: uuid::Uuid::new_v4(),
            major: self.major.into(),
            minor: self.minor.into(),
            patch: self.patch.into(),
        }
    }
}

fn option_new_version_grpc_to_db(from_version: Option<Version>) -> Option<CollectionVersion> {
    from_version.map(|v| v.new_coll_db_version())
}

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
