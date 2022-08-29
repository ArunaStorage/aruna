use super::utils::*;
use crate::api::aruna::api::storage::models::v1::{
    collection_overview, CollectionOverview, CollectionOverviews, CollectionStats, LabelOntology,
    Stats, Version,
};
use crate::api::aruna::api::storage::services::v1::{
    CreateNewCollectionRequest, CreateNewCollectionResponse, GetCollectionByIdRequest,
    GetCollectionByIdResponse, GetCollectionsRequest, GetCollectionsResponse,
    UpdateCollectionRequest, UpdateCollectionResponse,
};
use crate::database::connection::Database;
use crate::database::models;
use crate::database::models::collection::{
    Collection, CollectionKeyValue, CollectionVersion, RequiredLabel,
};
use crate::database::models::views::CollectionStat;
use crate::error::ArunaError;
use chrono::Local;
use diesel::insert_into;
use diesel::prelude::*;
use diesel::r2d2::ConnectionManager;
use diesel::result::Error;
use r2d2::PooledConnection;

#[derive(Debug)]
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
        _user_id: uuid::Uuid,
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

        let _old_collection_id = uuid::Uuid::parse_str(&request.collection_id)?;

        let created_overview = CollectionOverview {
            id: uuid::Uuid::new_v4(),
            name: request.name,
            description: request.description,
            labels: request.labels,
            hooks: request.hooks,
            label_ontology: todo!(),
            created: chrono::Utc::now(),
            stats: todo!(),
            is_public: todo!(),
            version: todo!(),
        };

        todo!();
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
            Some(vers) => Some(collection_overview::Version::SemanticVersion(Version {
                major: vers.major as i32,
                minor: vers.minor as i32,
                patch: vers.patch as i32,
            })),
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

fn _pin_collection_to_version(
    origin_collection: CollectionOverview,
    _new_collection: Collection,
    _conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
) -> Collection {
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

    todo!()
}
