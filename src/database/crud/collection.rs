use super::utils::*;
use crate::api::aruna::api::storage::models::v1::{
    collection, collection_overview, label_or_id_query, CollectionOverview, CollectionStats,
    KeyValue, LabelOntology, Stats, Version,
};
use crate::api::aruna::api::storage::services::v1::{
    CreateNewCollectionRequest, CreateNewCollectionResponse, GetCollectionByIdRequest,
    GetCollectionByIdResponse, GetCollectionsRequest, GetCollectionsResponse,
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
use r2d2::PooledConnection;

impl Database {
    pub fn create_new_collection(
        &self,
        request: CreateNewCollectionRequest,
        creator: uuid::Uuid,
    ) -> Result<CreateNewCollectionResponse, ArunaError> {
        use crate::database::schema::collection_key_value::dsl::*;
        use crate::database::schema::collections::dsl::*;
        use diesel::result::Error;

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
        use crate::database::schema::collection_key_value::dsl as ckv;
        use crate::database::schema::collection_stats::dsl as clstats;
        use crate::database::schema::collection_version::dsl as clversion;
        use crate::database::schema::collections::dsl as col;
        use crate::database::schema::required_labels::dsl as rlbl;
        use diesel::prelude::*;
        use diesel::result::Error;

        let collection_id = uuid::Uuid::parse_str(&request.collection_id)?;

        let ret_collection = self.pg_connection.get()?.transaction::<Option<(
            Collection,
            Option<Vec<CollectionKeyValue>>,
            Option<Vec<RequiredLabel>>,
            Option<CollectionStat>,
            Option<CollectionVersion>,
        )>, Error, _>(|conn| {
            let collection_info = col::collections
                .filter(col::id.eq(collection_id))
                .first::<models::collection::Collection>(conn)
                .optional()?;

            match collection_info {
                Some(coll_info) => {
                    let collection_key_values = ckv::collection_key_value
                        .filter(ckv::collection_id.eq(collection_id))
                        .load::<models::collection::CollectionKeyValue>(conn)
                        .optional()?;

                    let req_labels = rlbl::required_labels
                        .filter(rlbl::collection_id.eq(collection_id))
                        .load::<models::collection::RequiredLabel>(conn)
                        .optional()?;

                    let stats = clstats::collection_stats
                        .filter(clstats::id.eq(collection_id))
                        .first::<CollectionStat>(conn)
                        .optional()?;

                    if let Some(cl_version) = coll_info.version_id {
                        let version = clversion::collection_version
                            .filter(clversion::id.eq(cl_version))
                            .first::<CollectionVersion>(conn)?;

                        return Ok(Some((
                            coll_info,
                            collection_key_values,
                            req_labels,
                            stats,
                            Some(version),
                        )));
                    } else {
                        return Ok(Some((
                            coll_info,
                            collection_key_values,
                            req_labels,
                            stats,
                            None,
                        )));
                    }
                }
                None => return Ok(None),
            };
        })?;

        Ok(GetCollectionByIdResponse {
            collection: map_to_collection_overview(ret_collection)?,
        })
    }

    pub fn get_collections(
        &self,
        request: GetCollectionsRequest,
        project_id: uuid::Uuid,
    ) -> Result<GetCollectionsResponse, ArunaError> {
        use crate::database::schema::collection_key_value::dsl as ckv;
        use crate::database::schema::collection_stats::dsl as clstats;
        use crate::database::schema::collection_version::dsl as clversion;
        use crate::database::schema::collections::dsl as col;
        use crate::database::schema::required_labels::dsl as rlbl;
        use diesel::prelude::*;
        use diesel::result::Error;

        let ret_collection = self.pg_connection.get()?.transaction::<Option<(
            Collection,
            Option<Vec<CollectionKeyValue>>,
            Option<Vec<RequiredLabel>>,
            Option<CollectionStat>,
            Option<CollectionVersion>,
        )>, Error, _>(|conn| {
            let collection_info = col::collections
                .filter(col::id.eq(collection_id))
                .first::<models::collection::Collection>(conn)
                .optional()?;

            match collection_info {
                Some(coll_info) => {
                    let collection_key_values = ckv::collection_key_value
                        .filter(ckv::collection_id.eq(collection_id))
                        .load::<models::collection::CollectionKeyValue>(conn)
                        .optional()?;

                    let req_labels = rlbl::required_labels
                        .filter(rlbl::collection_id.eq(collection_id))
                        .load::<models::collection::RequiredLabel>(conn)
                        .optional()?;

                    let stats = clstats::collection_stats
                        .filter(clstats::id.eq(collection_id))
                        .first::<CollectionStat>(conn)
                        .optional()?;

                    if let Some(cl_version) = coll_info.version_id {
                        let version = clversion::collection_version
                            .filter(clversion::id.eq(cl_version))
                            .first::<CollectionVersion>(conn)?;

                        return Ok(Some((
                            coll_info,
                            collection_key_values,
                            req_labels,
                            stats,
                            Some(version),
                        )));
                    } else {
                        return Ok(Some((
                            coll_info,
                            collection_key_values,
                            req_labels,
                            stats,
                            None,
                        )));
                    }
                }
                None => return Ok(None),
            };
        })?;

        Ok(GetCollectionByIdResponse {
            collection: map_to_collection_overview(ret_collection)?,
        })
    }
}

/* ----------------- Section for collection specific helper functions ------------------- */

fn build_get_collection_query(
    req: GetCollectionsRequest,
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
) -> Result<Option<Vec<Collection>>, ArunaError> {
    use crate::database::schema::collection_key_value::dsl as ckv;
    use crate::database::schema::collection_stats::dsl as clstats;
    use crate::database::schema::collection_version::dsl as clversion;
    use crate::database::schema::collections::dsl as col;
    use crate::database::schema::required_labels::dsl as rlbl;
    use diesel::prelude::*;
    use diesel::result::Error;

    let proj_id = uuid::Uuid::parse_str(&req.project_id)?;

    let (pagesize, last_id) = match req.page_request {
        Some(p_req) => {
            if p_req.last_uuid.is_empty() {
                if p_req.page_size >= 0 {
                    (20, None)
                } else {
                    (p_req.page_size, None)
                }
            } else {
                if p_req.page_size >= 0 {
                    (20, Some(uuid::Uuid::parse_str(&p_req.last_uuid)?))
                } else {
                    (
                        p_req.page_size,
                        Some(uuid::Uuid::parse_str(&p_req.last_uuid)?),
                    )
                }
            }
        }
        None => (20, None),
    };

    let (id_list, labels) = if let Some(l_filter) = req.label_filter {
        if let Some(quer) = l_filter.query {
            match quer {
                label_or_id_query::Query::Labels(label_filter) => {
                    (None, Some((label_filter.labels, label_filter.r#type)))
                }
                label_or_id_query::Query::Ids(ids) => (Some(ids.ids), None),
            }
        } else {
            (None, None)
        }
    } else {
        (None, None)
    };

    match req.label_filter {
        Some(filter) => match filter.query {
            Some(grpc_query) => match grpc_query {
                label_or_id_query::Query::Labels(_) => todo!(),
                label_or_id_query::Query::Ids(_) => todo!(),
            },
            None => Ok(col::collections
                .filter(col::project_id.eq(proj_id))
                .limit(pagesize)
                .load::<models::collection::Collection>(conn)
                .optional()?),
        },

        None => Ok(col::collections
            .filter(col::project_id.eq(proj_id))
            .limit(pagesize)
            .load::<models::collection::Collection>(conn)
            .optional()?),
    }
}

/// This is a helper function that maps different database information to a grpc collection_overview
///
/// ## Arguments
///
/// * coll_infos:
/// Option                                  Optional, might be None
/// <(
///     Collection,                         Database collection info
///     Option<Vec<CollectionKeyValue>>,    Optional Vector with key_values -> (labels, hooks)
///     Option<Vec<RequiredLabel>>,         Optional Vector with required_labels -> LabelOntology
///     Option<CollectionStat>,             Optional CollectionStats (This can only be None for freshly created collections)
///     Option<CollectionVersion>,          Optional Version, if no version is present -> latest
/// )>
///
/// ## Returns
///
/// * Result<Option<CollectionOverview>, ArunaError>: Returns an Option<CollectionOverview> (gRPC) or ArunaError
///
fn map_to_collection_overview(
    coll_infos: Option<(
        Collection,
        Option<Vec<CollectionKeyValue>>,
        Option<Vec<RequiredLabel>>,
        Option<CollectionStat>,
        Option<CollectionVersion>,
    )>,
) -> Result<Option<CollectionOverview>, ArunaError> {
    if let Some(ret_coll) = coll_infos {
        let (labels, hooks) = if let Some(ret_kv) = ret_coll.1 {
            from_collection_key_values(ret_kv)
        } else {
            (Vec::new(), Vec::new())
        };

        let label_ont = if let Some(req_labels) = ret_coll.2 {
            Some(LabelOntology {
                required_label_keys: req_labels
                    .iter()
                    .map(|val| val.label_key.to_string())
                    .collect::<Vec<String>>(),
            })
        } else {
            None
        };

        let tstmpt = naivedatetime_to_prost_time(ret_coll.0.created_at)?;

        let stats: Result<Option<CollectionStats>, ArunaError> = if let Some(sts) = ret_coll.3 {
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

        let is_public = match ret_coll.0.dataclass {
            Some(dclass) => match dclass {
                models::enums::Dataclass::PUBLIC => true,
                _ => false,
            },
            None => false,
        };

        let mapped_version = match ret_coll.4 {
            Some(vers) => Some(collection_overview::Version::SemanticVersion(Version {
                major: vers.major as i32,
                minor: vers.minor as i32,
                patch: vers.patch as i32,
            })),
            None => Some(collection_overview::Version::Latest(true)),
        };

        Ok(Some(CollectionOverview {
            id: ret_coll.0.id.to_string(),
            name: ret_coll.0.name,
            description: ret_coll.0.description,
            labels: labels,
            hooks: hooks,
            label_ontology: label_ont,
            created: Some(tstmpt),
            stats: stats?,
            is_public: is_public,
            version: mapped_version,
        }))
    } else {
        Ok(None)
    }
}
