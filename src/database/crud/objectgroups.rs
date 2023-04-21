use std::collections::HashMap;
use std::str::FromStr;

use crate::database;
use crate::database::connection::Database;
use crate::database::crud::utils::{check_all_for_db_kv, ParsedQuery};
use crate::database::models::collection::CollectionObject;
use crate::database::models::enums::ReferenceStatus;
use crate::database::models::{
    collection::CollectionObjectGroup,
    object_group::{ObjectGroup, ObjectGroupKeyValue, ObjectGroupObject},
    views::ObjectGroupStat,
};
use crate::database::schema::collection_objects::dsl::collection_objects;
use crate::error::ArunaError;
use aruna_rust_api::api::storage::services::v1::{
    AddLabelsToObjectGroupRequest, AddLabelsToObjectGroupResponse,
};
use aruna_rust_api::api::storage::{
    models::v1::{
        KeyValue, Object as ProtoObject, ObjectGroupOverview, ObjectGroupOverviews,
        ObjectGroupStats, Stats,
    },
    services::v1::{
        CreateObjectGroupRequest, CreateObjectGroupResponse, DeleteObjectGroupRequest,
        DeleteObjectGroupResponse, GetObjectGroupByIdRequest, GetObjectGroupByIdResponse,
        GetObjectGroupHistoryRequest, GetObjectGroupHistoryResponse, GetObjectGroupObjectsRequest,
        GetObjectGroupObjectsResponse, GetObjectGroupsFromObjectRequest,
        GetObjectGroupsFromObjectResponse, GetObjectGroupsRequest, GetObjectGroupsResponse,
        ObjectGroupObject as ProtoObjectGroupObject, UpdateObjectGroupRequest,
        UpdateObjectGroupResponse,
    },
};
use bigdecimal::ToPrimitive;
use chrono::Utc;
use diesel::{delete, insert_into, prelude::*, r2d2::ConnectionManager, result::Error, update};
use itertools::Itertools;
use r2d2::PooledConnection;

use super::{
    object::{check_if_obj_in_coll, get_object, ObjectDto},
    utils::{
        from_key_values, naivedatetime_to_prost_time, parse_page_request, parse_query,
        to_key_values,
    },
};

#[derive(Clone, Debug)]
pub struct ObjectGroupDb {
    pub object_group: ObjectGroup,
    pub labels: Vec<KeyValue>,
    pub hooks: Vec<KeyValue>,
    pub stats: Option<ObjectGroupStat>,
}

/// Implementing CRUD+ database operations for ObjectGroups
impl Database {
    pub fn create_object_group(
        &self,
        request: &CreateObjectGroupRequest,
        creator: &diesel_ulid::DieselUlid,
    ) -> Result<CreateObjectGroupResponse, ArunaError> {
        use crate::database::schema::collection_object_groups::dsl::*;
        use crate::database::schema::object_group_key_value::dsl::*;
        use crate::database::schema::object_group_objects::dsl::*;
        use crate::database::schema::object_groups::dsl::*;

        let parsed_col_id = diesel_ulid::DieselUlid::from_str(&request.collection_id)?;
        let new_obj_grp_uuid = diesel_ulid::DieselUlid::generate();

        let database_obj_group = ObjectGroup {
            id: new_obj_grp_uuid,
            shared_revision_id: diesel_ulid::DieselUlid::generate(),
            revision_number: 0,
            name: Some(request.name.to_string()),
            description: Some(request.description.to_string()),
            created_at: Utc::now().naive_utc(),
            created_by: *creator,
        };

        let collection_object_group = CollectionObjectGroup {
            id: diesel_ulid::DieselUlid::generate(),
            collection_id: parsed_col_id,
            object_group_id: new_obj_grp_uuid,
            writeable: true,
        };

        let key_values = to_key_values::<ObjectGroupKeyValue>(
            request.labels.clone(),
            request.hooks.clone(),
            new_obj_grp_uuid,
        );

        let objgrp_obs = request
            .object_ids
            .iter()
            .map(|id_str| {
                let obj_id = diesel_ulid::DieselUlid::from_str(id_str)?;
                Ok(ObjectGroupObject {
                    id: diesel_ulid::DieselUlid::generate(),
                    object_group_id: new_obj_grp_uuid,
                    object_id: obj_id,
                    is_meta: false,
                })
            })
            .chain(request.meta_object_ids.iter().map(|id_str| {
                let obj_id = diesel_ulid::DieselUlid::from_str(id_str)?;
                Ok(ObjectGroupObject {
                    id: diesel_ulid::DieselUlid::generate(),
                    object_group_id: new_obj_grp_uuid,
                    object_id: obj_id,
                    is_meta: true,
                })
            }))
            .collect::<Result<Vec<ObjectGroupObject>, ArunaError>>()?;

        let obj_uuids = objgrp_obs
            .iter()
            .map(|e| e.object_id)
            .unique()
            .collect::<Vec<diesel_ulid::DieselUlid>>();

        // Insert all defined object_groups into the database
        let overview = self
            .pg_connection
            .get()?
            .transaction::<ObjectGroupDb, ArunaError, _>(|conn| {
                // Validate that request does not contain staging objects
                if !collection_objects
                    .filter(database::schema::collection_objects::object_id.eq_any(&obj_uuids))
                    .filter(
                        database::schema::collection_objects::reference_status
                            .eq(&ReferenceStatus::STAGING),
                    )
                    .load::<CollectionObject>(conn)
                    .optional()?
                    .unwrap_or_default()
                    .is_empty()
                {
                    return Err(ArunaError::InvalidRequest(
                        "Cannot create object group with staging objects.".to_string(),
                    ));
                }

                insert_into(object_groups)
                    .values(&database_obj_group)
                    .execute(conn)?;
                insert_into(object_group_key_value)
                    .values(&key_values)
                    .execute(conn)?;
                insert_into(collection_object_groups)
                    .values(&collection_object_group)
                    .execute(conn)?;

                if !check_if_obj_in_coll(&obj_uuids, &parsed_col_id, conn) {
                    return Err(ArunaError::DieselError(Error::NotFound));
                }

                diesel::insert_into(object_group_objects)
                    .values(&objgrp_obs)
                    .execute(conn)?;

                let grp = query_object_group(new_obj_grp_uuid, conn)?;

                grp.ok_or(ArunaError::DieselError(Error::NotFound))
            })?;

        // Return already complete gRPC response
        Ok(CreateObjectGroupResponse {
            object_group: Some(overview.into()),
        })
    }

    pub fn update_object_group(
        &self,
        request: &UpdateObjectGroupRequest,
        creator: &diesel_ulid::DieselUlid,
    ) -> Result<UpdateObjectGroupResponse, ArunaError> {
        use crate::database::schema::collection_object_groups::dsl::*;
        use crate::database::schema::object_group_key_value::dsl::*;
        use crate::database::schema::object_group_objects::dsl::*;
        use crate::database::schema::object_groups::dsl::*;

        let parsed_col_id = diesel_ulid::DieselUlid::from_str(&request.collection_id)?;
        let parsed_old_id = diesel_ulid::DieselUlid::from_str(&request.group_id)?;
        let new_obj_grp_uuid = diesel_ulid::DieselUlid::generate();

        let mut database_obj_group = ObjectGroup {
            id: new_obj_grp_uuid,
            shared_revision_id: diesel_ulid::DieselUlid::default(),
            revision_number: -10,
            name: Some(request.name.to_string()),
            description: Some(request.description.to_string()),
            created_at: Utc::now().naive_utc(),
            created_by: *creator,
        };

        let collection_object_group = CollectionObjectGroup {
            id: diesel_ulid::DieselUlid::generate(),
            collection_id: parsed_col_id,
            object_group_id: new_obj_grp_uuid,
            writeable: true,
        };

        let key_values = to_key_values::<ObjectGroupKeyValue>(
            request.labels.clone(),
            request.hooks.clone(),
            new_obj_grp_uuid,
        );

        let objgrp_obs = request
            .object_ids
            .iter()
            .map(|id_str| {
                let obj_id = diesel_ulid::DieselUlid::from_str(id_str)?;
                Ok(ObjectGroupObject {
                    id: diesel_ulid::DieselUlid::generate(),
                    object_group_id: new_obj_grp_uuid,
                    object_id: obj_id,
                    is_meta: false,
                })
            })
            .chain(request.meta_object_ids.iter().map(|id_str| {
                let obj_id = diesel_ulid::DieselUlid::from_str(id_str)?;
                Ok(ObjectGroupObject {
                    id: diesel_ulid::DieselUlid::generate(),
                    object_group_id: new_obj_grp_uuid,
                    object_id: obj_id,
                    is_meta: true,
                })
            }))
            .collect::<Result<Vec<ObjectGroupObject>, ArunaError>>()?;

        let obj_uuids = objgrp_obs
            .iter()
            .map(|e| e.object_id)
            .unique()
            .collect::<Vec<diesel_ulid::DieselUlid>>();

        //Insert all defined object_groups into the database
        let overview = self
            .pg_connection
            .get()?
            .transaction::<ObjectGroupDb, ArunaError, _>(|conn| {
                // Validate that request does not contain staging objects
                if !collection_objects
                    .filter(database::schema::collection_objects::object_id.eq_any(&obj_uuids))
                    .filter(
                        database::schema::collection_objects::reference_status
                            .eq(&ReferenceStatus::STAGING),
                    )
                    .load::<CollectionObject>(conn)
                    .optional()?
                    .unwrap_or_default()
                    .is_empty()
                {
                    return Err(ArunaError::InvalidRequest(
                        "Cannot create object group with staging objects.".to_string(),
                    ));
                }

                let old_grp = object_groups
                    .filter(crate::database::schema::object_groups::id.eq(parsed_old_id))
                    .first::<ObjectGroup>(conn)?;

                database_obj_group.shared_revision_id = old_grp.shared_revision_id;
                database_obj_group.revision_number = old_grp.revision_number + 1;

                diesel::insert_into(object_groups)
                    .values(&database_obj_group)
                    .execute(conn)?;
                diesel::insert_into(object_group_key_value)
                    .values(&key_values)
                    .execute(conn)?;
                diesel::insert_into(collection_object_groups)
                    .values(&collection_object_group)
                    .execute(conn)?;
                if !check_if_obj_in_coll(&obj_uuids, &parsed_col_id, conn) {
                    return Err(ArunaError::DieselError(Error::NotFound));
                }

                diesel::insert_into(object_group_objects)
                    .values(&objgrp_obs)
                    .execute(conn)?;

                let grp = query_object_group(new_obj_grp_uuid, conn)?;

                grp.ok_or(ArunaError::DieselError(diesel::NotFound))
            })?;

        // Return already complete gRPC response
        Ok(UpdateObjectGroupResponse {
            object_group: Some(overview.into()),
        })
    }

    pub fn get_object_group_by_id(
        &self,
        request: &GetObjectGroupByIdRequest,
    ) -> Result<GetObjectGroupByIdResponse, ArunaError> {
        let parsed_obj_id = diesel_ulid::DieselUlid::from_str(&request.group_id)?;

        //Insert all defined object_groups into the database
        let overview = self
            .pg_connection
            .get()?
            .transaction::<ObjectGroupDb, Error, _>(|conn| {
                let grp = query_object_group(parsed_obj_id, conn)?;
                grp.ok_or(diesel::NotFound)
            })?;

        // Return already complete gRPC response
        Ok(GetObjectGroupByIdResponse {
            object_group: Some(overview.into()),
        })
    }

    pub fn get_object_groups_from_object(
        &self,
        request: &GetObjectGroupsFromObjectRequest,
    ) -> Result<GetObjectGroupsFromObjectResponse, ArunaError> {
        use crate::database::schema::object_group_objects::dsl::*;

        let obj_id = diesel_ulid::DieselUlid::from_str(&request.object_id)?;

        //Insert all defined object_groups into the database
        let overviews = self
            .pg_connection
            .get()?
            .transaction::<Vec<ObjectGroupDb>, Error, _>(|conn| {
                let object_grp_ids = object_group_objects
                    .filter(crate::database::schema::object_group_objects::object_id.eq(obj_id))
                    .select(crate::database::schema::object_group_objects::object_group_id)
                    .load::<diesel_ulid::DieselUlid>(conn)?;

                object_grp_ids
                    .iter()
                    .map(|obj_grp_id| {
                        query_object_group(*obj_grp_id, conn)?.ok_or(diesel::NotFound)
                    })
                    .collect::<Result<Vec<ObjectGroupDb>, _>>()
            })?;

        let ogoverview = ObjectGroupOverviews {
            object_group_overviews: overviews
                .iter()
                .map(|ov| ObjectGroupOverview::from(ov.clone()))
                .collect::<Vec<ObjectGroupOverview>>(),
        };

        // Return already complete gRPC response
        Ok(GetObjectGroupsFromObjectResponse {
            object_groups: Some(ogoverview),
        })
    }

    pub fn get_object_groups(
        &self,
        request: GetObjectGroupsRequest,
    ) -> Result<GetObjectGroupsResponse, ArunaError> {
        // Parse the page_request and get pagesize / lastuuid
        let (pagesize, last_uuid) = parse_page_request(request.page_request, 20)?;

        // Parse the query to a `ParsedQuery`
        let parsed_query = parse_query(request.label_id_filter)?;

        // Parse the collection-id of the request
        let collection_uuid = diesel_ulid::DieselUlid::from_str(&request.collection_id)?;

        // ObjectGroup context
        use crate::database::schema::collection_object_groups::dsl as cogrps;
        use crate::database::schema::object_group_key_value::dsl as ogkv;
        use crate::database::schema::object_groups::dsl as ogrps;
        use diesel::prelude::*;

        //Get object_groups of collection with filters
        let overviews = self
            .pg_connection
            .get()?
            .transaction::<Option<Vec<ObjectGroupDb>>, Error, _>(|conn| {
                // Get all collection object group ids of collection object group references for filter
                let ids: Option<Vec<diesel_ulid::DieselUlid>> = cogrps::collection_object_groups
                    .select(cogrps::object_group_id)
                    .filter(cogrps::collection_id.eq(&collection_uuid))
                    .load::<diesel_ulid::DieselUlid>(conn)
                    .optional()?;

                // First build a "boxed" base request to which additional parameters can be added later
                let mut base_request = ogrps::object_groups
                    .filter(ogrps::name.ne("DELETED")) // Exclude ObjectGroups with name == "DELETED"
                    .into_boxed();

                // Filter for valid ids in collection
                if let Some(valid_ids) = ids {
                    base_request = base_request.filter(ogrps::id.eq_any(valid_ids));
                } else {
                    base_request = base_request
                        .filter(ogrps::id.eq_any(Vec::<diesel_ulid::DieselUlid>::new()));
                }

                // Create returnvector of CollectionOverviewsDb
                let mut return_vec: Vec<ObjectGroupDb> = Vec::new();

                // If pagesize is not unlimited set it to pagesize or default = 20
                if let Some(pg_size) = pagesize {
                    base_request = base_request.limit(pg_size);
                }

                // Add "last_uuid" filter if it is specified
                if let Some(l_uid) = last_uuid {
                    base_request = base_request.filter(ogrps::id.gt(l_uid));
                }

                // Add query if it exists
                if let Some(p_query) = parsed_query {
                    // Check if query exists
                    match p_query {
                        // This is a label query request
                        ParsedQuery::LabelQuery(l_query) => {
                            // Create key value boxed request
                            let mut ckv_query = ogkv::object_group_key_value.into_boxed();
                            // Create vector with "matching" collections
                            let found_objs: Option<Vec<diesel_ulid::DieselUlid>>;
                            // Is "and"
                            if l_query.1 {
                                // Add each key / value to label query
                                for (obj_key, obj_value) in l_query.0.clone() {
                                    // Will be Some if keys only == false
                                    if let Some(val) = obj_value {
                                        ckv_query = ckv_query.or_filter(
                                            ogkv::key.eq(obj_key).and(ogkv::value.eq(val)),
                                        );
                                    } else {
                                        ckv_query = ckv_query.or_filter(ogkv::key.eq(obj_key));
                                    }
                                }
                                // Execute request and get a list with all found key values
                                let found_obj_kv: Option<Vec<ObjectGroupKeyValue>> =
                                    ckv_query.load::<ObjectGroupKeyValue>(conn).optional()?;
                                // Parse the returned key_values for the "all" constraint
                                // and only return matching collection ids
                                found_objs = check_all_for_db_kv(found_obj_kv, l_query.0);
                                // If the query is "or"
                            } else {
                                // Query all key / values
                                for (obj_key, obj_value) in l_query.0 {
                                    ckv_query = ckv_query.or_filter(ogkv::key.eq(obj_key));
                                    // Only Some() if key_only is false
                                    if let Some(val) = obj_value {
                                        ckv_query = ckv_query.filter(ogkv::value.eq(val));
                                    }
                                }
                                // Can query the matches collections directly
                                found_objs = ckv_query
                                    .select(ogkv::object_group_id)
                                    .distinct()
                                    .load::<diesel_ulid::DieselUlid>(conn)
                                    .optional()?;
                            }
                            // Add to query if something was found otherwise return Only
                            if let Some(fobjs) = found_objs {
                                base_request = base_request.filter(ogrps::id.eq_any(fobjs));
                            } else {
                                return Ok(None);
                            }
                        }
                        // If the request was an ID request, just filter for all ids
                        // And for uuids makes no sense
                        ParsedQuery::IdsQuery(ids) => {
                            base_request = base_request.filter(ogrps::id.eq_any(ids));
                        }
                    }
                }

                // Execute the preconfigured query
                let query_object_groups: Option<Vec<ObjectGroup>> =
                    base_request.load::<ObjectGroup>(conn).optional()?;

                // Query overviews for each object group
                // TODO: This might be inefficient and can be optimized later
                if let Some(q_objs_grp) = query_object_groups {
                    for s_obj_grp in q_objs_grp {
                        if let Some(ogdb) = query_object_group(s_obj_grp.id, conn)? {
                            return_vec.push(ogdb);
                        }
                    }
                    Ok(Some(return_vec))
                } else {
                    Ok(None)
                }
            })?;

        let ogoverview = overviews
            .map(|elem| {
                elem.iter()
                    .map(|ov| ObjectGroupOverview::from(ov.clone()))
                    .collect::<Vec<ObjectGroupOverview>>()
            })
            .map(|some_ogoverview| ObjectGroupOverviews {
                object_group_overviews: some_ogoverview,
            });

        // Return already complete gRPC response
        Ok(GetObjectGroupsResponse {
            object_groups: ogoverview,
        })
    }

    pub fn get_object_group_history(
        &self,
        request: GetObjectGroupHistoryRequest,
    ) -> Result<GetObjectGroupHistoryResponse, ArunaError> {
        use crate::database::schema::object_groups::dsl::*;

        let grp_id = diesel_ulid::DieselUlid::from_str(&request.group_id)?;
        let (pagesize, last_uuid) = parse_page_request(request.page_request, 20)?;
        //Insert all defined object_groups into the database
        let overviews = self
            .pg_connection
            .get()?
            .transaction::<Vec<ObjectGroupDb>, Error, _>(|conn| {
                let base_group = object_groups
                    .filter(crate::database::schema::object_groups::id.eq(grp_id))
                    .first::<ObjectGroup>(conn)?;

                // First build a "boxed" base request to which additional parameters can be added later
                let mut base_request = object_groups
                    .filter(
                        crate::database::schema::object_groups::shared_revision_id
                            .eq(base_group.shared_revision_id),
                    )
                    .into_boxed();
                // If pagesize is not unlimited set it to pagesize or default = 20
                if let Some(pg_size) = pagesize {
                    base_request = base_request.limit(pg_size);
                }
                // Add "last_uuid" filter if it is specified
                if let Some(l_uid) = last_uuid {
                    base_request =
                        base_request.filter(crate::database::schema::object_groups::id.ge(l_uid));
                }

                let all: Vec<diesel_ulid::DieselUlid> = base_request
                    .select(crate::database::schema::object_groups::id)
                    .load::<diesel_ulid::DieselUlid>(conn)?;
                Ok(all
                    .iter()
                    .filter_map(|s_obj_grp| query_object_group(*s_obj_grp, conn).ok())
                    .flatten()
                    .collect::<Vec<ObjectGroupDb>>())
            })?;

        let ogoverview = ObjectGroupOverviews {
            object_group_overviews: overviews
                .iter()
                .map(|ov| ObjectGroupOverview::from(ov.clone()))
                .collect::<Vec<ObjectGroupOverview>>(),
        };

        // Return already complete gRPC response
        Ok(GetObjectGroupHistoryResponse {
            object_groups: Some(ogoverview),
        })
    }

    pub fn get_object_group_objects(
        &self,
        request: GetObjectGroupObjectsRequest,
    ) -> Result<GetObjectGroupObjectsResponse, ArunaError> {
        use crate::database::schema::object_group_objects::dsl::*;

        let grp_id = diesel_ulid::DieselUlid::from_str(&request.group_id)?;
        let col_id = diesel_ulid::DieselUlid::from_str(&request.collection_id)?;
        let (pagesize, last_uuid) = parse_page_request(request.page_request, 20)?;
        //Insert all defined object_groups into the database
        let overviews = self
            .pg_connection
            .get()?
            .transaction::<Vec<(ObjectDto, bool)>, Error, _>(|conn| {
                // First build a "boxed" base request to which additional parameters can be added later
                let mut base_request = object_group_objects
                    .filter(
                        crate::database::schema::object_group_objects::object_group_id.eq(grp_id),
                    )
                    .into_boxed();
                // If meta_only filter for is_meta == true
                if request.meta_only {
                    base_request = base_request
                        .filter(crate::database::schema::object_group_objects::is_meta.eq(true));
                }
                // If pagesize is not unlimited set it to pagesize or default = 20
                if let Some(pg_size) = pagesize {
                    base_request = base_request.limit(pg_size);
                }
                // Add "last_uuid" filter if it is specified
                if let Some(l_uid) = last_uuid {
                    base_request = base_request
                        .filter(crate::database::schema::object_group_objects::object_id.ge(l_uid));
                }

                let all: Vec<ObjectGroupObject> = base_request.load::<ObjectGroupObject>(conn)?;

                all.iter()
                    .filter_map(|obj_grp_obj| {
                        match get_object(&obj_grp_obj.object_id, &col_id, true, conn) {
                            Ok(opt) => opt.map(|s| Ok((s, obj_grp_obj.is_meta))),
                            Err(e) => Some(Err(e)),
                        }
                    })
                    .collect::<Result<Vec<(_, _)>, _>>()
            })?;

        let ogobjects = overviews
            .iter()
            .map(|(ov, is_meta_obj)| {
                Ok(ProtoObjectGroupObject {
                    object: Some(ProtoObject::try_from(ov.clone())?),
                    is_metadata: *is_meta_obj,
                })
            })
            .collect::<Result<Vec<ProtoObjectGroupObject>, ArunaError>>()?;

        // Return already complete gRPC response
        Ok(GetObjectGroupObjectsResponse {
            object_group_objects: ogobjects,
        })
    }

    pub fn delete_object_group(
        &self,
        request: DeleteObjectGroupRequest,
    ) -> Result<DeleteObjectGroupResponse, ArunaError> {
        use crate::database::schema::collection_object_groups::dsl::*;
        use crate::database::schema::object_group_key_value::dsl::*;
        use crate::database::schema::object_group_objects::dsl::*;
        use crate::database::schema::object_groups::dsl::*;

        // Parse id of object group to be deleted
        let object_group_uuid = diesel_ulid::DieselUlid::from_str(&request.group_id)?;

        // Deletion transaction
        self.pg_connection.get()?.transaction::<_, Error, _>(|conn| {
            // Query the relevant object groups from the database
            let queried_groups = if request.with_revisions {
                // If with_revisions == true:
                //  - Query all revisions of provided object group and delete them descending
                let og_shared_revision_id = object_groups
                    .filter(crate::database::schema::object_groups::id.eq(object_group_uuid))
                    .select(crate::database::schema::object_groups::shared_revision_id)
                    .first::<diesel_ulid::DieselUlid>(conn)?;

                object_groups
                    .filter(crate::database::schema::object_groups::shared_revision_id.eq(&og_shared_revision_id))
                    .order_by(crate::database::schema::object_groups::revision_number.desc())
                    .load::<ObjectGroup>(conn)?
            } else {
                // If with_revisions == false:
                //  - Create vector only with provided object group
                vec![object_groups
                    .filter(crate::database::schema::object_groups::id.eq(object_group_uuid))
                    .first::<ObjectGroup>(conn)?]
            };

            // Loop over queried object groups and delete them individually
            for queried_group in queried_groups {
                if let Some(object_group_name) = queried_group.name {
                    // Check if specified ObjectGroup revision is already deleted
                    match object_group_name.as_str() {
                        "DELETED" => {
                            // Do nothing. Revision is already deleted.
                        }
                        _ => {
                            // Delete key values of object group
                            delete(object_group_key_value)
                                .filter(
                                    crate::database::schema::object_group_key_value::object_group_id.eq(
                                        &queried_group.id
                                    )
                                )
                                .execute(conn)?;

                            // Delete collection_object_groups
                            delete(collection_object_groups)
                                .filter(
                                    crate::database::schema::collection_object_groups::object_group_id.eq(
                                        &queried_group.id
                                    )
                                )
                                .execute(conn)?;

                            // Delete object_group_objects
                            delete(object_group_objects)
                                .filter(
                                    crate::database::schema::object_group_objects::object_group_id.eq(
                                        &queried_group.id
                                    )
                                )
                                .execute(conn)?;

                            // Rename ObjectGroup revision name and description to "DELETED"
                            update(object_groups)
                                .filter(
                                    crate::database::schema::object_groups::id.eq(&queried_group.id)
                                )
                                .set((
                                    crate::database::schema::object_groups::name.eq(
                                        "DELETED".to_string()
                                    ),
                                    crate::database::schema::object_groups::description.eq(
                                        "DELETED".to_string()
                                    ),
                                ))
                                .execute(conn)?;

                            // Count undeleted ObjectGroup revisions
                            let undeleted = object_groups
                                .select(
                                    diesel::dsl::count(crate::database::schema::object_groups::name)
                                )
                                .filter(
                                    crate::database::schema::object_groups::shared_revision_id.eq(
                                        queried_group.shared_revision_id
                                    )
                                )
                                .filter(
                                    crate::database::schema::object_groups::name.ne(
                                        "DELETED".to_string()
                                    )
                                )
                                .filter(
                                    crate::database::schema::object_groups::description.ne(
                                        "DELETED".to_string()
                                    )
                                )
                                .first::<i64>(conn)?;

                            if undeleted == 0 {
                                delete(object_groups)
                                    .filter(
                                        crate::database::schema::object_groups::shared_revision_id.eq(
                                            queried_group.shared_revision_id
                                        )
                                    )
                                    .execute(conn)?;
                            }
                        }
                    }
                }
            }

            Ok(())
        })?;

        // Return already complete gRPC response
        Ok(DeleteObjectGroupResponse {})
    }

    pub fn add_labels_to_object_group(
        &self,
        request: AddLabelsToObjectGroupRequest,
    ) -> Result<AddLabelsToObjectGroupResponse, ArunaError> {
        use crate::database::schema::object_group_key_value::dsl::*;

        // Parse id of object group to be deleted
        let object_group_uuid = diesel_ulid::DieselUlid::from_str(&request.group_id)?;

        // Deletion transaction
        let updated_ogroup = self
            .pg_connection
            .get()?
            .transaction::<Option<ObjectGroupDb>, Error, _>(|conn| {
                let db_key_values = to_key_values::<ObjectGroupKeyValue>(
                    request.labels_to_add,
                    Vec::new(),
                    object_group_uuid,
                );

                insert_into(object_group_key_value)
                    .values(&db_key_values)
                    .execute(conn)?;

                query_object_group(object_group_uuid, conn)
            })?;

        // Return already complete gRPC response
        Ok(AddLabelsToObjectGroupResponse {
            object_group: updated_ogroup.map(|ogrp| ogrp.into()),
        })
    }
}

/* ----------------- Section for object specific helper functions ------------------- */

pub fn query_object_group(
    ogroup_id: diesel_ulid::DieselUlid,
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
) -> Result<Option<ObjectGroupDb>, diesel::result::Error> {
    use crate::database::schema::object_group_stats::dsl::*;
    use crate::database::schema::object_groups::dsl::*;

    let object_group = object_groups
        .filter(crate::database::schema::object_groups::id.eq(&ogroup_id))
        .first::<ObjectGroup>(conn)
        .optional()?;

    if let Some(ogroup) = object_group {
        let object_key_values =
            ObjectGroupKeyValue::belonging_to(&ogroup).load::<ObjectGroupKeyValue>(conn)?;
        let (labels, hooks) = from_key_values(object_key_values);

        let stats = object_group_stats
            .filter(crate::database::schema::object_group_stats::id.eq(ogroup.id))
            .first::<ObjectGroupStat>(conn)
            .optional()?;

        Ok(Some(ObjectGroupDb {
            object_group: ogroup,
            labels,
            hooks,
            stats,
        }))
    } else {
        Ok(None)
    }
}

/// Helper function that bumps all specified object_groups to a "new" revision. This will create
/// a copy for each Objectgroup with same objects / keyvalues.
/// Will also work when the objectgroup that is updated is not already or still the latest revision.
/// ATTENTION: This expects that each objectgroup is unique per collection. If borrowing for objectgroups
/// is reintroduced this function must be expanded / changed.
///
/// ## Arguments:
///
/// * `conn: &mut PooledConnection<ConnectionManager<PgConnection>>` - Database connection
/// * `creator_id`: `&diesel_ulid::DieselUlid` - UUID of the user that initialized the change.
/// * `objectgroups` `&Vec<diesel_ulid::DieselUlid>` - UUIDs of all object_groups that should be bumped to a new revision
///
/// ## Resturns:
///
/// `Result<Vec<ObjectGroup>, diesel::result::Error>` - List with new updated object_groups
///
pub fn bump_revisisions(
    objectgroups: &Vec<diesel_ulid::DieselUlid>,
    creator_id: &diesel_ulid::DieselUlid,
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
) -> Result<Vec<ObjectGroup>, diesel::result::Error> {
    use crate::database::schema::collection_object_groups::dsl as collobjgrps;
    use crate::database::schema::object_group_key_value::dsl as objgrpkv;
    use crate::database::schema::object_group_objects::dsl as objgrpobjs;
    use crate::database::schema::object_groups::dsl as objgrps;

    // First get all old objectgroups
    // This is ok as long as object_groups can NOT be borrowed.
    let groups = objgrps::object_groups
        .filter(objgrps::id.eq_any(objectgroups))
        .load::<ObjectGroup>(conn)?;

    let mut mappings = HashMap::new();
    // Load all collection references
    let collection_obj_groups = collobjgrps::collection_object_groups
        .filter(collobjgrps::object_group_id.eq_any(objectgroups))
        .load::<CollectionObjectGroup>(conn)?;

    let new_groups = groups
        .iter()
        .map(|old| {
            // Create a new uuid
            let new_uuid = diesel_ulid::DieselUlid::generate();
            // Insert the mapping old vs. new uuid in the mappings hashmap
            mappings.insert(old.id, new_uuid);
            // Query the "latest" version first
            // Note: This could be skipped if updates are only allowed for the currently "latest" revision
            let latest = get_latest_objgrp(conn, old.id)?;
            // Create a new object_group entry
            // with increased revision number
            Ok(ObjectGroup {
                id: new_uuid,
                shared_revision_id: old.shared_revision_id,
                revision_number: latest.revision_number + 1,
                name: old.name.clone(),
                description: old.description.clone(),
                created_at: Utc::now().naive_utc(),
                created_by: *creator_id,
            })
        })
        .collect::<Result<Vec<_>, diesel::result::Error>>()?;

    let old_object_group_obj = objgrpobjs::object_group_objects
        .filter(objgrpobjs::object_group_id.eq_any(objectgroups))
        .load::<ObjectGroupObject>(conn)?;

    let old_object_group_kv = objgrpkv::object_group_key_value
        .filter(objgrpkv::object_group_id.eq_any(objectgroups))
        .load::<ObjectGroupKeyValue>(conn)?;

    let new_object_group_kv = old_object_group_kv
        .iter()
        .map(|old| {
            Ok(ObjectGroupKeyValue {
                id: diesel_ulid::DieselUlid::generate(),
                object_group_id: *mappings
                    .get(&old.object_group_id)
                    .ok_or(diesel::result::Error::NotFound)?,
                key: old.key.to_string(),
                value: old.value.to_string(),
                key_value_type: old.key_value_type,
            })
        })
        .collect::<Result<Vec<_>, diesel::result::Error>>()?;

    let old_coll_objectgroup_ids = collection_obj_groups
        .iter()
        .map(|old| old.id)
        .collect::<Vec<_>>();

    let new_object_group_obj = old_object_group_obj
        .iter()
        .map(|old| {
            Ok(ObjectGroupObject {
                id: diesel_ulid::DieselUlid::generate(),
                object_group_id: *mappings
                    .get(&old.object_group_id)
                    .ok_or(diesel::result::Error::NotFound)?,
                object_id: old.object_id,
                is_meta: old.is_meta,
            })
        })
        .collect::<Result<Vec<_>, diesel::result::Error>>()?;

    let new_collection_object_groups = collection_obj_groups
        .iter()
        .map(|old| {
            Ok(CollectionObjectGroup {
                id: diesel_ulid::DieselUlid::generate(),
                collection_id: old.collection_id,
                object_group_id: *mappings
                    .get(&old.object_group_id)
                    .ok_or(diesel::result::Error::NotFound)?,
                writeable: old.writeable,
            })
        })
        .collect::<Result<Vec<_>, diesel::result::Error>>()?;

    // Insert new object_groups
    let new_inserted_grps = insert_into(objgrps::object_groups)
        .values(&new_groups)
        .get_results::<ObjectGroup>(conn)?;
    // Insert key_values
    insert_into(objgrpkv::object_group_key_value)
        .values(&new_object_group_kv)
        .execute(conn)?;
    // New object_group objects
    insert_into(objgrpobjs::object_group_objects)
        .values(&new_object_group_obj)
        .execute(conn)?;
    // delete old collection object_groups
    delete(
        collobjgrps::collection_object_groups
            .filter(collobjgrps::id.eq_any(&old_coll_objectgroup_ids)),
    )
    .execute(conn)?;
    // Insert new coll obj grp
    insert_into(collobjgrps::collection_object_groups)
        .values(&new_collection_object_groups)
        .execute(conn)?;

    Ok(new_inserted_grps)
}

/// This is a helper method that queries the "latest" object_group based on the current object_group_uuid.
/// If returned object_group.id == ref_object_group_id -> the current object_group is "latest"
///
/// ## Arguments:
///
/// * `conn: &mut PooledConnection<ConnectionManager<PgConnection>>` - Database connection
/// * `ref_object_group_id`: `diesel_ulid::DieselUlid` - The Uuid for which the latest object_group revision should be found
///
/// ## Resturns:
///
/// `Result<use aruna_rust_api::api::storage::models::ObjectGroup, ArunaError>` -
/// The latest database object_group or error if the request failed.
///
pub fn get_latest_objgrp(
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
    ref_object_group_id: diesel_ulid::DieselUlid,
) -> Result<ObjectGroup, diesel::result::Error> {
    use crate::database::schema::object_groups::dsl as objgrps;
    let shared_id = objgrps::object_groups
        .filter(objgrps::id.eq(ref_object_group_id))
        .select(objgrps::shared_revision_id)
        .first::<diesel_ulid::DieselUlid>(conn)?;

    let latest_object_grp = objgrps::object_groups
        .filter(objgrps::shared_revision_id.eq(shared_id))
        .order_by(objgrps::revision_number.desc())
        .first::<ObjectGroup>(conn)?;

    Ok(latest_object_grp)
}

impl From<ObjectGroupDb> for ObjectGroupOverview {
    fn from(ogroup_db: ObjectGroupDb) -> Self {
        let stats = ogroup_db.stats.map(|ogstats| ObjectGroupStats {
            object_stats: Some(Stats {
                count: ogstats.object_count,
                acc_size: ogstats.size.to_i64().unwrap_or_default(),
            }),
            last_updated: Some(
                naivedatetime_to_prost_time(ogstats.last_updated).unwrap_or_default(),
            ),
        });

        ObjectGroupOverview {
            id: ogroup_db.object_group.id.to_string(),
            name: ogroup_db.object_group.name.unwrap_or_default(),
            description: ogroup_db.object_group.description.unwrap_or_default(),
            labels: ogroup_db.labels,
            hooks: ogroup_db.hooks,
            stats,
            rev_number: ogroup_db.object_group.revision_number,
        }
    }
}
