use std::collections::HashMap;

use chrono::Utc;
use diesel::{ delete, insert_into, prelude::*, r2d2::ConnectionManager, result::Error };
use r2d2::PooledConnection;
use crate::{
    database::schema::{
        collection_object_groups::dsl::*,
        object_groups::dsl::*,
        object_group_objects::dsl::*,
        object_group_key_value::dsl::*,
        object_group_stats::dsl::*,
    },
    api::aruna::api::storage::models::v1::{ ObjectGroupStats, Stats },
};
use itertools::Itertools;
use crate::{
    database::{
        connection::Database,
        models::{
            collection::CollectionObjectGroup,
            object_group::{ ObjectGroup, ObjectGroupKeyValue, ObjectGroupObject },
            views::ObjectGroupStat,
        },
    },
    api::aruna::api::storage::services::v1::CreateObjectGroupRequest,
    api::aruna::api::storage::{
        services::v1::CreateObjectGroupResponse,
        models::v1::{ ObjectGroupOverview, KeyValue },
    },
    error::ArunaError,
};

use super::{
    utils::{ to_key_values, from_key_values, naivedatetime_to_prost_time },
    object::check_if_obj_in_coll,
};

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
        creator: &uuid::Uuid
    ) -> Result<CreateObjectGroupResponse, ArunaError> {
        let parsed_col_id = uuid::Uuid::parse_str(&request.collection_id)?;

        let new_obj_grp_uuid = uuid::Uuid::new_v4();

        let database_obj_group = ObjectGroup {
            id: new_obj_grp_uuid,
            shared_revision_id: uuid::Uuid::new_v4(),
            revision_number: 0,
            name: Some(request.name.to_string()),
            description: Some(request.description.to_string()),
            created_at: Utc::now().naive_utc(),
            created_by: *creator,
        };

        let collection_object_group = CollectionObjectGroup {
            id: uuid::Uuid::new_v4(),
            collection_id: parsed_col_id,
            object_group_id: new_obj_grp_uuid,
            writeable: true,
        };

        let key_values = to_key_values::<ObjectGroupKeyValue>(
            request.labels.clone(),
            request.hooks.clone(),
            new_obj_grp_uuid
        );

        let objgrp_obs = request.object_ids
            .iter()
            .map(|id_str| {
                let obj_id = uuid::Uuid::parse_str(id_str)?;
                Ok(ObjectGroupObject {
                    id: uuid::Uuid::new_v4(),
                    object_group_id: new_obj_grp_uuid,
                    object_id: obj_id,
                    is_meta: false,
                })
            })
            .chain(
                request.meta_object_ids.iter().map(|id_str| {
                    let obj_id = uuid::Uuid::parse_str(id_str)?;
                    Ok(ObjectGroupObject {
                        id: uuid::Uuid::new_v4(),
                        object_group_id: new_obj_grp_uuid,
                        object_id: obj_id,
                        is_meta: true,
                    })
                })
            )
            .collect::<Result<Vec<ObjectGroupObject>, ArunaError>>()?;

        let obj_uuids = objgrp_obs
            .iter()
            .map(|e| e.object_id)
            .unique()
            .collect::<Vec<uuid::Uuid>>();

        //Insert all defined object_groups into the database
        let overview = self.pg_connection.get()?.transaction::<ObjectGroupDb, Error, _>(|conn| {
            diesel::insert_into(object_groups).values(&database_obj_group).execute(conn)?;
            diesel::insert_into(object_group_key_value).values(&key_values).execute(conn)?;
            diesel
                ::insert_into(collection_object_groups)
                .values(&collection_object_group)
                .execute(conn)?;
            if !check_if_obj_in_coll(&obj_uuids, &parsed_col_id, conn) {
                return Err(diesel::result::Error::NotFound);
            }

            diesel::insert_into(object_group_objects).values(&objgrp_obs).execute(conn)?;

            let grp = query_object_group(new_obj_grp_uuid, conn)?;

            grp.ok_or(diesel::NotFound)
        })?;

        // Return already complete gRPC response
        Ok(CreateObjectGroupResponse {
            object_group: Some(overview.into()),
        })
    }
}

/* ----------------- Section for object specific helper functions ------------------- */

pub fn query_object_group(
    ogroup_id: uuid::Uuid,
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>
) -> Result<Option<ObjectGroupDb>, diesel::result::Error> {
    let object_group = object_groups
        .filter(crate::database::schema::object_groups::id.eq(&ogroup_id))
        .first::<ObjectGroup>(conn)
        .optional()?;

    if let Some(ogroup) = object_group {
        let object_key_values = ObjectGroupKeyValue::belonging_to(
            &ogroup
        ).load::<ObjectGroupKeyValue>(conn)?;
        let (labels, hooks) = from_key_values(object_key_values);

        let stats = object_group_stats
            .filter(crate::database::schema::object_group_stats::id.eq(ogroup.id))
            .first::<ObjectGroupStat>(conn)
            .optional()?;

        Ok(Some(ObjectGroupDb { object_group: ogroup, labels, hooks, stats }))
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
/// * `creator_id`: `&uuid::Uuid` - UUID of the user that initialized the change.
/// * `objectgroups` `&Vec<uuid::Uuid>` - UUIDs of all object_groups that should be bumped to a new revision
///
/// ## Resturns:
///
/// `Result<Vec<ObjectGroup>, diesel::result::Error>` - List with new updated object_groups
///
pub fn bump_revisisions(
    objectgroups: &Vec<uuid::Uuid>,
    creator_id: &uuid::Uuid,
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>
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
            let new_uuid = uuid::Uuid::new_v4();
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
                id: uuid::Uuid::new_v4(),
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
                id: uuid::Uuid::new_v4(),
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
                id: uuid::Uuid::new_v4(),
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
    insert_into(objgrpkv::object_group_key_value).values(&new_object_group_kv).execute(conn)?;
    // New object_group objects
    insert_into(objgrpobjs::object_group_objects).values(&new_object_group_obj).execute(conn)?;
    // delete old collection object_groups
    delete(
        collobjgrps::collection_object_groups.filter(
            collobjgrps::id.eq_any(&old_coll_objectgroup_ids)
        )
    ).execute(conn)?;
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
/// * `ref_object_group_id`: `uuid::Uuid` - The Uuid for which the latest object_group revision should be found
///
/// ## Resturns:
///
/// `Result<use crate::api::aruna::api::storage::models::ObjectGroup, ArunaError>` -
/// The latest database object_group or error if the request failed.
///
pub fn get_latest_objgrp(
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
    ref_object_group_id: uuid::Uuid
) -> Result<ObjectGroup, diesel::result::Error> {
    use crate::database::schema::object_groups::dsl as objgrps;
    let shared_id = objgrps::object_groups
        .filter(objgrps::id.eq(ref_object_group_id))
        .select(objgrps::shared_revision_id)
        .first::<uuid::Uuid>(conn)?;

    let latest_object_grp = objgrps::object_groups
        .filter(objgrps::shared_revision_id.eq(shared_id))
        .order_by(objgrps::revision_number.desc())
        .first::<ObjectGroup>(conn)?;

    Ok(latest_object_grp)
}

impl From<ObjectGroupDb> for ObjectGroupOverview {
    fn from(ogroup_db: ObjectGroupDb) -> Self {
        let stats = ogroup_db.stats.map(|ogstats| ObjectGroupStats {
            object_stats: Some(Stats { count: ogstats.object_count, acc_size: ogstats.size }),
            last_updated: Some(
                naivedatetime_to_prost_time(ogstats.last_updated).unwrap_or_default()
            ),
        });

        ObjectGroupOverview {
            id: ogroup_db.object_group.id.to_string(),
            name: ogroup_db.object_group.name.unwrap_or_else(|| "".to_string()),
            description: ogroup_db.object_group.description.unwrap_or_else(|| "".to_string()),
            labels: ogroup_db.labels,
            hooks: ogroup_db.hooks,
            stats,
            rev_number: ogroup_db.object_group.revision_number,
        }
    }
}