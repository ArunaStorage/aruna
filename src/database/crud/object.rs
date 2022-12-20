use std::collections::HashMap;
use std::convert::TryInto;
use std::hash::Hash;
use std::hash::Hasher;

use chrono::Local;
use diesel::dsl::{count, max};
use diesel::r2d2::ConnectionManager;
use diesel::result::Error;
use diesel::{delete, insert_into, prelude::*, update};
use r2d2::PooledConnection;

use crate::database::models::object_group::ObjectGroupObject;
use crate::error::{ArunaError, GrpcNotFoundError};
use aruna_rust_api::api::internal::v1::{Location as ProtoLocation, LocationType};
use aruna_rust_api::api::storage::services::v1::{
    AddLabelsToObjectRequest, AddLabelsToObjectResponse, DeleteObjectsRequest,
    DeleteObjectsResponse, GetReferencesRequest, GetReferencesResponse, ObjectReference,
    SetHooksOfObjectRequest, SetHooksOfObjectResponse, StageObject,
};
use aruna_rust_api::api::storage::{
    models::v1::{
        Hash as ProtoHash, Hashalgorithm, KeyValue, Object as ProtoObject, Origin as ProtoOrigin,
        Source as ProtoSource,
    },
    services::v1::{
        CloneObjectRequest, CloneObjectResponse, CreateObjectReferenceRequest,
        CreateObjectReferenceResponse, DeleteObjectRequest, DeleteObjectResponse,
        FinishObjectStagingRequest, FinishObjectStagingResponse, GetLatestObjectRevisionRequest,
        GetLatestObjectRevisionResponse, GetObjectByIdRequest, GetObjectRevisionsRequest,
        GetObjectsRequest, InitializeNewObjectRequest, InitializeNewObjectResponse,
        UpdateObjectRequest, UpdateObjectResponse,
    },
};

use crate::database;
use crate::database::connection::Database;
use crate::database::crud::utils::{
    check_all_for_db_kv, db_to_grpc_dataclass, db_to_grpc_object_status, from_key_values,
    naivedatetime_to_prost_time, parse_page_request, parse_query, to_key_values,
};
use crate::database::models::collection::CollectionObject;
use crate::database::models::enums::{
    HashType, KeyValueType, ObjectStatus, ReferenceStatus, SourceType,
};
use crate::database::models::object::{
    Endpoint, Hash as ApiHash, Object, ObjectKeyValue, ObjectLocation, Source,
};
use crate::database::schema::{
    collection_object_groups::dsl::*, collection_objects::dsl::*, endpoints::dsl::*,
    hashes::dsl::*, object_group_objects::dsl::*, object_key_value::dsl::*,
    object_locations::dsl::*, objects::dsl::*, sources::dsl::*,
};

use super::objectgroups::bump_revisisions;
use super::utils::{grpc_to_db_dataclass, ParsedQuery};

// Struct to hold the database objects
#[derive(Debug, Clone)]
pub struct ObjectDto {
    pub object: Object,
    pub labels: Vec<KeyValue>,
    pub hooks: Vec<KeyValue>,
    pub hash: ApiHash,
    pub source: Option<Source>,
    pub latest: bool,
    pub update: bool,
}

impl PartialEq for ObjectDto {
    fn eq(&self, other: &Self) -> bool {
        self.object == other.object
            && self.labels == other.labels
            && self.hooks == other.hooks
            && self.hash == other.hash
            && self.source == other.source
            && self.latest == other.latest
            && self.update == other.update
    }
}

impl Eq for ObjectDto {}

impl Hash for ObjectDto {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.object.hash(state);
    }
}

/// Implementing CRUD+ database operations for Objects
impl Database {
    /// Creates the following records in the database to initialize a new object:
    /// * Source
    /// * Object incl. join table entry
    /// * ObjectLocation
    /// * Hash (empty)
    /// * ObjectKeyValue(s)
    ///
    /// ## Arguments
    ///
    /// * `request` - A gRPC request containing the needed information to create a new object
    ///
    /// ## Returns
    ///
    /// * `Result<(InitializeNewObjectResponse, ArunaError>`
    ///
    /// The InitializeNewObjectResponse contains:
    ///   * The object id - Immutable und unique id the object can be identified with
    ///   * The upload id - Can be used to upload data associated to the object
    ///   * The collection id the object belongs to
    ///
    pub fn create_object(
        &self,
        request: &InitializeNewObjectRequest,
        creator: &uuid::Uuid,
        location: &ProtoLocation,
        upload_id: String,
        default_endpoint: uuid::Uuid,
        object_uuid: uuid::Uuid,
    ) -> Result<InitializeNewObjectResponse, ArunaError> {
        // Check if StageObject is available
        let staging_object = request.object.clone().ok_or(GrpcNotFoundError::STAGEOBJ)?;

        //Define source object from updated request; None if empty
        let source: Option<Source> = match &staging_object.source {
            Some(source) => Some(Source {
                id: uuid::Uuid::new_v4(),
                link: source.identifier.clone(),
                source_type: SourceType::from_i32(source.source_type)?,
            }),
            _ => None,
        };

        // Check if preferred endpoint is specified
        let endpoint_uuid = match uuid::Uuid::parse_str(&request.preferred_endpoint_id) {
            Ok(ep_id) => ep_id,
            Err(_) => default_endpoint,
        };

        // Define object in database representation
        let object = Object {
            id: object_uuid,
            shared_revision_id: uuid::Uuid::new_v4(),
            revision_number: 0,
            filename: staging_object.filename.clone(),
            created_at: Local::now().naive_local(),
            created_by: *creator,
            content_len: staging_object.content_len,
            object_status: ObjectStatus::INITIALIZING,
            dataclass: grpc_to_db_dataclass(&staging_object.dataclass),
            source_id: source.as_ref().map(|src| src.id),
            origin_id: Some(object_uuid),
        };

        // Define the join table entry collection <--> object
        let collection_object = CollectionObject {
            id: uuid::Uuid::new_v4(),
            collection_id: uuid::Uuid::parse_str(&request.collection_id)?,
            is_latest: false, // Will be checked on finish
            reference_status: ReferenceStatus::STAGING,
            object_id: object.id,
            auto_update: false, //Note: Finally set with FinishObjectStagingRequest
            is_specification: request.is_specification,
            writeable: true, //Note: Original object is initially always writeable
        };

        // Define the initial object location
        let object_location = ObjectLocation {
            id: uuid::Uuid::new_v4(),
            bucket: location.bucket.clone(),
            path: location.path.clone(),
            endpoint_id: endpoint_uuid,
            object_id: object.id,
            is_primary: true,
        };

        // Define the hash placeholder for the object
        let empty_hash = ApiHash {
            id: uuid::Uuid::new_v4(),
            hash: "".to_string(), //Note: Empty hash will be updated later
            object_id: object.id,
            hash_type: HashType::MD5, //Note: Default. Will be updated later
        };

        // Convert the object's labels and hooks to their database representation
        let key_value_pairs = to_key_values::<ObjectKeyValue>(
            staging_object.labels,
            staging_object.hooks,
            object_uuid,
        );

        // Insert all defined objects into the database
        self.pg_connection
            .get()?
            .transaction::<_, Error, _>(|conn| {
                if let Some(sour) = source {
                    diesel::insert_into(sources).values(&sour).execute(conn)?;
                }
                diesel::insert_into(objects).values(&object).execute(conn)?;
                diesel::insert_into(object_locations)
                    .values(&object_location)
                    .execute(conn)?;
                diesel::insert_into(hashes)
                    .values(&empty_hash)
                    .execute(conn)?;
                diesel::insert_into(object_key_value)
                    .values(&key_value_pairs)
                    .execute(conn)?;
                diesel::insert_into(collection_objects)
                    .values(&collection_object)
                    .execute(conn)?;

                Ok(())
            })?;

        // Return already complete gRPC response
        Ok(InitializeNewObjectResponse {
            object_id: object.id.to_string(),
            upload_id,
            collection_id: request.collection_id.clone(),
        })
    }

    /// ToDo: Rust Doc
    pub fn finish_object_staging(
        &self,
        request: &FinishObjectStagingRequest,
        user_id: &uuid::Uuid,
    ) -> Result<FinishObjectStagingResponse, ArunaError> {
        let req_object_uuid = uuid::Uuid::parse_str(&request.object_id)?;
        let req_coll_uuid = uuid::Uuid::parse_str(&request.collection_id)?;

        // Insert all defined objects into the database
        let object_dto = self.pg_connection
            .get()?
            .transaction::<Option<ObjectDto>, ArunaError, _>(|conn| {
                let latest = get_latest_obj(conn, req_object_uuid)?;
                let is_still_latest = latest.id == req_object_uuid;

                // Update the object itself to be available
                let returned_obj = diesel
                    ::update(objects.filter(database::schema::objects::id.eq(req_object_uuid)))
                    .set(database::schema::objects::object_status.eq(ObjectStatus::AVAILABLE))
                    .get_result::<Object>(conn)?;

                // Update hash if (re-)upload and request contains hash
                if !request.no_upload && request.hash.is_some() {
                    (match &request.hash {
                        None => {
                            return Err(
                                ArunaError::InvalidRequest(
                                    "Missing hash after re-upload.".to_string()
                                )
                            );
                        }
                        Some(req_hash) =>
                            diesel
                                ::update(ApiHash::belonging_to(&returned_obj))
                                .set((
                                    database::schema::hashes::hash.eq(&req_hash.hash),
                                    database::schema::hashes::hash_type.eq(
                                        HashType::from_grpc(req_hash.alg)
                                    ),
                                ))
                                .execute(conn),
                    })?;
                }

                // Check if the origin id is different from uuid
                // This indicates an "updated" object and not a new one
                // Finishing updates need extra steps to update all references
                // In other collections / objectgroups
                if let Some(orig_id) = returned_obj.origin_id {
                    // origin_id != object_id => Update
                    if orig_id != returned_obj.id {
                        // Get all revisions of the object it could be that an older version still has "auto_update" set
                        let all_revisions = get_all_revisions(conn, req_object_uuid)?;
                        // Filter out the UUIDs
                        let all_rev_ids = all_revisions
                            .iter()
                            .map(|full| full.id)
                            .collect::<Vec<_>>();

                        // Get all CollectionObjects that contain any of the all_rev_ids and are auto_update == true
                        // Set all auto_updates and is_latest to be false
                        let auto_updating_coll_obj: Vec<CollectionObject> = collection_objects
                            .filter(
                                database::schema::collection_objects::object_id.eq_any(&all_rev_ids)
                            )
                            .filter(database::schema::collection_objects::auto_update.eq(true))
                            .load::<CollectionObject>(conn)
                            .optional()?
                            .unwrap_or_default();

                        let auto_update_collection_references = auto_updating_coll_obj
                            .iter()
                            .filter(|elem| elem.collection_id == req_coll_uuid)
                            .collect::<Vec<_>>();

                        let (
                            auto_update_collection_reference,
                            auto_update_collection_reference_id,
                        ): (Option<CollectionObject>, uuid::Uuid) = match
                            auto_update_collection_references.len()
                        {
                            0 => (None, uuid::Uuid::default()),
                            1 =>
                                (
                                    Some(auto_update_collection_references[0].clone()),
                                    auto_update_collection_references[0].clone().id,
                                ),
                            _ => {
                                return Err(
                                    ArunaError::InvalidRequest(
                                        "More than one revision with auto_update == true".to_string()
                                    )
                                );
                            }
                        };

                        // object ids inside references
                        let auto_updating_obj_id = auto_updating_coll_obj
                            .iter()
                            .filter(|elem| elem.id != auto_update_collection_reference_id)
                            .map(|elem| elem.object_id)
                            .collect::<Vec<_>>();

                        // collection_object ids
                        let auto_updating_coll_obj_id = auto_updating_coll_obj
                            .iter()
                            .filter(|elem| elem.id != auto_update_collection_reference_id)
                            .map(|elem| elem.id)
                            .collect::<Vec<_>>();

                        // Only proceed if the list () is not empty, if it is empty no updates need to be performed
                        // Update ObjectGroups and Objects reference in other collections
                        if !auto_updating_coll_obj_id.is_empty() {
                            // Query the affected object_groups
                            let affected_object_groups: Option<Vec<uuid::Uuid>> =
                                object_group_objects
                                    .filter(
                                        database::schema::object_group_objects::object_id.eq_any(
                                            &auto_updating_obj_id
                                        )
                                    )
                                    .select(database::schema::object_group_objects::object_group_id)
                                    .load::<uuid::Uuid>(conn)
                                    .optional()?;

                            match affected_object_groups {
                                None => {}
                                Some(obj_grp_ids) => {
                                    // Bump all revisions for object_groups
                                    let new_ogroups = bump_revisisions(
                                        &obj_grp_ids,
                                        user_id,
                                        conn
                                    )?;
                                    let new_group_ids = new_ogroups
                                        .iter()
                                        .map(|group| group.id)
                                        .collect::<Vec<_>>();

                                    // Update object_group references
                                    update(object_group_objects)
                                        .filter(
                                            database::schema::object_group_objects::object_group_id.eq_any(
                                                &new_group_ids
                                            )
                                        )
                                        .filter(
                                            database::schema::object_group_objects::object_id.eq(
                                                orig_id
                                            )
                                        )
                                        .set(
                                            database::schema::object_group_objects::object_id.eq(
                                                req_object_uuid
                                            )
                                        )
                                        .execute(conn)?;
                                }
                            }

                            // Update Collection_Objects to use the new object_id
                            update(
                                collection_objects.filter(
                                    database::schema::collection_objects::id.eq_any(
                                        &auto_updating_coll_obj_id
                                    )
                                )
                            )
                                .set((
                                    database::schema::collection_objects::object_id.eq(
                                        req_object_uuid
                                    ),
                                ))
                                .execute(conn)?;
                        }

                        // Query the affected object_groups
                        let affected_object_groups: Option<Vec<uuid::Uuid>> = object_group_objects
                            .filter(
                                database::schema::object_group_objects::object_id.eq(
                                    &returned_obj.origin_id.unwrap_or_default()
                                )
                            )
                            .select(database::schema::object_group_objects::object_group_id)
                            .load::<uuid::Uuid>(conn)
                            .optional()?;

                        match affected_object_groups {
                            None => {}
                            Some(obj_grp_ids) => {
                                // Bump all revisions for object_groups
                                let new_ogroups = bump_revisisions(&obj_grp_ids, user_id, conn)?;
                                let new_group_ids = new_ogroups
                                    .iter()
                                    .map(|group| group.id)
                                    .collect::<Vec<_>>();

                                // Update object_group references
                                update(object_group_objects)
                                    .filter(
                                        database::schema::object_group_objects::object_group_id.eq_any(
                                            &new_group_ids
                                        )
                                    )
                                    .filter(
                                        database::schema::object_group_objects::object_id.eq(
                                            orig_id
                                        )
                                    )
                                    .set(
                                        database::schema::object_group_objects::object_id.eq(
                                            req_object_uuid
                                        )
                                    )
                                    .execute(conn)?;
                            }
                        }

                        // Update inside collection of update object
                        match auto_update_collection_reference {
                            None => {
                                // Update latest reference
                                update(collection_objects)
                                    .filter(
                                        database::schema::collection_objects::object_id.eq(
                                            &req_object_uuid
                                        )
                                    )
                                    .filter(
                                        database::schema::collection_objects::collection_id.eq(
                                            &req_coll_uuid
                                        )
                                    )
                                    .filter(
                                        database::schema::collection_objects::reference_status.eq(
                                            &ReferenceStatus::STAGING
                                        )
                                    )
                                    .set((
                                        database::schema::collection_objects::object_id.eq(
                                            req_object_uuid
                                        ),
                                        database::schema::collection_objects::is_latest.eq(
                                            is_still_latest
                                        ),
                                        database::schema::collection_objects::reference_status.eq(
                                            ReferenceStatus::OK
                                        ),
                                        database::schema::collection_objects::auto_update.eq(
                                            request.auto_update
                                        ),
                                    ))
                                    .execute(conn)?;
                            }
                            Some(reference) => {
                                // Delete staging reference
                                delete(collection_objects)
                                    .filter(
                                        database::schema::collection_objects::object_id.eq(
                                            &req_object_uuid
                                        )
                                    )
                                    .filter(
                                        database::schema::collection_objects::collection_id.eq(
                                            &req_coll_uuid
                                        )
                                    )
                                    .filter(
                                        database::schema::collection_objects::reference_status.eq(
                                            &ReferenceStatus::STAGING
                                        )
                                    )
                                    .execute(conn)?;

                                // Update latest reference
                                update(collection_objects)
                                    .filter(
                                        database::schema::collection_objects::id.eq(&reference.id)
                                    )
                                    .filter(
                                        database::schema::collection_objects::collection_id.eq(
                                            &req_coll_uuid
                                        )
                                    )
                                    .set((
                                        database::schema::collection_objects::object_id.eq(
                                            req_object_uuid
                                        ),
                                        database::schema::collection_objects::is_latest.eq(
                                            is_still_latest
                                        ),
                                        database::schema::collection_objects::reference_status.eq(
                                            ReferenceStatus::OK
                                        ),
                                        database::schema::collection_objects::auto_update.eq(
                                            request.auto_update && is_still_latest
                                        ),
                                    ))
                                    .execute(conn)?;
                            }
                        }
                        // origin_id == object_id => Initialize
                    } else {
                        // Update the collection objects
                        // - Status
                        // - is_latest
                        // - auto_update
                        diesel
                            ::update(
                                collection_objects.filter(
                                    database::schema::collection_objects::object_id.eq(
                                        req_object_uuid
                                    )
                                )
                            )
                            .set((
                                database::schema::collection_objects::is_latest.eq(is_still_latest),
                                database::schema::collection_objects::reference_status.eq(
                                    ReferenceStatus::OK
                                ),
                                database::schema::collection_objects::auto_update.eq(
                                    request.auto_update
                                ),
                            ))
                            .execute(conn)?;
                    }
                }

                Ok(get_object(&req_object_uuid, &req_coll_uuid, true, conn)?)
            })?;

        let mapped = object_dto
            .map(|e| e.try_into())
            .map_or(Ok(None), |r| r.map(Some))?;
        Ok(FinishObjectStagingResponse { object: mapped })
    }

    ///ToDo: Rust Doc
    pub fn update_object(
        &self,
        request: &UpdateObjectRequest,
        location: &Option<ProtoLocation>,
        creator_uuid: &uuid::Uuid,
        default_endpoint: uuid::Uuid,
        new_obj_id: uuid::Uuid,
    ) -> Result<UpdateObjectResponse, ArunaError> {
        if let Some(sobj) = &request.object {
            let parsed_old_id = uuid::Uuid::parse_str(&request.object_id)?;
            let parsed_col_id = uuid::Uuid::parse_str(&request.collection_id)?;

            let updated_object = self.pg_connection
                .get()?
                .transaction::<Object, ArunaError, _>(|conn| {
                    // Get latest revision of the Object to be updated
                    let latest = get_latest_obj(conn, parsed_old_id)?;

                    if latest.id != parsed_old_id && !request.force {
                        return Err(ArunaError::InvalidRequest(
                            "Concurrency error: Updates without force are only allowed for the latest object"
                                .to_string(),
                        ));
                    }

                    //Define source object from updated request; None if empty
                    let source: Option<Source> = match &sobj.source {
                        Some(source) => Some(Source {
                            id: uuid::Uuid::new_v4(),
                            link: source.identifier.clone(),
                            source_type: SourceType::from_i32(source.source_type)?,
                        }),
                        _ => None,
                    };

                    // Check if object is already staging
                    let reference_opt: Option<CollectionObject> = collection_objects
                        .filter(database::schema::collection_objects::object_id.eq(parsed_old_id))
                        .first::<CollectionObject>(conn)
                        .optional()?;

                    if let Some(reference) = reference_opt {
                        if reference.reference_status == ReferenceStatus::STAGING {
                            return Ok(update_object_in_place(conn, &parsed_old_id, creator_uuid, sobj)?);
                        }
                    }

                    // Define new Object with updated values
                    let new_object = Object {
                        id: new_obj_id,
                        shared_revision_id: latest.shared_revision_id,
                        revision_number: latest.revision_number + 1,
                        filename: sobj.filename.to_string(),
                        created_at: chrono::Utc::now().naive_utc(),
                        created_by: *creator_uuid,
                        content_len: sobj.content_len,
                        object_status: ObjectStatus::UNAVAILABLE, // Is a staging object
                        dataclass: grpc_to_db_dataclass(&sobj.dataclass),
                        source_id: source.as_ref().map(|source| source.id),
                        origin_id: Some(parsed_old_id),
                    };

                    // Define temporary STAGING join table entry collection <-->  staging object
                    let collection_object = CollectionObject {
                        id: uuid::Uuid::new_v4(),
                        collection_id: parsed_col_id,
                        is_latest: false, // Will be checked on finish
                        reference_status: ReferenceStatus::STAGING,
                        object_id: new_obj_id,
                        auto_update: false, //Note: Finally set with FinishObjectStagingRequest
                        is_specification: request.is_specification,
                        writeable: true,
                    };

                    // Convert the object's labels and hooks to their database representation
                    // Clone could be removed if the to_object_key_values method takes borrowed vec instead of moved / owned reference
                    let key_value_pairs = to_key_values::<ObjectKeyValue>(
                        sobj.labels.clone(),
                        sobj.hooks.clone(),
                        new_obj_id,
                    );

                    // Insert entities which are always created on update
                    diesel::insert_into(objects)
                        .values(&new_object)
                        .execute(conn)?;
                    diesel::insert_into(object_key_value)
                        .values(&key_value_pairs)
                        .execute(conn)?;
                    diesel::insert_into(collection_objects)
                        .values(&collection_object)
                        .execute(conn)?;

                    // Insert Source only if it exists in request
                    if source.is_some() {
                        diesel::insert_into(sources).values(&source).execute(conn)?;
                    }

                    // Insert updated object location and hash if data re-upload
                    if request.reupload {
                        if let Some(loc) = location {
                            // Check if preferred endpoint is specified
                            let endpoint_uuid =
                                match uuid::Uuid::parse_str(&request.preferred_endpoint_id) {
                                    Ok(ep_id) => ep_id,
                                    Err(_) => default_endpoint,
                                };
                            let object_location = ObjectLocation {
                                id: uuid::Uuid::new_v4(),
                                bucket: loc.bucket.clone(),
                                path: loc.path.clone(),
                                endpoint_id: endpoint_uuid,
                                object_id: new_obj_id,
                                is_primary: true,
                            };

                            // Define the hash placeholder for the object
                            let empty_hash = ApiHash {
                                id: uuid::Uuid::new_v4(),
                                hash: "".to_string(), //Note: Empty hash will be updated later
                                object_id: new_obj_id,
                                hash_type: HashType::MD5, //Note: Default. Will be updated later
                            };
                            diesel::insert_into(object_locations)
                                .values(&object_location)
                                .execute(conn)?;
                            diesel::insert_into(hashes)
                                .values(&empty_hash)
                                .execute(conn)?;
                        }
                    } else {
                        // Clone old location for new Object
                        let old_object = objects
                            .filter(database::schema::objects::id.eq(&parsed_old_id))
                            .first::<Object>(conn)?;
                        let old_location: ObjectLocation =
                            ObjectLocation::belonging_to(&old_object)
                                .first::<ObjectLocation>(conn)?;

                        let new_location = ObjectLocation {
                            id: uuid::Uuid::new_v4(),
                            bucket: old_location.bucket,
                            path: old_location.path,
                            endpoint_id: old_location.endpoint_id,
                            object_id: new_obj_id,
                            is_primary: old_location.is_primary,
                        };

                        diesel::insert_into(object_locations)
                            .values(&new_location)
                            .execute(conn)?;
                    }

                    Ok(new_object)
                })?;

            Ok(UpdateObjectResponse {
                object_id: updated_object.id.to_string(),
                staging_id: updated_object.id.to_string(),
                collection_id: parsed_col_id.to_string(),
            })
        } else {
            Err(ArunaError::InvalidRequest(
                "Staging object must be provided".to_string(),
            ))
        }

        /*ToDo:
         *  - Check permissions if update on collection is ok
         *  - Create staging object and update differing metadata
         *      - Set object status UNAVAILABLE
         *      - Set revision e.g. None
         *  - Copy key-value pairs and sync them with the provided (Add, Update, Delete)
         *  - Create collection_objects reference with status STAGING
         *  - If reupload == true
         *      - Set object status INITIALIZING
         *      - Create new location
         *      - Create new empty hash
         *      - Generate upload id with data proxy request
         */
    }

    /// Returns the object from the database in its gRPC format.
    ///
    /// ## Arguments
    ///
    /// * `request` - A gRPC request containing the needed information to get the specific object
    ///
    /// ## Returns
    ///
    /// * `Result<aruna_server::api::aruna::api::storage::models::v1::Object, ArunaError>` -
    /// All of the Objects fields are filled as long as the database contains the corresponding values.
    ///
    pub fn get_object(
        &self,
        request: &GetObjectByIdRequest,
    ) -> Result<Option<ProtoObject>, ArunaError> {
        // Check if id in request has valid format
        let object_uuid = uuid::Uuid::parse_str(&request.object_id)?;
        let collection_uuid = uuid::Uuid::parse_str(&request.collection_id)?;

        // Read object from database
        let object_dto = self
            .pg_connection
            .get()?
            .transaction::<Option<ObjectDto>, Error, _>(|conn| {
                // Use the helper function to execute the request
                get_object(&object_uuid, &collection_uuid, true, conn)
            })?;
        object_dto
            .map(|e| e.try_into())
            .map_or(Ok(None), |r| r.map(Some))
    }

    ///ToDo: Rust Doc
    pub fn get_object_by_id(&self, object_uuid: &uuid::Uuid) -> Result<Object, ArunaError> {
        // Read object from database
        let db_object = self
            .pg_connection
            .get()?
            .transaction::<Object, Error, _>(|conn| {
                let object = objects
                    .filter(database::schema::objects::id.eq(object_uuid))
                    .first::<Object>(conn)?;

                Ok(object)
            })?;

        Ok(db_object)
    }

    ///ToDo: Rust Doc
    pub fn get_primary_object_location(
        &self,
        object_uuid: &uuid::Uuid,
    ) -> Result<ProtoLocation, ArunaError> {
        let location = self
            .pg_connection
            .get()?
            .transaction::<ProtoLocation, Error, _>(|conn| {
                let location: ObjectLocation = object_locations
                    .filter(database::schema::object_locations::object_id.eq(&object_uuid))
                    .filter(database::schema::object_locations::is_primary.eq(true))
                    .first::<ObjectLocation>(conn)?;

                Ok(ProtoLocation {
                    r#type: LocationType::S3 as i32, //ToDo: How to get LocationType?
                    bucket: location.bucket,
                    path: location.path,
                })
            })?;

        Ok(location)
    }

    ///ToDo: Rust Doc
    pub fn get_primary_object_location_with_endpoint(
        &self,
        object_uuid: &uuid::Uuid,
    ) -> Result<(ObjectLocation, Endpoint), ArunaError> {
        let location = self
            .pg_connection
            .get()?
            .transaction::<(ObjectLocation, Endpoint), Error, _>(|conn| {
                let location: ObjectLocation = object_locations
                    .filter(database::schema::object_locations::object_id.eq(&object_uuid))
                    .filter(database::schema::object_locations::is_primary.eq(true))
                    .first::<ObjectLocation>(conn)?;

                let endpoint: Endpoint = endpoints
                    .filter(database::schema::endpoints::id.eq(&location.endpoint_id))
                    .first::<Endpoint>(conn)?;

                Ok((location, endpoint))
            })?;

        Ok(location)
    }

    /// ToDo: Rust Doc
    pub fn get_object_locations(
        &self,
        object_uuid: &uuid::Uuid,
    ) -> Result<Vec<ObjectLocation>, ArunaError> {
        let locations = self
            .pg_connection
            .get()?
            .transaction::<Vec<ObjectLocation>, Error, _>(|conn| {
                let locations: Vec<ObjectLocation> = object_locations
                    .filter(database::schema::object_locations::object_id.eq(&object_uuid))
                    .filter(database::schema::object_locations::is_primary.eq(true))
                    .load::<ObjectLocation>(conn)?;

                Ok(locations)
            })?;

        Ok(locations)
    }

    ///ToDo: Rust Doc
    pub fn get_latest_object_revision(
        &self,
        request: GetLatestObjectRevisionRequest,
    ) -> Result<GetLatestObjectRevisionResponse, ArunaError> {
        let parsed_object_id = uuid::Uuid::parse_str(&request.object_id)?;
        let parsed_collection_id = uuid::Uuid::parse_str(&request.collection_id)?;

        let latest_rev = self
            .pg_connection
            .get()?
            .transaction::<Option<ObjectDto>, Error, _>(|conn| {
                let lat_obj = get_latest_obj(conn, parsed_object_id)?;
                get_object(&lat_obj.id, &parsed_collection_id, false, conn)
            })?;

        let mapped = latest_rev
            .map(|e| e.try_into())
            .map_or(Ok(None), |r| r.map(Some))?;

        Ok(GetLatestObjectRevisionResponse { object: mapped })
    }

    ///ToDo: Rust Doc
    ///
    pub fn get_object_revisions(
        &self,
        request: GetObjectRevisionsRequest,
    ) -> Result<Vec<ObjectDto>, ArunaError> {
        let parsed_object_id = uuid::Uuid::parse_str(&request.object_id)?;
        let parsed_collection_id = uuid::Uuid::parse_str(&request.collection_id)?;

        let all_revs = self
            .pg_connection
            .get()?
            .transaction::<Vec<ObjectDto>, Error, _>(|conn| {
                // This is a safety measure to make sure on revision is referenced in the current collection
                // Otherwise get_object_ignore_coll could be used to break safety measures / permission boundaries
                let all = get_all_revisions(conn, parsed_object_id)?;
                let all_ids = all.iter().map(|e| e.id).collect::<Vec<_>>();

                let issomewherereferenced = collection_objects
                    .filter(database::schema::collection_objects::object_id.eq_any(&all_ids))
                    .filter(
                        database::schema::collection_objects::collection_id
                            .eq(parsed_collection_id),
                    )
                    .first::<CollectionObject>(conn)
                    .optional()?;

                // Query and return all revisions
                Ok(if issomewherereferenced.is_some() {
                    all.iter()
                        .filter_map(|obj| match get_object_ignore_coll(&obj.id, conn) {
                            Ok(opt) => opt.map(Ok),
                            Err(e) => Some(Err(e)),
                        })
                        .collect::<Result<Vec<ObjectDto>, _>>()?
                } else {
                    Vec::new()
                })
            })?;

        Ok(all_revs)
    }

    /// ToDo: Rust Docs
    pub fn get_objects(
        &self,
        request: GetObjectsRequest,
    ) -> Result<Option<Vec<ObjectDto>>, ArunaError> {
        // Parse the page_request and get pagesize / lastuuid
        let (pagesize, last_uuid) = parse_page_request(request.page_request, 20)?;
        // Parse the query to a `ParsedQuery`
        let parsed_query = parse_query(request.label_id_filter)?;
        // Collection context

        let query_collection_id = uuid::Uuid::parse_str(&request.collection_id)?;

        // Execute request
        use crate::database::schema::collection_objects::dsl as colobj;
        use crate::database::schema::object_key_value::dsl as okv;
        use diesel::prelude::*;
        let ret_objects = self
            .pg_connection
            .get()?
            .transaction::<Option<Vec<ObjectDto>>, Error, _>(|conn| {
                // First build a "boxed" base request to which additional parameters can be added later
                let mut base_request = colobj::collection_objects.into_boxed();
                // Filter collection_id
                base_request = base_request.filter(colobj::collection_id.eq(&query_collection_id));

                // Create returnvector of CollectionOverviewsDb
                let mut return_vec: Vec<ObjectDto> = Vec::new();
                // If pagesize is not unlimited set it to pagesize or default = 20
                if let Some(pg_size) = pagesize {
                    base_request = base_request.limit(pg_size);
                }
                // Add "last_uuid" filter if it is specified
                if let Some(l_uid) = last_uuid {
                    base_request = base_request.filter(colobj::object_id.gt(l_uid));
                }
                // Add query if it exists
                if let Some(p_query) = parsed_query {
                    // Check if query exists
                    match p_query {
                        // This is a label query request
                        ParsedQuery::LabelQuery(l_query) => {
                            // Create key value boxed request
                            let mut ckv_query = okv::object_key_value.into_boxed();
                            // Create vector with "matching" collections
                            let found_objs: Option<Vec<uuid::Uuid>>;
                            // Is "and"
                            if l_query.1 {
                                // Add each key / value to label query
                                for (obj_key, obj_value) in l_query.0.clone() {
                                    // Will be Some if keys only == false
                                    if let Some(val) = obj_value {
                                        ckv_query = ckv_query.or_filter(
                                            okv::key.eq(obj_key).and(okv::value.eq(val)),
                                        );
                                    } else {
                                        ckv_query = ckv_query.or_filter(okv::key.eq(obj_key));
                                    }
                                }
                                // Execute request and get a list with all found key values
                                let found_obj_kv: Option<Vec<ObjectKeyValue>> =
                                    ckv_query.load::<ObjectKeyValue>(conn).optional()?;
                                // Parse the returned key_values for the "all" constraint
                                // and only return matching collection ids
                                found_objs = check_all_for_db_kv(found_obj_kv, l_query.0);
                                // If the query is "or"
                            } else {
                                // Query all key / values
                                for (obj_key, obj_value) in l_query.0 {
                                    ckv_query = ckv_query.or_filter(okv::key.eq(obj_key));
                                    // Only Some() if key_only is false
                                    if let Some(val) = obj_value {
                                        ckv_query = ckv_query.filter(okv::value.eq(val));
                                    }
                                }
                                // Can query the matches collections directly
                                found_objs = ckv_query
                                    .select(okv::object_id)
                                    .distinct()
                                    .load::<uuid::Uuid>(conn)
                                    .optional()?;
                            }
                            // Add to query if something was found otherwise return Only
                            if let Some(fobjs) = found_objs {
                                base_request = base_request.filter(colobj::object_id.eq_any(fobjs));
                            } else {
                                return Ok(None);
                            }
                        }
                        // If the request was an ID request, just filter for all ids
                        // And for uuids makes no sense
                        ParsedQuery::IdsQuery(ids) => {
                            base_request = base_request.filter(colobj::object_id.eq_any(ids));
                        }
                    }
                }

                // Execute the preconfigured query
                let query_collections: Option<Vec<CollectionObject>> =
                    base_request.load::<CollectionObject>(conn).optional()?;
                // Query overviews for each collection
                // TODO: This might be inefficient and can be optimized later
                if let Some(q_objs) = query_collections {
                    for s_obj in q_objs {
                        if let Some(obj) =
                            get_object(&s_obj.object_id, &query_collection_id, false, conn)?
                        {
                            return_vec.push(obj);
                        }
                    }
                    Ok(Some(return_vec))
                } else {
                    Ok(None)
                }
            })?;

        Ok(ret_objects)
    }

    /// Creates a reference to the original object in the target collection.
    ///
    /// ## Arguments:
    ///
    /// * `BorrowObjectRequest` - Request which contains the needed information to borrow an object to another collection
    ///
    /// ## Returns:
    ///
    /// * `Result<BorrowObjectResponse, ArunaError>` - Empty BorrowObjectResponse signals success
    ///
    /// ## Behaviour:
    ///
    /// Returns an error if `collection_id == target_collection_id` and/or the object is already borrowed
    /// to the target collection as object duplicates in collections are not allowed.
    ///
    pub fn create_object_reference(
        &self,
        request: CreateObjectReferenceRequest,
    ) -> Result<CreateObjectReferenceResponse, ArunaError> {
        // Extract (and automagically validate) uuids from request
        let object_uuid = uuid::Uuid::parse_str(&request.object_id)?;
        let source_collection_uuid = uuid::Uuid::parse_str(&request.collection_id)?;
        let target_collection_uuid = uuid::Uuid::parse_str(&request.target_collection_id)?;

        // Transaction time
        self.pg_connection
            .get()?
            .transaction::<_, Error, _>(|conn| {
                // Get collection_object association of original object
                let original_reference: CollectionObject = collection_objects
                    .filter(database::schema::collection_objects::object_id.eq(&object_uuid))
                    .filter(
                        database::schema::collection_objects::collection_id
                            .eq(&source_collection_uuid),
                    )
                    .first::<CollectionObject>(conn)?;

                let collection_object = CollectionObject {
                    id: uuid::Uuid::new_v4(),
                    collection_id: target_collection_uuid,
                    object_id: object_uuid,
                    is_latest: original_reference.is_latest,
                    is_specification: original_reference.is_specification,
                    auto_update: request.auto_update,
                    writeable: request.writeable,
                    reference_status: original_reference.reference_status,
                };

                // Insert borrowed object reference
                diesel::insert_into(collection_objects)
                    .values(collection_object)
                    .execute(conn)?;

                Ok(())
            })?;

        // Empty response signals success
        Ok(CreateObjectReferenceResponse {})
    }

    /// Get all references for an object in a specific collection
    ///
    /// ## Arguments:
    ///
    /// * `GetReferencesRequest` - Request that specifies an object
    ///
    /// ## Returns:
    ///
    /// * `Result<GetReferencesResponse, ArunaError>` - List with all current references
    ///
    /// ## Behaviour:
    ///
    /// Returns a list with all current references of the specified object. Optional all references for all revisions
    /// are returned (=> `with_revisions`)
    ///
    pub fn get_references(
        &self,
        request: &GetReferencesRequest,
    ) -> Result<GetReferencesResponse, ArunaError> {
        // Extract (and automagically validate) uuids from request
        let object_uuid = uuid::Uuid::parse_str(&request.object_id)?;

        // Transaction time
        let references = self
            .pg_connection
            .get()?
            .transaction::<Vec<ObjectReference>, Error, _>(|conn| {
                let orig_object = objects
                    .filter(database::schema::objects::id.eq(object_uuid))
                    .first::<Object>(conn)?;

                if request.with_revisions {
                    let all_revisions = objects
                        .filter(
                            database::schema::objects::shared_revision_id
                                .eq(orig_object.shared_revision_id),
                        )
                        .load::<Object>(conn)?;
                    let mapped = all_revisions
                        .iter()
                        .map(|elem| (elem.id, elem.revision_number))
                        .collect::<HashMap<uuid::Uuid, i64>>();

                    let reved_references: Vec<CollectionObject> =
                        CollectionObject::belonging_to(&all_revisions)
                            .filter(
                                database::schema::collection_objects::reference_status
                                    .eq(ReferenceStatus::OK),
                            )
                            .load::<CollectionObject>(conn)?;

                    Ok(reved_references
                        .iter()
                        .map(|elem| ObjectReference {
                            object_id: elem.object_id.to_string(),
                            collection_id: elem.collection_id.to_string(),
                            revision_number: *mapped.get(&elem.id).unwrap_or(&0),
                            is_writeable: elem.writeable,
                        })
                        .collect::<Vec<_>>())
                } else {
                    let solo_references: Vec<CollectionObject> =
                        CollectionObject::belonging_to(&orig_object)
                            .load::<CollectionObject>(conn)?;

                    Ok(solo_references
                        .iter()
                        .map(|elem| ObjectReference {
                            object_id: elem.object_id.to_string(),
                            collection_id: elem.collection_id.to_string(),
                            revision_number: orig_object.revision_number,
                            is_writeable: elem.writeable,
                        })
                        .collect::<Vec<_>>())
                }
            })?;

        // Empty response signals success
        Ok(GetReferencesResponse { references })
    }

    /// ToDo: Rust Doc
    pub fn get_reference_status(
        &self,
        object_uuid: &uuid::Uuid,
        collection_uuid: &uuid::Uuid,
    ) -> Result<ReferenceStatus, ArunaError> {
        // Read specific object reference for collection from database
        let object_reference = self
            .pg_connection
            .get()?
            .transaction::<CollectionObject, Error, _>(|conn| {
                collection_objects
                    .filter(database::schema::collection_objects::object_id.eq(&object_uuid))
                    .filter(
                        database::schema::collection_objects::collection_id.eq(&collection_uuid),
                    )
                    .first::<CollectionObject>(conn) // Only one reference per collection can exist
            })?;

        Ok(object_reference.reference_status)
    }

    /// This clones a specific revision of an object into another collection.
    /// The cloned object is then treated like any other individual object.
    ///
    /// ## Arguments:
    ///
    /// * `request: CloneObjectRequest` - A gRPC request containing the needed information to clone a specific object
    ///
    /// ## Results:
    ///
    /// * `Result<CloneObjectResponse, ArunaError>` -
    /// The CloneObjectResponse contains the newly created object in its gRPC proto format.
    ///
    pub fn clone_object(
        &self,
        request: &CloneObjectRequest,
    ) -> Result<CloneObjectResponse, ArunaError> {
        // Extract (and automagically validate) uuids from request
        let object_uuid = uuid::Uuid::parse_str(&request.object_id)?;
        let source_collection_uuid = uuid::Uuid::parse_str(&request.collection_id)?;
        let target_collection_uuid = uuid::Uuid::parse_str(&request.target_collection_id)?;

        // Transaction time
        let cloned_object = self
            .pg_connection
            .get()?
            .transaction::<ProtoObject, Error, _>(|conn| {
                let proto_object = clone_object(
                    conn,
                    object_uuid,
                    source_collection_uuid,
                    target_collection_uuid,
                )?;

                Ok(proto_object)
            })?;

        Ok(CloneObjectResponse {
            object: Some(cloned_object),
        })
    }

    /// This performs a hard delete on the object. The object and all its assets will be
    /// removed from the database. Dependeing on the request this also includes all its
    /// revisions and also objects which were derived from the original.
    ///
    /// ## Arguments:
    ///
    /// * `request: DeleteObjectRequest` -
    ///
    /// ## Results:
    ///
    /// * `Result<DeleteObjectResponse, ArunaError>` - An empty DeleteObjectResponse signals success
    ///
    /// ## Behaviour:
    ///
    /// ToDo
    ///
    pub fn delete_object(
        &self,
        request: DeleteObjectRequest,
        creator_id: uuid::Uuid,
    ) -> Result<DeleteObjectResponse, ArunaError> {
        //ToDo: - Set status of all affected objects to UNAVAILABLE
        //ToDo: - What do with borrowed child objects?
        /*ToDo: - Delete only possible on latest revision?
         *      - Delete for each revision:
         *          - Hash
         *          - ObjectLocations --> S3 Objects (Currently no delete function available in data proxy)
         *          - Source (Only with original)
         *          - ObjectKeyValues
         *
         *          - CollectionObject
         *          - ObjectGroupObject
         *          - Object
         */

        //writeable = w+ or w-
        //history   = h+ or h-
        //force     = f+ or f-

        // Permissions needed:
        //   w*,h*,f-: Collection WRITE
        //   w*,h*,f+: Project ADMIN

        /*
         * w-,h-,f* : - Remove collection_object reference for specific collection
         *            - Remove object_group_object reference and update object_group
         * w-,h+,f* : - Remove collection_object reference for all revisions in specific collection
         *            - For all revisions remove object_group_object reference and update object_group
         * w+,h-,f- : - Check if last writeable for all revisions
         *              - False: - Remove collection_object reference for specific collection
         *                       - Remove object_group_object reference and update object_group
         *              - True: Error -> Transfer Ownership or force
         * w+,h+,f- : - Check if last writeable for all revisions
         *              - False: - Remove collection_object reference for all revisions in specific collection
         *                       - For all revisions remove object_group_object reference and update object_group
         *              - True: Error -> Transfer ownership or force
         * w+,h-,f+ : - Check if last writeable for all revisions
         *              - False: - Remove collection_object reference for specific collection
         *                       - Remove object_group_object reference and update object_group
         *              - True: - Remove all references and set object status to TRASH
         *                      - Update all object_groups with references to the specific object/revision
         * w+,h+,f+ : - Remove all references and set object status to TRASH
         *            - Update all object_groups with references to the specific object/revision
         */

        let parsed_collection_id = uuid::Uuid::parse_str(&request.collection_id)?;
        let parsed_object_id = uuid::Uuid::parse_str(&request.object_id)?;

        self.pg_connection
            .get()?
            .transaction::<_, ArunaError, _>(|conn| {
                delete_multiple_objects(
                    vec![parsed_object_id],
                    parsed_collection_id,
                    request.force,
                    request.with_revisions,
                    creator_id,
                    conn,
                )?;
                Ok(())
            })?;

        Ok(DeleteObjectResponse {})
    }

    pub fn delete_objects(
        &self,
        request: DeleteObjectsRequest,
        creator_id: uuid::Uuid,
    ) -> Result<DeleteObjectsResponse, ArunaError> {
        //writeable = w+ or w-
        //history   = h+ or h-
        //force     = f+ or f-

        // Permissions needed:
        //   w*,h*,f-: Collection WRITE
        //   w*,h*,f+: Project ADMIN

        /*
         * w-,h-,f* : - Remove collection_object reference for specific collection
         *            - Remove object_group_object reference and update object_group
         * w-,h+,f* : - Remove collection_object reference for all revisions in specific collection
         *            - For all revisions remove object_group_object reference and update object_group
         * w+,h-,f- : - Check if last writeable for all revisions
         *              - False: - Remove collection_object reference for specific collection
         *                       - Remove object_group_object reference and update object_group
         *              - True: Error -> Transfer Ownership or force
         * w+,h+,f- : - Check if last writeable for all revisions
         *              - False: - Remove collection_object reference for all revisions in specific collection
         *                       - For all revisions remove object_group_object reference and update object_group
         *              - True: Error -> Transfer ownership or force
         * w+,h-,f+ : - Check if last writeable for all revisions
         *              - False: - Remove collection_object reference for specific collection
         *                       - Remove object_group_object reference and update object_group
         *              - True: - Remove all references and set object status to TRASH
         *                      - Update all object_groups with references to the specific object/revision
         * w+,h+,f+ : - Remove all references and set object status to TRASH
         *            - Update all object_groups with references to the specific object/revision
         */

        let parsed_collection_id = uuid::Uuid::parse_str(&request.collection_id)?;

        // Parse objectids
        let parsed_object_ids = request
            .object_ids
            .iter()
            .map(|objid| uuid::Uuid::parse_str(objid))
            .collect::<Result<Vec<_>, _>>()?;

        self.pg_connection
            .get()?
            .transaction::<_, ArunaError, _>(|conn| {
                delete_multiple_objects(
                    parsed_object_ids,
                    parsed_collection_id,
                    request.force,
                    request.with_revisions,
                    creator_id,
                    conn,
                )?;
                Ok(())
            })?;
        Ok(DeleteObjectsResponse {})
    }

    /// ToDo: Rust Doc
    pub fn add_labels_to_object(
        &self,
        request: AddLabelsToObjectRequest,
    ) -> Result<AddLabelsToObjectResponse, ArunaError> {
        let parsed_object_id = uuid::Uuid::parse_str(&request.object_id)?;
        let parsed_collection_id = uuid::Uuid::parse_str(&request.collection_id)?;
        // Transaction time
        let updated_objects = self
            .pg_connection
            .get()?
            .transaction::<Option<ObjectDto>, Error, _>(|conn| {
                let db_key_values = to_key_values::<ObjectKeyValue>(
                    request.labels_to_add,
                    Vec::new(),
                    parsed_object_id,
                );

                insert_into(object_key_value)
                    .values(&db_key_values)
                    .execute(conn)?;

                get_object(&parsed_object_id, &parsed_collection_id, true, conn)
            })?;

        let mapped = updated_objects
            .map(|e| e.try_into())
            .map_or(Ok(None), |r| r.map(Some))?;

        Ok(AddLabelsToObjectResponse { object: mapped })
    }

    /// ToDo: Rust Doc
    pub fn set_hooks_of_object(
        &self,
        request: SetHooksOfObjectRequest,
    ) -> Result<SetHooksOfObjectResponse, ArunaError> {
        let parsed_object_id = uuid::Uuid::parse_str(&request.object_id)?;
        let parsed_collection_id = uuid::Uuid::parse_str(&request.collection_id)?;
        // Transaction time
        let updated_objects = self
            .pg_connection
            .get()?
            .transaction::<Option<ObjectDto>, Error, _>(|conn| {
                delete(object_key_value)
                    .filter(database::schema::object_key_value::object_id.eq(&parsed_object_id))
                    .filter(
                        database::schema::object_key_value::key_value_type.eq(KeyValueType::HOOK),
                    )
                    .execute(conn)?;

                let new_hooks = request
                    .hooks
                    .iter()
                    .map(|elem| ObjectKeyValue {
                        id: uuid::Uuid::new_v4(),
                        object_id: parsed_object_id,
                        key: elem.key.to_string(),
                        value: elem.value.to_string(),
                        key_value_type: KeyValueType::HOOK,
                    })
                    .collect::<Vec<_>>();

                insert_into(object_key_value)
                    .values(&new_hooks)
                    .execute(conn)?;

                get_object(&parsed_object_id, &parsed_collection_id, true, conn)
            })?;

        let mapped = updated_objects
            .map(|e| e.try_into())
            .map_or(Ok(None), |r| r.map(Some))?;

        Ok(SetHooksOfObjectResponse { object: mapped })
    }
}

/* ----------------- Section for object specific helper functions ------------------- */
/// This is a general helper function that can be use inside already open transactions
/// to update an object in-place without creating a new revision. This should only be
/// used for objects which are still/already in STAGING.
///
/// ## Arguments:
///
/// * `conn: &mut PooledConnection<ConnectionManager<PgConnection>>` - Database connection
/// * `object_uuid: &uuid::Uuid` - Unique object identifier
/// * `stage_object: &StageObject` - Stage object from the init/update request
///
/// ## Returns:
///
/// `Result<aruna_server::database::models::object::Object, Error>` -
/// The Object contains the updated database object
///
pub fn update_object_in_place(
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
    object_uuid: &uuid::Uuid,
    user_uuid: &uuid::Uuid,
    stage_object: &StageObject,
) -> Result<Object, Error> {
    // Get mutable object record from database
    let mut old_object: Object = objects
        .filter(database::schema::objects::id.eq(&object_uuid))
        .first::<Object>(conn)?;

    if let Some(stage_source) = &stage_object.source {
        if let Some(old_source) = &old_object.source_id {
            // Update Source in-place
            update(sources)
                .filter(database::schema::sources::id.eq(&old_source))
                .set((
                    link.eq(&stage_source.identifier),
                    source_type.eq(&SourceType::from_i32(stage_source.source_type)
                        .map_err(|_| Error::RollbackTransaction)?),
                ))
                .execute(conn)?;
        } else {
            // Insert new Source
            let new_source = Source {
                id: uuid::Uuid::new_v4(),
                link: stage_source.identifier.clone(),
                source_type: SourceType::from_i32(stage_source.source_type)
                    .map_err(|_| Error::RollbackTransaction)?,
            };

            insert_into(sources).values(&new_source).execute(conn)?;
        }
    } else if let Some(old_source) = &old_object.source_id {
        // Delete Source
        delete(sources)
            .filter(database::schema::sources::id.eq(&old_source))
            .execute(conn)?;

        old_object.source_id = None;
    } // else, do nothing.

    // Replace labels/hooks
    let key_value_pairs = to_key_values::<ObjectKeyValue>(
        stage_object.labels.clone(),
        stage_object.hooks.clone(),
        *object_uuid,
    );

    delete(object_key_value)
        .filter(database::schema::object_key_value::object_id.eq(&object_uuid))
        .execute(conn)?;
    insert_into(object_key_value)
        .values(&key_value_pairs)
        .execute(conn)?;

    // Update remaining object meta in-place
    old_object.filename = stage_object.filename.to_string();
    old_object.content_len = stage_object.content_len;
    old_object.created_at = chrono::Utc::now().naive_utc();
    old_object.created_by = *user_uuid;
    old_object.dataclass = grpc_to_db_dataclass(&stage_object.dataclass);

    // Update the object record in the database
    let updated_obj = update(objects)
        .filter(database::schema::objects::id.eq(&old_object.id))
        .set(&old_object)
        .get_result::<Object>(conn)?;

    // Return updated db object
    Ok(updated_obj)
}

/// This is a general helper function that can be use inside already open transactions
/// to clone an object.
///
/// ## Arguments:
///
/// * `conn: &mut PooledConnection<ConnectionManager<PgConnection>>` - Database connection
/// * `object_uuid: uuid::Uuid` - Unique object identifier
/// * `source_collection_uuid: uuid::Uuid` - Unique source collection identifier
/// * `target_collection_uuid: uuid::Uuid` - Unique target collection identifier
///
/// ## Resturns:
///
/// `Result<use aruna_rust_api::api::storage::models::Object, Error>` -
/// The Object contains the newly created object clone in its gRPC proto format
///
pub fn clone_object(
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
    object_uuid: uuid::Uuid,
    source_collection_uuid: uuid::Uuid,
    target_collection_uuid: uuid::Uuid,
) -> Result<ProtoObject, Error> {
    // Get original object, collection_object reference, key_values, hash and source
    let mut db_object: Object = objects
        .filter(database::schema::objects::id.eq(&object_uuid))
        .first::<Object>(conn)?;

    let mut db_collection_object: CollectionObject = CollectionObject::belonging_to(&db_object)
        .filter(database::schema::collection_objects::collection_id.eq(&source_collection_uuid))
        .first::<CollectionObject>(conn)?;

    let mut db_object_key_values: Vec<ObjectKeyValue> =
        ObjectKeyValue::belonging_to(&db_object).load::<ObjectKeyValue>(conn)?;

    let db_hash: ApiHash = ApiHash::belonging_to(&db_object).first::<ApiHash>(conn)?;

    let db_source: Option<Source> = match &db_object.source_id {
        None => None,
        Some(src_id) => Some(
            sources
                .filter(database::schema::sources::id.eq(src_id))
                .first::<Source>(conn)?,
        ),
    };

    // Modify object
    db_object.id = uuid::Uuid::new_v4();
    db_object.shared_revision_id = uuid::Uuid::new_v4();
    db_object.revision_number = 0;
    db_object.origin_id = Some(object_uuid);

    // Modify collection_object reference
    db_collection_object.id = uuid::Uuid::new_v4();
    db_collection_object.collection_id = target_collection_uuid;
    db_collection_object.object_id = object_uuid;

    // Modify object_key_values
    for kv in &mut db_object_key_values {
        kv.id = uuid::Uuid::new_v4();
        kv.object_id = db_object.id;
    }

    // Insert object, key_Values and references
    diesel::insert_into(objects)
        .values(&db_object)
        .execute(conn)?;
    diesel::insert_into(object_key_value)
        .values(&db_object_key_values)
        .execute(conn)?;
    diesel::insert_into(collection_objects)
        .values(&db_collection_object)
        .execute(conn)?;

    // Transform everything into gRPC proto format
    let (labels, hooks) = from_key_values(db_object_key_values);
    let timestamp = naivedatetime_to_prost_time(db_object.created_at)
        .map_err(|_| Error::RollbackTransaction)?;

    let proto_source = match db_source {
        None => None,
        Some(source) => Some(ProtoSource {
            identifier: source.link,
            source_type: source.source_type as i32,
        }),
    };

    // Return ProtoObject
    Ok(ProtoObject {
        id: db_object.id.to_string(),
        filename: db_object.filename,
        labels,
        hooks,
        created: Some(timestamp),
        content_len: db_object.content_len,
        status: db_object.object_status as i32,
        origin: Some(ProtoOrigin {
            r#type: 2,
            id: db_object.origin_id.unwrap_or_default().to_string(),
        }),
        data_class: db_object.dataclass as i32,
        hash: Some(ProtoHash {
            alg: db_hash.hash_type as i32,
            hash: db_hash.hash,
        }),
        rev_number: db_object.revision_number,
        source: proto_source,
        latest: db_collection_object.is_latest,
        auto_update: db_collection_object.auto_update,
    })
}

/// This is a helper method that queries the "latest" object based on the current object_uuid.
/// If returned object.id == ref_object_id -> the current object is "latest"
///
/// ## Arguments:
///
/// * `conn: &mut PooledConnection<ConnectionManager<PgConnection>>` - Database connection
/// * `ref_object_id`: `uuid::Uuid` - The Uuid for which the latest Object revision should be found
///
/// ## Returns:
///
/// `Result<use aruna_rust_api::api::storage::models::Object, ArunaError>` -
/// The latest database object or error if the request failed.
///
pub fn get_latest_obj(
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
    ref_object_id: uuid::Uuid,
) -> Result<Object, ArunaError> {
    let shared_id = objects
        .filter(database::schema::objects::id.eq(ref_object_id))
        .select(database::schema::objects::shared_revision_id)
        .first::<uuid::Uuid>(conn)?;

    let latest_object = objects
        .filter(database::schema::objects::shared_revision_id.eq(shared_id))
        .order_by(database::schema::objects::revision_number.desc())
        .first::<Object>(conn)?;

    Ok(latest_object)
}

/// This is a helper method that deletes all specified objects (by id)
/// it has two options (with_force) == true => allow for sideeffects in other collections
/// otherwise this method will error
///
///
/// ## Arguments:
///
/// * `conn: &mut PooledConnection<ConnectionManager<PgConnection>>` - Database connection
/// * `object_ids`: `Vec<uuid::Uuid>` - The Uuids of all objects that should be deleted
/// * `coll_id`: uuid::Uuid - The collection where these ids should be deleted from
/// * `with_force`: bool - Are sideeffects allowed ?
///
/// ## Returns:
///
/// `Result<(), ArunaError>` - Will return empty Result or Error
///
pub fn delete_multiple_objects(
    original_object_ids: Vec<uuid::Uuid>,
    coll_id: uuid::Uuid,
    with_force: bool,
    with_revisions: bool,
    creator_id: uuid::Uuid,
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
) -> Result<(), ArunaError> {
    let object_ids = if with_revisions {
        let shared_ids = objects
            .filter(database::schema::objects::id.eq_any(&original_object_ids))
            .select(database::schema::objects::shared_revision_id)
            .load::<uuid::Uuid>(conn)?;

        objects
            .filter(database::schema::objects::shared_revision_id.eq_any(&shared_ids))
            .select(database::schema::objects::id)
            .load::<uuid::Uuid>(conn)?
    } else {
        original_object_ids.clone()
    };

    // This will query all references and Error if no reference exists
    let references = collection_objects
        .filter(database::schema::collection_objects::object_id.eq_any(&object_ids))
        .load::<CollectionObject>(conn)?;

    // This contains all references per object_id
    let mut object_refs_per_obj: HashMap<uuid::Uuid, Vec<CollectionObject>> = HashMap::new();
    // This contains all specific references for "this" collection
    let mut object_refs_for_col = Vec::new();

    // Populate object_refs_per_obj && object_refs_for_col
    for colref in references {
        match object_refs_per_obj.get_mut(&colref.object_id) {
            Some(arr) => {
                arr.push(colref.clone());
            }
            None => {
                object_refs_per_obj.insert(colref.object_id, vec![colref.clone()]);
            }
        }
        if colref.collection_id == coll_id {
            object_refs_for_col.push(colref)
        }
    }

    // This contains all UUIDs that should only be deleted by reference
    let mut ref_only: Vec<uuid::Uuid> = Vec::new();

    for (k, v) in object_refs_per_obj.clone() {
        // This can have two reasons:
        // A. An object_id was specified that is not present in the collection
        // B. A revision of an object is not referenced in the current collection but in another one

        if object_refs_per_obj.len() != object_refs_for_col.len() {
            let mut in_collection = false;
            for col_ref in &v {
                if col_ref.collection_id == coll_id {
                    in_collection = true;
                }
            }
            if !in_collection {
                // A or B
                if original_object_ids.contains(&k) {
                    // A
                    return Err(ArunaError::InvalidRequest(format!(
                        "ObjectID {} not part of collection: {}",
                        k, coll_id
                    )));
                } else {
                    // B
                    if !with_force {
                        let refed_colls = v.iter().map(|e| e.collection_id).collect::<Vec<_>>();
                        return Err(ArunaError::InvalidRequest(format!(
                            "Object with ID {} is still referenced in collection(s): {:#?}",
                            k, refed_colls
                        )));
                    }
                }
            }
        }

        // If it has more than 1 reference (This could create a dangling reference)
        if v.is_empty() {
            // Assume it is the only writeable
            let mut is_only_writeable = true;
            // Iterate through all other refs
            for other_refs in &v {
                // If another reference is writeable
                if other_refs.collection_id != coll_id && other_refs.writeable {
                    is_only_writeable = false;
                    // This object has other "writeable" targets ->
                    // Delete only the reference, regardless of force or not
                    // add it to the reference only list (-> we can delete this reference only)
                    // Add to ref_only
                    ref_only.push(k);
                    // Remove from hashmap (This should only delete the ref not the object itself)
                    object_refs_per_obj.remove(&k);
                }
                if other_refs.collection_id == coll_id && !other_refs.writeable {
                    // It is not writeable -> Delete only the reference
                    is_only_writeable = false;
                    // Add to ref_only
                    ref_only.push(k);
                    // Remove from hashmap (This should only delete the ref not the object itself)
                    object_refs_per_obj.remove(&k);
                }
            }
            // If it is the only writeable and force is not used
            if is_only_writeable && !with_force {
                return Err(ArunaError::InvalidRequest(format!(
                    "Object with ID {} is the last writeable reference while referenced in other collection(s), delete references or use force",
                    k
                )));
            }
        }
    }

    let mut object_ids_per_coll: HashMap<uuid::Uuid, Vec<uuid::Uuid>> = HashMap::new();
    let mut object_ids_to_delete = Vec::new();

    for v in object_refs_per_obj.values() {
        for e in v {
            if e.collection_id == coll_id {
                object_ids_to_delete.push(e.object_id)
            }

            match object_ids_per_coll.get_mut(&e.collection_id) {
                Some(arr) => {
                    arr.push(e.object_id);
                }
                None => {
                    object_ids_per_coll.insert(e.collection_id, vec![e.object_id]);
                }
            }
        }
    }

    // Delete object_references and update object_groups for "deleteable" without the "reference onlys"
    for (d_coll_id, d_object_ids) in object_ids_per_coll {
        delete_and_bump_objs(&d_object_ids, &d_coll_id, &creator_id, conn)?;
    }

    // Delete and bump the "ref_onlys"
    delete_and_bump_objs(&ref_only, &coll_id, &creator_id, conn)?;

    // Update object_status to "TRASH"
    update(objects)
        .filter(database::schema::objects::id.eq_any(&object_ids_to_delete))
        .set(database::schema::objects::object_status.eq(ObjectStatus::TRASH))
        .execute(conn)?;

    // TODO: Should this also delete key_values etc. ?

    Ok(())
}

/// Query all revisions associated to a specific object.
///
/// ## Arguments:
///
/// * `conn: &mut PooledConnection<ConnectionManager<PgConnection>>` - Database connection
/// * `ref_object_id`: `uuid::Uuid` - the Uuid for the object the latest revisions should be determined for
///
/// ## Resturns:
///
/// `Result<Vec<Object>, ArunaError>` -
/// List of all database objects that are revivisons of the original object
///
pub fn get_all_revisions(
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
    ref_object_id: uuid::Uuid,
) -> Result<Vec<Object>, diesel::result::Error> {
    let shared_id = objects
        .filter(database::schema::objects::id.eq(ref_object_id))
        .select(database::schema::objects::shared_revision_id)
        .first::<uuid::Uuid>(conn)?;

    let all_revision_objects = objects
        .filter(database::schema::objects::shared_revision_id.eq(shared_id))
        .load::<Object>(conn)?;

    Ok(all_revision_objects)
}

/// Query all references for a specific object optionally with revisions
///
/// ## Arguments:
///
/// * `conn: &mut PooledConnection<ConnectionManager<PgConnection>>` - Database connection
/// * `ref_object_id`: `uuid::Uuid` - the Uuid for the object the latest revisions should be determined for
/// * `with_revisions`: `bool` - If true all references for all revisions of the provided object will be returned
///
/// ## Resturns:
///
/// `Result<Vec<CollectionObjects>, ArunaError>` -
/// List of all collectionobjects that reference the object with or without all associated revisions
///
pub fn get_all_references(
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
    ref_object_id: &uuid::Uuid,
    with_revisions: &bool,
) -> Result<Vec<CollectionObject>, diesel::result::Error> {
    if !with_revisions {
        // If with revisions is false -> return just all collection_objects belonging
        // to the current object
        collection_objects
            .filter(database::schema::collection_objects::object_id.eq(ref_object_id))
            .load::<CollectionObject>(conn)
    } else {
        // Alias are needed to join a table with itself
        // see: https://docs.diesel.rs/master/diesel/macro.alias.html
        let (orig_obj, other_obj) = diesel::alias!(
            database::schema::objects as orig_obj,
            database::schema::objects as other_obj
        );

        // Hacky way to query all references in one db request
        orig_obj
            // First filter only the requested object
            .filter(
                orig_obj
                    .field(database::schema::objects::id)
                    .eq(ref_object_id),
            )
            // Join the objects table with itself based on the filtered collection object
            // This should return a new object table that only contains entrys with the same
            // shared_revision_id as the original object
            .inner_join(
                other_obj.on(orig_obj
                    .field(database::schema::objects::shared_revision_id)
                    .eq(other_obj.field(database::schema::objects::shared_revision_id))),
            )
            // Join the result with collection objects on object_id
            // This will result with a combined result with objects <-> collection_objects
            // that should all belong to the same shared revision id
            .inner_join(
                collection_objects.on(other_obj
                    .field(database::schema::objects::id)
                    .eq(database::schema::collection_objects::object_id)),
            )
            // Only select the collection_object section of the result
            // as_select needs a Selectable and table specification on the
            // model: https://docs.diesel.rs/master/diesel/prelude/derive.Selectable.html
            .select(CollectionObject::as_select())
            // Load as list of collection_objects and return
            .load::<CollectionObject>(conn)
    }
}

/// Helper function to query the current database version of an object "ObjectDto"
/// ObjectDto can be transformed to gRPC objects
///
/// ## Arguments:
///
/// * `conn: &mut PooledConnection<ConnectionManager<PgConnection>>` - Database connection
/// * `object_uuid`: `&uuid::Uuid` - The Uuid of the requested object
/// * `collection_uuid` `&uuid::Uuid` - The Uuid of the requesting collection
/// * `include_staging`: bool - Should non finished objects be included
///
/// ## Resturns:
///
/// `Result<use aruna_rust_api::api::storage::models::Object, ArunaError>` -
/// Database representation of an object
///
pub fn get_object(
    object_uuid: &uuid::Uuid,
    collection_uuid: &uuid::Uuid,
    include_staging: bool,
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
) -> Result<Option<ObjectDto>, diesel::result::Error> {
    let object: Object = objects
        .filter(database::schema::objects::id.eq(&object_uuid))
        .first::<Object>(conn)?;

    let object_key_values = ObjectKeyValue::belonging_to(&object).load::<ObjectKeyValue>(conn)?;
    let (labels, hooks) = from_key_values(object_key_values);

    let object_hash: Option<ApiHash> = ApiHash::belonging_to(&object)
        .first::<ApiHash>(conn)
        .optional()?;

    let object_hash = object_hash.unwrap_or_else(|| ApiHash {
        id: uuid::Uuid::default(),
        hash: "".to_string(),
        object_id: uuid::Uuid::default(),
        hash_type: HashType::MD5,
    });

    let source: Option<Source> = match &object.source_id {
        None => None,
        Some(src_id) => Some(
            sources
                .filter(database::schema::sources::id.eq(src_id))
                .first::<Source>(conn)?,
        ),
    };

    let incollection: Option<CollectionObject> = match include_staging {
        true => CollectionObject::belonging_to(&object)
            .filter(database::schema::collection_objects::collection_id.eq(collection_uuid))
            .first::<CollectionObject>(conn)
            .optional()?,
        false => CollectionObject::belonging_to(&object)
            .filter(database::schema::collection_objects::collection_id.eq(collection_uuid))
            .filter(database::schema::collection_objects::reference_status.eq(ReferenceStatus::OK))
            .first::<CollectionObject>(conn)
            .optional()?,
    };

    match incollection {
        Some(colobj) => {
            let latest_object_revision: Option<i64> = objects
                .select(max(database::schema::objects::revision_number))
                .filter(
                    database::schema::objects::shared_revision_id.eq(&object.shared_revision_id),
                )
                .first::<Option<i64>>(conn)?;

            let latest = (match latest_object_revision {
                None => Err(Error::NotFound), // false,
                Some(revision) => Ok(revision == object.revision_number),
            })?;

            Ok(Some(ObjectDto {
                object,
                labels,
                hooks,
                hash: object_hash,
                source,
                latest,
                update: colobj.auto_update,
            }))
        }
        None => Ok(None),
    }
}

/// WARNING: This function should be used with care, it could be used to cross permission boundaries
/// because collections are not checked.
///
/// This function is needed to correctly query all revisions which might not be in any collection anymore.
///
/// Helper function to query the current database version of an object "ObjectDto"
/// ObjectDto can be transformed to gRPC objects
///
/// ## Arguments:
///
/// * `conn: &mut PooledConnection<ConnectionManager<PgConnection>>` - Database connection
/// * `object_uuid`: `&uuid::Uuid` - The Uuid of the requested object
///
/// ## Resturns:
///
/// `Result<use aruna_rust_api::api::storage::models::Object, ArunaError>` -
/// Database representation of an object
///
pub fn get_object_ignore_coll(
    object_uuid: &uuid::Uuid,
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
) -> Result<Option<ObjectDto>, diesel::result::Error> {
    let object: Object = objects
        .filter(database::schema::objects::id.eq(&object_uuid))
        .first::<Object>(conn)?;

    let object_key_values = ObjectKeyValue::belonging_to(&object).load::<ObjectKeyValue>(conn)?;
    let (labels, hooks) = from_key_values(object_key_values);

    let object_hash: ApiHash = ApiHash::belonging_to(&object).first::<ApiHash>(conn)?;

    let source: Option<Source> = match &object.source_id {
        None => None,
        Some(src_id) => Some(
            sources
                .filter(database::schema::sources::id.eq(src_id))
                .first::<Source>(conn)?,
        ),
    };

    let latest_object_revision: Option<i64> = objects
        .select(max(database::schema::objects::revision_number))
        .filter(database::schema::objects::shared_revision_id.eq(&object.shared_revision_id))
        .first::<Option<i64>>(conn)?;

    let latest = (match latest_object_revision {
        None => Err(Error::NotFound), // false,
        Some(revision) => Ok(revision == object.revision_number),
    })?;

    Ok(Some(ObjectDto {
        object,
        labels,
        hooks,
        hash: object_hash,
        source,
        latest,
        update: false, // Always false might not include any collection_info
    }))
}

/// Implement TryFrom for ObjectDto to ProtoObject
///
/// This can convert an ObjectDto to a ProtoObject via built-in try convert functions
///
impl TryFrom<ObjectDto> for ProtoObject {
    type Error = ArunaError;

    fn try_from(object_dto: ObjectDto) -> Result<Self, Self::Error> {
        // Transform db Source to proto Source
        let proto_source = match object_dto.source {
            None => None,
            Some(source) => Some(ProtoSource {
                identifier: source.link,
                source_type: source.source_type as i32,
            }),
        };

        // If object id == origin id --> original uploaded object
        //Note: OriginType only stored implicitly
        let proto_origin: Option<ProtoOrigin> = match object_dto.object.origin_id {
            None => None,
            Some(origin_uuid) => Some(ProtoOrigin {
                id: origin_uuid.to_string(),
                r#type: match object_dto.object.id == origin_uuid {
                    true => 1,
                    false => 2,
                },
            }),
        };

        // Transform NaiveDateTime to Timestamp
        let timestamp = naivedatetime_to_prost_time(object_dto.object.created_at)?;

        // Transform db Hash to proto Hash
        let proto_hash = ProtoHash {
            //alg: object_dto.hash.hash_type as i32,
            alg: match object_dto.hash.hash_type {
                HashType::MD5 => Hashalgorithm::Md5 as i32,
                HashType::SHA1 => Hashalgorithm::Sha1 as i32,
                HashType::SHA256 => Hashalgorithm::Sha256 as i32,
                HashType::SHA512 => Hashalgorithm::Sha512 as i32,
                HashType::MURMUR3A32 => Hashalgorithm::Murmur3a32 as i32,
                HashType::XXHASH32 => Hashalgorithm::Xxhash32 as i32,
            },
            hash: object_dto.hash.hash,
        };

        // Construct proto Object
        Ok(ProtoObject {
            id: object_dto.object.id.to_string(),
            filename: object_dto.object.filename,
            labels: object_dto.labels,
            hooks: object_dto.hooks,
            created: Some(timestamp),
            content_len: object_dto.object.content_len,
            status: db_to_grpc_object_status(object_dto.object.object_status) as i32,
            origin: proto_origin,
            data_class: db_to_grpc_dataclass(&object_dto.object.dataclass) as i32,
            hash: Some(proto_hash),
            rev_number: object_dto.object.revision_number,
            source: proto_source,
            latest: object_dto.latest,
            auto_update: object_dto.update,
        })
    }
}

fn delete_and_bump_objs(
    deletable_objects_uuids: &Vec<uuid::Uuid>,
    target_collection: &uuid::Uuid,
    creator_id: &uuid::Uuid,
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
) -> Result<(), diesel::result::Error> {
    // Remove object_group_object reference and update object_group
    // Query all related object_groups
    let all_coll_obj_grps: Option<Vec<ObjectGroupObject>> = collection_object_groups
        .inner_join(
            object_group_objects.on(database::schema::collection_object_groups::object_group_id
                .eq(database::schema::object_group_objects::object_group_id)),
        )
        .filter(database::schema::object_group_objects::object_id.eq_any(deletable_objects_uuids))
        .filter(database::schema::collection_object_groups::collection_id.eq(target_collection))
        .select(ObjectGroupObject::as_select())
        .load::<ObjectGroupObject>(conn)
        .optional()?;

    // Only proceed if at least one reference exists
    if let Some(coll_obj_grps) = all_coll_obj_grps {
        // Get object_group_ids
        let object_grp_ids = coll_obj_grps
            .iter()
            .map(|e| e.object_group_id)
            .collect::<Vec<_>>();

        // Bump the revision of all related object_groups -> revision_num +1
        let revisioned_ogroups = bump_revisisions(&object_grp_ids, creator_id, conn)?;

        // Parse the returned info as Vec<UUID>
        let new_ids = revisioned_ogroups.iter().map(|e| e.id).collect::<Vec<_>>();

        // Delete all object_group_objects that reference the object_id and are part of the "new" bumped objectgroups
        // Bumping the version will delete the "old" objectgroup reference and create an updated new one
        // This ensures that the history can be preserved when a "soft" delete occurs
        delete(object_group_objects)
            .filter(
                database::schema::object_group_objects::object_id.eq_any(deletable_objects_uuids),
            )
            .filter(database::schema::object_group_objects::object_group_id.eq_any(new_ids))
            .execute(conn)?;
    }
    // Remove collection_object reference for specific collection
    delete(collection_objects)
        .filter(database::schema::collection_objects::object_id.eq_any(deletable_objects_uuids))
        .filter(database::schema::collection_objects::collection_id.eq(target_collection))
        .execute(conn)?;
    Ok(())
}

pub fn check_if_obj_in_coll(
    object_ids: &Vec<uuid::Uuid>,
    collection_uuid: &uuid::Uuid,
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
) -> bool {
    let result = collection_objects
        .filter(database::schema::collection_objects::collection_id.eq(collection_uuid))
        .filter(database::schema::collection_objects::object_id.eq_any(object_ids))
        .select(count(database::schema::collection_objects::object_id))
        .first::<i64>(conn)
        .unwrap_or(0);

    result == (object_ids.len() as i64)
}
