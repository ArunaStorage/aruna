use std::collections::HashMap;

use chrono::Local;
use diesel::dsl::max;
use diesel::r2d2::ConnectionManager;
use diesel::result::Error;
use diesel::{delete, prelude::*, update};
use r2d2::PooledConnection;

use crate::api::aruna::api::storage::services::v1::{
    GetReferencesRequest, GetReferencesResponse, ObjectReference,
};
use crate::api::aruna::api::storage::{
    internal::v1::{Location as ProtoLocation, LocationType},
    models::v1::{
        Hash as ProtoHash, KeyValue, Object as ProtoObject, Origin as ProtoOrigin,
        Source as ProtoSource,
    },
    services::v1::{
        CloneObjectRequest, CloneObjectResponse, CreateObjectReferenceRequest,
        CreateObjectReferenceResponse, DeleteObjectRequest, DeleteObjectResponse,
        FinishObjectStagingRequest, FinishObjectStagingResponse, GetLatestObjectRevisionRequest,
        GetLatestObjectRevisionResponse, GetObjectByIdRequest, GetObjectRevisionsRequest,
        GetObjectRevisionsResponse, GetObjectsRequest, InitializeNewObjectRequest,
        InitializeNewObjectResponse, UpdateObjectRequest, UpdateObjectResponse,
    },
};
use crate::database::models::object_group::ObjectGroupObject;
use crate::error::{ArunaError, GrpcNotFoundError};

use crate::database;
use crate::database::connection::Database;
use crate::database::crud::utils::{
    check_all_for_db_kv, from_object_key_values, naivedatetime_to_prost_time, parse_page_request,
    parse_query, to_object_key_values,
};
use crate::database::models::collection::CollectionObject;
use crate::database::models::enums::{HashType, ObjectStatus, ReferenceStatus, SourceType};
use crate::database::models::object::{
    Endpoint, Hash, Object, ObjectKeyValue, ObjectLocation, Source,
};
use crate::database::schema::{
    collection_object_groups::dsl::*, collection_objects::dsl::*, endpoints::dsl::*,
    hashes::dsl::*, object_group_objects::dsl::*, object_key_value::dsl::*,
    object_locations::dsl::*, objects::dsl::*, sources::dsl::*,
};

use super::objectgroups::bump_revisisions;
use super::utils::{parse_dataclass, ParsedQuery};

// Struct to hold the database objects
pub struct ObjectDto {
    pub object: Object,
    pub labels: Vec<KeyValue>,
    pub hooks: Vec<KeyValue>,
    pub hash: Hash,
    pub source: Option<Source>,
    pub latest: bool,
    pub update: bool,
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
        let object_uuid = uuid::Uuid::new_v4();
        let object = Object {
            id: object_uuid,
            shared_revision_id: uuid::Uuid::new_v4(),
            revision_number: 0,
            filename: staging_object.filename.clone(),
            created_at: Local::now().naive_local(),
            created_by: *creator,
            content_len: 0,
            object_status: ObjectStatus::INITIALIZING,
            dataclass: parse_dataclass(&staging_object.dataclass),
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
        let empty_hash = Hash {
            id: uuid::Uuid::new_v4(),
            hash: "".to_string(), //Note: Empty hash will be updated later
            object_id: object.id,
            hash_type: HashType::MD5, //Note: Default. Will be updated later
        };

        // Convert the object's labels and hooks to their database representation
        let key_value_pairs =
            to_object_key_values(staging_object.labels, staging_object.hooks, object_uuid);

        // Insert all defined objects into the database
        self.pg_connection
            .get()?
            .transaction::<_, Error, _>(|conn| {
                diesel::insert_into(sources).values(&source).execute(conn)?;
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

    pub fn finish_object_staging(
        &self,
        request: &FinishObjectStagingRequest,
        user_id: &uuid::Uuid,
    ) -> Result<FinishObjectStagingResponse, ArunaError> {
        let req_object_uuid = uuid::Uuid::parse_str(&request.object_id)?;
        let req_coll_uuid = uuid::Uuid::parse_str(&request.collection_id)?;

        // Insert all defined objects into the database
        let object_dto = self
            .pg_connection
            .get()?
            .transaction::<ObjectDto, Error, _>(|conn| {
                let latest = get_latest_obj(conn, req_object_uuid)?;

                let is_still_latest = latest.id == req_object_uuid;

                // Update the object itself to be available
                let returned_obj = diesel::update(
                    objects.filter(database::schema::objects::id.eq(req_object_uuid)),
                )
                .set(database::schema::objects::object_status.eq(ObjectStatus::AVAILABLE))
                .get_result::<Object>(conn)?;

                // Check if the origin id is different from uuid
                // This indicates an "updated" object and not a new one
                // Finishing updates need extra steps to update all references
                // In other collections / objectgroups
                if let Some(orig_id) = returned_obj.origin_id {
                    if orig_id != returned_obj.id {
                        // Get all revisions of the object it could be that an older version still has "auto_update" set
                        let all_revisions = get_all_revisions(conn, req_object_uuid)?;
                        // Filter out the UUIDs
                        let all_rev_ids =
                            all_revisions.iter().map(|full| full.id).collect::<Vec<_>>();

                        // Get all CollectionObjects that contain any of the all_rev_ids and are auto_update == true
                        // Set all auto_updates and is_latest to be false
                        let auto_updating_coll_obj = update(
                            collection_objects
                                .filter(
                                    database::schema::collection_objects::object_id
                                        .eq_any(all_rev_ids),
                                )
                                .filter(database::schema::collection_objects::auto_update.eq(true)),
                        )
                        .set((
                            database::schema::collection_objects::auto_update.eq(false),
                            database::schema::collection_objects::is_latest.eq(false),
                        ))
                        .get_results::<CollectionObject>(conn)?;

                        let auto_updating_obj_id = auto_updating_coll_obj
                            .iter()
                            .map(|elem| elem.object_id)
                            .collect::<Vec<_>>();

                        let auto_updating_coll_obj_id = auto_updating_coll_obj
                            .iter()
                            .map(|elem| elem.id)
                            .collect::<Vec<_>>();

                        // Only proceed if the list is not empty, if it is empty no updates need to be performed

                        if !auto_updating_coll_obj.is_empty() {
                            // Query the affected object_groups
                            let affected_object_groups = object_group_objects
                                .filter(
                                    database::schema::object_group_objects::object_id
                                        .eq_any(&auto_updating_obj_id),
                                )
                                .select(database::schema::object_group_objects::object_group_id)
                                .load::<uuid::Uuid>(conn)?;
                            // Bump all revisions for object_groups
                            let new_ogroups =
                                bump_revisisions(&affected_object_groups, user_id, conn)?;
                            let new_group_ids =
                                new_ogroups.iter().map(|group| group.id).collect::<Vec<_>>();
                            // Update Collectionobjects to use the new object_id
                            update(
                                collection_objects.filter(
                                    database::schema::collection_objects::id
                                        .eq_any(&auto_updating_coll_obj_id),
                                ),
                            )
                            .set((
                                database::schema::collection_objects::object_id.eq(req_object_uuid),
                                database::schema::collection_objects::is_latest.eq(true),
                                database::schema::collection_objects::auto_update.eq(true),
                            ))
                            .execute(conn)?;
                            // Update object_group references
                            update(object_group_objects)
                                .filter(
                                    database::schema::object_group_objects::object_group_id
                                        .eq_any(&new_group_ids),
                                )
                                .filter(
                                    database::schema::object_group_objects::object_id.eq(orig_id),
                                )
                                .set(
                                    database::schema::object_group_objects::object_id
                                        .eq(req_object_uuid),
                                )
                                .execute(conn)?;
                        }
                    }
                }

                // Update the collection objects
                // - Status
                // - is_latest
                // - auto_update
                diesel::update(
                    collection_objects.filter(
                        database::schema::collection_objects::object_id.eq(req_object_uuid),
                    ),
                )
                .set((
                    database::schema::collection_objects::is_latest.eq(is_still_latest),
                    database::schema::collection_objects::reference_status.eq(ReferenceStatus::OK),
                    database::schema::collection_objects::auto_update.eq(request.auto_update),
                ))
                .execute(conn)?;
                get_object(&req_object_uuid, &req_coll_uuid, conn)
            })?;

        Ok(FinishObjectStagingResponse {
            object: Some(object_dto.try_into()?),
        })
    }

    ///ToDo: Rust Doc
    pub fn update_object(
        &self,
        request: &UpdateObjectRequest,
        location: &Option<ProtoLocation>,
        creator_uuid: &uuid::Uuid,
        default_endpoint: uuid::Uuid,
    ) -> Result<UpdateObjectResponse, ArunaError> {
        if let Some(sobj) = &request.object {
            let new_obj_id = uuid::Uuid::new_v4();

            let parsed_old_id = uuid::Uuid::parse_str(&request.object_id)?;
            let parsed_col_id = uuid::Uuid::parse_str(&request.collection_id)?;

            self.pg_connection
                .get()?
                .transaction::<_, ArunaError, _>(|conn| {
                    let latest = get_latest_obj(conn, parsed_old_id)?;

                    //Define source object from updated request; None if empty
                    let source: Option<Source> = match &sobj.source {
                        Some(source) => Some(Source {
                            id: uuid::Uuid::new_v4(),
                            link: source.identifier.clone(),
                            source_type: SourceType::from_i32(source.source_type)?,
                        }),
                        _ => None,
                    };

                    let new_object = Object {
                        id: new_obj_id,
                        shared_revision_id: latest.shared_revision_id,
                        revision_number: latest.revision_number + 1,
                        filename: sobj.filename.to_string(),
                        created_at: chrono::Utc::now().naive_utc(),
                        created_by: *creator_uuid,
                        content_len: sobj.content_len,
                        object_status: ObjectStatus::UNAVAILABLE, // Is a staging object
                        dataclass: parse_dataclass(&sobj.dataclass),
                        source_id: source.as_ref().map(|source| source.id),
                        origin_id: Some(parsed_old_id),
                    };
                    // Define the join table entry collection <--> object
                    let collection_object = CollectionObject {
                        id: uuid::Uuid::new_v4(),
                        collection_id: parsed_col_id,
                        is_latest: false, // Will be checked on finish
                        reference_status: ReferenceStatus::STAGING,
                        object_id: new_obj_id,
                        auto_update: false, //Note: Finally set with FinishObjectStagingRequest
                        is_specification: request.is_specification,
                        writeable: true, //Note: Original object is initially always writeable
                    };
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
                            let empty_hash = Hash {
                                id: uuid::Uuid::new_v4(),
                                hash: "".to_string(), //Note: Empty hash will be updated later
                                object_id: new_obj_id,
                                hash_type: HashType::MD5, //Note: Default. Will be updated later
                            };
                            diesel::insert_into(object_locations)
                                .values(&object_location)
                                .execute(conn)?;
                            diesel::insert_into(sources).values(&source).execute(conn)?;
                            diesel::insert_into(hashes)
                                .values(&empty_hash)
                                .execute(conn)?;
                        }
                    }
                    // Define the initial object location
                    // Convert the object's labels and hooks to their database representation
                    // Clone could be removed if the to_object_key_values method takes borrowed vec instead of moved / owned reference
                    let key_value_pairs =
                        to_object_key_values(sobj.labels.clone(), sobj.hooks.clone(), new_obj_id);

                    diesel::insert_into(objects)
                        .values(&new_object)
                        .execute(conn)?;
                    diesel::insert_into(object_key_value)
                        .values(&key_value_pairs)
                        .execute(conn)?;
                    diesel::insert_into(collection_objects)
                        .values(&collection_object)
                        .execute(conn)?;

                    Ok(())
                })?;

            Ok(UpdateObjectResponse {
                object_id: new_obj_id.to_string(),
                staging_id: new_obj_id.to_string(),
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
    pub fn get_object(&self, request: &GetObjectByIdRequest) -> Result<ProtoObject, ArunaError> {
        // Check if id in request has valid format
        let object_uuid = uuid::Uuid::parse_str(&request.object_id)?;
        let collection_uuid = uuid::Uuid::parse_str(&request.collection_id)?;

        // Read object from database
        let object_dto = self
            .pg_connection
            .get()?
            .transaction::<ObjectDto, Error, _>(|conn| {
                // Use the helper function to execute the request
                get_object(&object_uuid, &collection_uuid, conn)
            })?;

        object_dto.try_into()
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
    pub fn get_location_endpoint(&self, location: &ObjectLocation) -> Result<Endpoint, ArunaError> {
        let endpoint = self
            .pg_connection
            .get()?
            .transaction::<Endpoint, Error, _>(|conn| {
                let endpoint: Endpoint = endpoints
                    .filter(database::schema::endpoints::id.eq(&location.endpoint_id))
                    .first::<Endpoint>(conn)?;

                Ok(endpoint)
            })?;

        Ok(endpoint)
    }

    ///ToDo: Rust Doc
    pub fn get_endpoint(&self, endpoint_uuid: &uuid::Uuid) -> Result<Endpoint, ArunaError> {
        let endpoint = self
            .pg_connection
            .get()?
            .transaction::<Endpoint, Error, _>(|conn| {
                let endpoint: Endpoint = endpoints
                    .filter(database::schema::endpoints::id.eq(&endpoint_uuid))
                    .first::<Endpoint>(conn)?;

                Ok(endpoint)
            })?;

        Ok(endpoint)
    }

    ///ToDo: Rust Doc
    pub fn get_latest_object_revision(
        &self,
        _request: GetLatestObjectRevisionRequest,
    ) -> Result<GetLatestObjectRevisionResponse, ArunaError> {
        todo!()

        // Case depends on writeable == true
        //  If writeable == false --> object is borrowed and revisions have to be fetched through
    }

    ///ToDo: Rust Doc
    pub fn get_object_revisions(
        &self,
        _request: GetObjectRevisionsRequest,
    ) -> Result<GetObjectRevisionsResponse, ArunaError> {
        todo!()
    }

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
        use crate::database::schema::object_key_value::dsl as okv;
        use crate::database::schema::objects::dsl as obj;
        use diesel::prelude::*;
        let ret_objects = self
            .pg_connection
            .get()?
            .transaction::<Option<Vec<ObjectDto>>, Error, _>(|conn| {
                // First build a "boxed" base request to which additional parameters can be added later
                let mut base_request = obj::objects.into_boxed();
                // Create returnvector of CollectionOverviewsDb
                let mut return_vec: Vec<ObjectDto> = Vec::new();
                // If pagesize is not unlimited set it to pagesize or default = 20
                if let Some(pg_size) = pagesize {
                    base_request = base_request.limit(pg_size);
                }
                // Add "last_uuid" filter if it is specified
                if let Some(l_uid) = last_uuid {
                    base_request = base_request.filter(obj::id.ge(l_uid));
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
                                    // Always add keys
                                    ckv_query = ckv_query.filter(okv::key.eq(obj_key));
                                    // Will be Some if keys only == false
                                    if let Some(val) = obj_value {
                                        ckv_query = ckv_query.filter(okv::value.eq(val))
                                    };
                                }
                                // Execute request and get a list with all found key values
                                let found_obj_kv: Option<Vec<ObjectKeyValue>> =
                                    ckv_query.load::<ObjectKeyValue>(conn).optional()?;
                                // Parse the returned key_values for the "all" constraint
                                // and only return matching collection ids
                                found_objs = check_all_for_db_kv(found_obj_kv, l_query.0)
                            // If the query is "or"
                            } else {
                                // Query all key / values
                                for (obj_key, obj_value) in l_query.0 {
                                    ckv_query = ckv_query.or_filter(okv::key.eq(obj_key));
                                    // Only Some() if key_only is false
                                    if let Some(val) = obj_value {
                                        ckv_query = ckv_query.filter(okv::value.eq(val))
                                    };
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
                                base_request = base_request.filter(obj::id.eq_any(fobjs))
                            } else {
                                return Ok(None);
                            }
                        }
                        // If the request was an ID request, just filter for all ids
                        // And for uuids makes no sense
                        ParsedQuery::IdsQuery(ids) => {
                            base_request = base_request.filter(obj::id.eq_any(ids));
                        }
                    };
                };

                // Execute the preconfigured query
                let query_collections: Option<Vec<Object>> =
                    base_request.load::<Object>(conn).optional()?;
                // Query overviews for each collection
                // TODO: This might be inefficient and can be optimized later
                if let Some(q_objs) = query_collections {
                    for s_obj in q_objs {
                        return_vec.push(get_object(&s_obj.id, &query_collection_id, conn)?);
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
            .transaction::<_, Error, _>(|conn| {
                let object_ref = collection_objects
                    .filter(database::schema::collection_objects::object_id.eq(parsed_object_id))
                    .filter(
                        database::schema::collection_objects::collection_id
                            .eq(parsed_collection_id),
                    )
                    .first::<CollectionObject>(conn)?;

                // The reference is writeable
                if object_ref.writeable {

                    // Reference is not writeable
                } else {
                    // Consider all "revisions" in this collection
                    if request.with_revisions {

                        // Consider only the specified object reference
                    } else {
                        // Remove object_group_object reference and update object_group

                        // Query all related object_groups
                        let all_coll_obj_grps: Option<Vec<ObjectGroupObject>> =
                            collection_object_groups
                                .inner_join(object_group_objects.on(
                                    database::schema::collection_object_groups::object_group_id.eq(
                                        database::schema::object_group_objects::object_group_id,
                                    ),
                                ))
                                .filter(
                                    database::schema::object_group_objects::object_id
                                        .eq(parsed_object_id),
                                )
                                .filter(
                                    database::schema::collection_object_groups::collection_id
                                        .eq(parsed_collection_id),
                                )
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
                            let revisioned_ogroups =
                                bump_revisisions(&object_grp_ids, &creator_id, conn)?;

                            // Parse the returned info as Vec<UUID>
                            let new_ids =
                                revisioned_ogroups.iter().map(|e| e.id).collect::<Vec<_>>();

                            // Delete all object_group_objects that reference the object_id and are part of the "new" bumped objectgroups
                            // Bumping the version will delete the "old" objectgroup reference and create an updated new one
                            // This ensures that the history can be preserved when a "soft" delete occurs
                            delete(object_group_objects)
                                .filter(
                                    database::schema::object_group_objects::object_id
                                        .eq(parsed_object_id),
                                )
                                .filter(
                                    database::schema::object_group_objects::object_group_id
                                        .eq_any(new_ids),
                                )
                                .execute(conn)?;
                        }
                        // Remove collection_object reference for specific collection
                        delete(collection_objects)
                            .filter(
                                database::schema::collection_objects::object_id
                                    .eq(parsed_object_id),
                            )
                            .filter(
                                database::schema::collection_objects::collection_id
                                    .eq(parsed_collection_id),
                            )
                            .execute(conn)?;
                    }
                }

                Ok(())
            })?;

        Ok(DeleteObjectResponse {})
    }

    //ToDo: Implement higher level database operations
    //      - e.g. get_object_with_labels
    //      - ...
}

/* ----------------- Section for object specific helper functions ------------------- */
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
/// `Result<use crate::api::aruna::api::storage::models::Object, Error>` -
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

    let db_hash: Hash = Hash::belonging_to(&db_object).first::<Hash>(conn)?;

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
    let (labels, hooks) = from_object_key_values(db_object_key_values);
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
            id: db_object.id.to_string(),
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
/// ## Resturns:
///
/// `Result<use crate::api::aruna::api::storage::models::Object, ArunaError>` -
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
///
/// ## Resturns:
///
/// `Result<use crate::api::aruna::api::storage::models::Object, ArunaError>` -
/// Database representation of an object
///
fn get_object(
    object_uuid: &uuid::Uuid,
    collection_uuid: &uuid::Uuid,
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
) -> Result<ObjectDto, diesel::result::Error> {
    let object: Object = objects
        .filter(database::schema::objects::id.eq(&object_uuid))
        .first::<Object>(conn)?;

    let object_key_values = ObjectKeyValue::belonging_to(&object).load::<ObjectKeyValue>(conn)?;
    let (labels, hooks) = from_object_key_values(object_key_values);

    let object_hash: Hash = Hash::belonging_to(&object).first::<Hash>(conn)?;

    let source: Option<Source> = match &object.source_id {
        None => None,
        Some(src_id) => Some(
            sources
                .filter(database::schema::sources::id.eq(src_id))
                .first::<Source>(conn)?,
        ),
    };

    let update: bool = CollectionObject::belonging_to(&object)
        .select(database::schema::collection_objects::auto_update)
        .filter(database::schema::collection_objects::collection_id.eq(collection_uuid))
        .first::<bool>(conn)?;

    let latest_object_revision: Option<i64> = objects
        .select(max(database::schema::objects::revision_number))
        .filter(database::schema::objects::shared_revision_id.eq(&object.shared_revision_id))
        .first::<Option<i64>>(conn)?;

    let latest = match latest_object_revision {
        None => Err(Error::NotFound), // false,
        Some(revision) => Ok(revision == object.revision_number),
    }?;

    Ok(ObjectDto {
        object,
        labels,
        hooks,
        hash: object_hash,
        source,
        latest,
        update,
    })
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
            alg: object_dto.hash.hash_type as i32,
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
            status: object_dto.object.object_status as i32,
            origin: proto_origin,
            data_class: object_dto.object.dataclass as i32,
            hash: Some(proto_hash),
            rev_number: object_dto.object.revision_number,
            source: proto_source,
            latest: object_dto.latest,
            auto_update: object_dto.update,
        })
    }
}
