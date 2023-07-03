use super::objectgroups::bump_revisisions;
use super::utils;
use super::utils::*;
use crate::database;
use crate::database::connection::Database;
use crate::database::crud::collection::is_collection_versioned;
use crate::database::crud::utils::{
    check_all_for_db_kv, db_to_grpc_dataclass, db_to_grpc_hash_type, db_to_grpc_object_status,
    from_key_values, naivedatetime_to_prost_time, parse_page_request, parse_query, to_key_values,
};
use crate::database::models::collection::CollectionObject;
use crate::database::models::collection::CollectionVersion;
use crate::database::models::enums::{
    Dataclass, HashType, KeyValueType, ObjectStatus, ReferenceStatus, Resources, SourceType,
    UserRights,
};
use crate::database::models::object::{EncryptionKey, Hash as Db_Hash, Relation};
use crate::database::models::object::{
    Endpoint, Hash as ApiHash, Object, ObjectKeyValue, ObjectLocation, Source,
};
use crate::database::models::object_group::ObjectGroupObject;
use crate::database::schema::encryption_keys::dsl::encryption_keys;
use crate::database::schema::{
    collection_object_groups::dsl::*, collection_objects::dsl::*, collection_version::dsl::*,
    collections::dsl::*, endpoints::dsl::*, hashes::dsl::*, object_group_objects::dsl::*,
    object_key_value::dsl::*, object_locations::dsl::*, objects::dsl::*, projects::dsl::*,
    relations::dsl::*, sources::dsl::*,
};
use crate::error::{ArunaError, GrpcNotFoundError};
use crate::server::services::authz::Context;
use aruna_rust_api::api::internal::v1::{
    FinalizeObjectRequest, GetOrCreateEncryptionKeyRequest, GetOrCreateObjectByPathRequest,
    GetOrCreateObjectByPathResponse, Location as ProtoLocation, LocationType,
};
use aruna_rust_api::api::storage::services::v1::{
    AddLabelsToObjectRequest, AddLabelsToObjectResponse, CreateObjectPathRequest,
    CreateObjectPathResponse, DeleteObjectsRequest, GetObjectPathRequest, GetObjectPathResponse,
    GetObjectPathsRequest, GetObjectPathsResponse, GetReferencesResponse,
    InitializeNewObjectResponse, ObjectReference, ObjectWithUrl, Path as ProtoPath,
    SetHooksOfObjectRequest, SetHooksOfObjectResponse, StageObject,
};
use aruna_rust_api::api::storage::{
    models::v1::{
        Hash as ProtoHash, Hashalgorithm, KeyValue, Object as ProtoObject, Origin as ProtoOrigin,
        Source as ProtoSource,
    },
    services::v1::{
        CloneObjectRequest, CreateObjectReferenceRequest, CreateObjectReferenceResponse,
        DeleteObjectRequest, FinishObjectStagingRequest, GetLatestObjectRevisionRequest,
        GetObjectByIdRequest, GetObjectRevisionsRequest, GetObjectsByPathRequest,
        GetObjectsByPathResponse, GetObjectsRequest, InitializeNewObjectRequest,
        SetObjectPathVisibilityRequest, SetObjectPathVisibilityResponse, UpdateObjectRequest,
        UpdateObjectResponse,
    },
};
use diesel::dsl::{count, max, min};
use diesel::r2d2::ConnectionManager;
use diesel::result::Error;
use diesel::{delete, insert_into, prelude::*, update};
use diesel_ulid::DieselUlid;
use itertools::Itertools;
use r2d2::PooledConnection;
use rand::distributions::{Alphanumeric, DistString};
use std::collections::HashMap;
use std::convert::TryInto;
use std::hash::Hash;
use std::hash::Hasher;
use std::str::FromStr;
use std::thread;
use std::time;

// Struct to hold a database object with all its assets
#[derive(Debug, Clone)]
pub struct ObjectDto {
    pub object: Object,
    pub labels: Vec<KeyValue>,
    pub hooks: Vec<KeyValue>,
    pub object_hashes: Vec<ApiHash>,
    pub source: Option<Source>,
    pub latest: bool,
    pub update: bool,
}

impl PartialEq for ObjectDto {
    fn eq(&self, other: &Self) -> bool {
        self.object == other.object
            && self.labels == other.labels
            && self.hooks == other.hooks
            && self.object_hashes == other.object_hashes
            && self.source == other.source
            && self.latest == other.latest
            && self.update == other.update
    }
}

impl Eq for ObjectDto {}

/// Implement hash for ObjectDTO but only include database object
impl Hash for ObjectDto {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.object.hash(state);
    }
}

/// Implement TryFrom for ObjectDto to ProtoObject.
/// This can convert an ObjectDto to a ProtoObject via built-in try convert functions.
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

        // Transform NaiveDateTime to Timestamp
        let timestamp = naivedatetime_to_prost_time(object_dto.object.created_at)?;

        let proto_hashes = object_dto
            .object_hashes
            .iter()
            .map(|h| ProtoHash {
                //alg: object_dto.hash.hash_type as i32,
                alg: match h.hash_type {
                    HashType::MD5 => Hashalgorithm::Md5 as i32,
                    HashType::SHA256 => Hashalgorithm::Sha256 as i32,
                    _ => Hashalgorithm::Unspecified as i32,
                },
                hash: h.hash.to_string(),
            })
            .collect::<Vec<ProtoHash>>();

        // Construct proto Object
        Ok(ProtoObject {
            id: object_dto.object.id.to_string(),
            filename: object_dto.object.filename,
            labels: object_dto.labels,
            hooks: object_dto.hooks,
            created: Some(timestamp),
            content_len: object_dto.object.content_len,
            status: db_to_grpc_object_status(object_dto.object.object_status) as i32,
            origin: Some(ProtoOrigin {
                id: object_dto.object.origin_id.to_string(),
            }),
            data_class: db_to_grpc_dataclass(&object_dto.object.dataclass) as i32,
            rev_number: object_dto.object.revision_number,
            source: proto_source,
            latest: object_dto.latest,
            auto_update: object_dto.update,
            hashes: proto_hashes,
        })
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
        creator_uuid: &diesel_ulid::DieselUlid,
        object_uuid: diesel_ulid::DieselUlid,
        endpoint_uuid: &diesel_ulid::DieselUlid,
    ) -> Result<InitializeNewObjectResponse, ArunaError> {
        // Check if StageObject is available
        let staging_object = request.object.clone().ok_or(GrpcNotFoundError::STAGEOBJ)?;

        let collection_uuid =
            diesel_ulid::DieselUlid::from_str(&request.collection_id).map_err(ArunaError::from)?;

        let mut backoff = 2;
        let mut transaction_result;
        let mut connection = self.pg_connection.get()?;
        // Insert staging object with all its needed assets into database
        loop {
            log::info!("Current backoff: {backoff}, create_object");
            transaction_result = connection.transaction::<Object, ArunaError, _>(|conn| {
                create_staging_object(
                    conn,
                    staging_object.clone(),
                    &object_uuid,
                    request.hash.clone(),
                    &collection_uuid,
                    creator_uuid,
                    request.is_specification,
                    Some(endpoint_uuid),
                )
            });

            match &transaction_result {
                Ok(_) => {
                    break;
                }
                Err(err) => match err {
                    ArunaError::DieselError(diesel::result::Error::SerializationError(_))
                    | ArunaError::DieselError(diesel::result::Error::DatabaseError(
                        diesel::result::DatabaseErrorKind::SerializationFailure,
                        _,
                    )) => {
                        thread::sleep(time::Duration::from_millis(backoff as u64));
                        backoff = i32::pow(backoff, 2);
                        if backoff > 100000 {
                            log::warn!("Backoff reached for object insert retries!");
                            break;
                        }
                    }
                    _ => break,
                },
            }
        }

        let created_object = transaction_result?;
        // Return response which is missing the upload id which will be created by upload init
        Ok(InitializeNewObjectResponse {
            object_id: created_object.id.to_string(),
            upload_id: "".to_string(), //Note: Filled later
            collection_id: request.collection_id.clone(),
        })
    }

    /// ToDo: Rust Doc
    pub fn finish_object_staging(
        &self,
        request: &FinishObjectStagingRequest,
        _user_id: &diesel_ulid::DieselUlid,
        //) -> Result<FinishObjectStagingResponse, ArunaError> {
    ) -> Result<Option<ObjectDto>, ArunaError> {
        let req_object_uuid = diesel_ulid::DieselUlid::from_str(&request.object_id)?;
        let req_coll_uuid = diesel_ulid::DieselUlid::from_str(&request.collection_id)?;

        // Try to finish object staging (inside transaction retry logic)
        let mut backoff = 2;
        let mut transaction_result;
        let mut connection = self.pg_connection.get()?;
        loop {
            transaction_result =
                connection.transaction::<Option<ObjectDto>, ArunaError, _>(|conn| {
                    // Check if object is latest!
                    // let latest = get_latest_obj(conn, req_object_uuid)?;
                    // let is_still_latest = latest.id == req_object_uuid;

                    // if !is_still_latest {
                    //     return Err(ArunaError::InvalidRequest(format!(
                    //         "Object {req_object_uuid} is not latest revision. "
                    //     )));
                    // }

                    // What can we do here ?
                    // - Set auto update ?
                    // - Set or check expected hashes
                    // - Finalize EMPTY object ?

                    // // Update the object itself to be available
                    // let returned_obj = diesel::update(
                    //     objects.filter(database::schema::objects::id.eq(req_object_uuid)),
                    // )
                    // .set(database::schema::objects::object_status.eq(ObjectStatus::AVAILABLE))
                    // .get_result::<Object>(conn)?;

                    let mut queried_object: Object = objects
                        .filter(database::schema::objects::id.eq(&req_object_uuid))
                        .first::<Object>(conn)?;

                    // Update hash if (re-)upload and request contains hash
                    if !request.no_upload && request.hash.is_some() {
                        (match &request.hash {
                            None => {
                                return Err(ArunaError::InvalidRequest(
                                    "Missing hash after re-upload.".to_string(),
                                ));
                            }
                            Some(req_hash) => diesel::update(database::schema::hashes::dsl::hashes)
                                .filter(database::schema::hashes::object_id.eq(&req_object_uuid))
                                .set((
                                    database::schema::hashes::hash.eq(&req_hash.hash),
                                    database::schema::hashes::hash_type
                                        .eq(HashType::from_grpc(req_hash.alg)),
                                ))
                                .execute(conn),
                        })?;
                    }

                    // Check if the origin id is different from uuid
                    // This indicates an "updated" object and not a new one
                    // Finishing updates need extra steps to update all references
                    // In other collections / objectgroups
                    if queried_object.object_status != ObjectStatus::AVAILABLE {
                        update(objects)
                            .filter(database::schema::objects::id.eq(&req_object_uuid))
                            .set((database::schema::objects::object_status
                                .eq(ObjectStatus::FINALIZING),))
                            .execute(conn)?;

                        queried_object.object_status = ObjectStatus::FINALIZING;
                    }

                    // Special treatment if only metadata was updated
                    if request.no_upload {
                        // Only on update without upload
                        if queried_object.origin_id != req_object_uuid {
                            // Clone object locations of old object with new object id as data stays the same.
                            let mut cloned_locations = Vec::new();
                            for old_location in object_locations
                                .filter(
                                    database::schema::object_locations::object_id
                                        .eq(&queried_object.origin_id),
                                )
                                .load::<ObjectLocation>(conn)?
                            {
                                cloned_locations.push(ObjectLocation {
                                    id: diesel_ulid::DieselUlid::generate(),
                                    bucket: old_location.bucket,
                                    path: old_location.path,
                                    endpoint_id: old_location.endpoint_id,
                                    object_id: req_object_uuid,
                                    is_primary: old_location.is_primary,
                                    is_encrypted: old_location.is_encrypted,
                                    is_compressed: old_location.is_compressed,
                                });
                            }
                            insert_into(object_locations)
                                .values(&cloned_locations)
                                .execute(conn)?;

                            // Clone encryption keys of old object for specific data hashes
                            let mut cloned_keys = Vec::new();
                            for old_key in encryption_keys
                                .filter(
                                    database::schema::encryption_keys::object_id
                                        .eq(&queried_object.origin_id),
                                )
                                .load::<EncryptionKey>(conn)?
                            {
                                cloned_keys.push(EncryptionKey {
                                    id: diesel_ulid::DieselUlid::generate(),
                                    hash: old_key.hash,
                                    object_id: req_object_uuid,
                                    endpoint_id: old_key.endpoint_id,
                                    is_temporary: old_key.is_temporary,
                                    encryption_key: old_key.encryption_key,
                                });
                            }
                            insert_into(encryption_keys)
                                .values(&cloned_keys)
                                .execute(conn)?;
                        }

                        set_object_available(conn, &queried_object, &req_coll_uuid, None)?;
                        queried_object.object_status = ObjectStatus::AVAILABLE;
                    }

                    Ok(get_object(
                        &req_object_uuid,
                        Some(queried_object),
                        &req_coll_uuid,
                        true,
                        conn,
                    )?)
                });

            match &transaction_result {
                Ok(_) => {
                    break;
                }
                Err(err) => match err {
                    ArunaError::DieselError(diesel::result::Error::SerializationError(_))
                    | ArunaError::DieselError(diesel::result::Error::DatabaseError(
                        diesel::result::DatabaseErrorKind::SerializationFailure,
                        _,
                    )) => {
                        thread::sleep(time::Duration::from_millis(backoff as u64));
                        backoff = i32::pow(backoff, 2);
                        if backoff > 100000 {
                            log::warn!("Backoff reached for object finish retries!");
                            break;
                        }
                    }
                    _ => break,
                },
            }
        }

        transaction_result
    }

    /// Finalizes the object by updating the location, validating the hashes and setting the object
    ///status to `Available`.
    ///
    /// ## Arguments:
    ///
    /// * `Request<FinalizeObjectRequest>` -
    ///   A gRPC request which contains the final object location and the calculated hashes of the objects data.
    ///
    /// ## Returns:
    ///
    /// * `Result<FinalizeObjectResponse, ArunaError>` - An empty FinalizeObjectResponse signals success.
    ///
    /// ## Behaviour:
    ///
    /// Updates the sole existing object location with the provided data of the final location the
    /// object has been moved to. Also validates/creates the provided hashes depending if the individual hash
    /// already exists in the database. Finally the objects status is set to `Available`.
    pub fn finalize_object(
        &self,
        request: &FinalizeObjectRequest,
    ) -> Result<(Relation, Option<Vec<Relation>>), ArunaError> {
        // Check format of provided ids
        let object_uuid = diesel_ulid::DieselUlid::from_str(&request.object_id)?;
        let collection_uuid = diesel_ulid::DieselUlid::from_str(&request.collection_id)?;

        // Extract SHA256 hash from provided hashes for easier usage
        let mut sha256_hash = "".to_string();
        for proto_hash in &request.hashes {
            if proto_hash.alg == Hashalgorithm::Sha256 as i32 {
                sha256_hash = proto_hash.hash.clone();
                break;
            }
        }

        // Validate SHA256 hash is provided with the request
        if sha256_hash.is_empty() {
            return Err(ArunaError::InvalidRequest(format!(
                "No SHA256 hash provided to finalize object {object_uuid}"
            )));
        }

        // Start transaction to update object assets
        let result = self
            .pg_connection
            .get()?
            .transaction::<(Relation, Option<Vec<Relation>>), ArunaError, _>(|conn| {
                // Check if object is latest!
                let latest = get_latest_obj(conn, object_uuid)?;
                let is_still_latest = latest.id == object_uuid;

                if !is_still_latest {
                    return Err(ArunaError::InvalidRequest(format!(
                        "Object {object_uuid} is not latest revision."
                    )));
                }

                use crate::database::schema::encryption_keys::dsl as keys_dsl;
                if let Some(proto_location) = &request.location {
                    let endpoint_uuid =
                        diesel_ulid::DieselUlid::from_str(proto_location.endpoint_id.as_str())?;

                    let final_location = ObjectLocation {
                        id: diesel_ulid::DieselUlid::generate(),
                        bucket: proto_location.bucket.clone(),
                        path: proto_location.path.clone(),
                        endpoint_id: endpoint_uuid,
                        object_id: object_uuid,
                        is_primary: true, // First location of object, so primary.
                        is_encrypted: proto_location.is_encrypted,
                        is_compressed: proto_location.is_compressed,
                    };

                    if let Some(enc_key) = encryption_keys
                        .filter(keys_dsl::hash.eq(&sha256_hash))
                        .filter(keys_dsl::endpoint_id.eq(&endpoint_uuid))
                        .first::<EncryptionKey>(conn)
                        .optional()?
                    {
                        if enc_key.object_id != object_uuid {
                            let encryption_key_insert = EncryptionKey {
                                id: diesel_ulid::DieselUlid::generate(),
                                hash: Some(sha256_hash.to_string()),
                                object_id: object_uuid,
                                endpoint_id: endpoint_uuid,
                                is_temporary: false,
                                encryption_key: enc_key.encryption_key,
                            };

                            diesel::insert_into(encryption_keys)
                                .values(&encryption_key_insert)
                                .execute(conn)?;
                        }
                    } else {
                        let encryption_key_insert = EncryptionKey {
                            id: diesel_ulid::DieselUlid::generate(),
                            hash: Some(sha256_hash.to_string()),
                            object_id: object_uuid,
                            endpoint_id: endpoint_uuid,
                            is_temporary: false,
                            encryption_key: proto_location.encryption_key.to_string(),
                        };

                        diesel::insert_into(encryption_keys)
                            .values(&encryption_key_insert)
                            .execute(conn)?;
                    }

                    // Delete all temporary encryption keys associated with this object_id
                    delete(encryption_keys)
                        .filter(database::schema::encryption_keys::object_id.eq(&object_uuid))
                        .filter(database::schema::encryption_keys::is_temporary.eq(true))
                        .execute(conn)?;

                    insert_into(object_locations)
                        .values(&final_location)
                        .execute(conn)?;
                } else {
                    return Err(ArunaError::InvalidRequest(format!(
                        "Request contains no valid location to finalize object {}",
                        object_uuid
                    )));
                }

                let db_hashes = hashes
                    .filter(database::schema::hashes::object_id.eq(&object_uuid))
                    .load::<ApiHash>(conn)?;

                // Validate all data proxy calculated hashes against existing
                let mut hashes_insert = Vec::new();
                for proto_hash in &request.hashes {
                    for db_hash in &db_hashes {
                        if grpc_to_db_hash_type(&proto_hash.alg)? == db_hash.hash_type {
                            if proto_hash.hash == db_hash.hash || db_hash.hash.is_empty() {
                                break;
                            } else {
                                return Err(ArunaError::InvalidRequest(format!(
                                    "Provided hash: {:#?} != Calculated hash: {:#?}",
                                    db_hash, proto_hash
                                )));
                            }
                        }
                    }

                    // Store hash for database insert after loop
                    hashes_insert.push(Db_Hash {
                        id: diesel_ulid::DieselUlid::generate(),
                        hash: proto_hash.hash.to_string(),
                        object_id: object_uuid,
                        hash_type: grpc_to_db_hash_type(&proto_hash.alg)?,
                    });
                }

                // Delete all existing hashes -> Will be inserted by proxy hashes
                delete(hashes)
                    .filter(database::schema::hashes::object_id.eq(&object_uuid))
                    .execute(conn)?;

                // Insert all object hashes which do not already exist
                insert_into(hashes).values(hashes_insert).execute(conn)?;

                if latest.object_status != ObjectStatus::AVAILABLE {
                    set_object_available(
                        conn,
                        &latest,
                        &collection_uuid,
                        Some(request.content_length),
                    )?;
                }

                // Fetch one of the collection specific object relations for event notification
                let object_relation = relations
                    .filter(crate::database::schema::relations::object_id.eq(&object_uuid))
                    .filter(crate::database::schema::relations::collection_id.eq(&collection_uuid))
                    .first::<Relation>(conn)?;

                // If object revision > 0 also fetch all relations of old object for notification
                let old_relations = if latest.revision_number > 0 {
                    Some(
                        relations
                            .filter(
                                crate::database::schema::relations::object_id.eq(&latest.origin_id),
                            )
                            .load::<Relation>(conn)?,
                    )
                } else {
                    None
                };

                Ok((object_relation, old_relations))
            });

        // Check if finalize succeeded. If not, set object status to ERROR and add internal label with truncated error message
        match result {
            Ok((rel, old_rels)) => Ok((rel, old_rels)),
            Err(err) => {
                self.pg_connection
                    .get()?
                    .transaction::<_, ArunaError, _>(|conn| {
                        use crate::database::schema::objects::dsl as object_dsl;

                        // Set object status to ERROR
                        update(objects)
                            .filter(object_dsl::id.eq(&object_uuid))
                            .set(object_dsl::object_status.eq(ObjectStatus::ERROR))
                            .execute(conn)?;

                        // Truncate error message to at most 255 characters
                        let mut error_msg = err.to_string();
                        error_msg.truncate(255);

                        // Add label with error message
                        let error_label = ObjectKeyValue {
                            id: diesel_ulid::DieselUlid::generate(),
                            object_id: object_uuid,
                            key: "app.aruna-storage.org/error".to_string(),
                            value: error_msg,
                            key_value_type: KeyValueType::LABEL,
                        };

                        insert_into(object_key_value)
                            .values(error_label)
                            .execute(conn)?;

                        Ok(())
                    })?;

                Err(err)
            }
        }
    }

    ///ToDo: Rust Doc
    pub fn update_object(
        &self,
        request: UpdateObjectRequest,
        creator_uuid: &diesel_ulid::DieselUlid,
        new_obj_id: diesel_ulid::DieselUlid,
        endpoint_uuid: &diesel_ulid::DieselUlid,
    ) -> Result<UpdateObjectResponse, ArunaError> {
        if let Some(sobj) = request.object {
            let parsed_old_id = diesel_ulid::DieselUlid::from_str(&request.object_id)?;
            let parsed_col_id = diesel_ulid::DieselUlid::from_str(&request.collection_id)?;

            let staging_object = self
                .pg_connection
                .get()?
                .transaction::<Object, ArunaError, _>(|conn| {
                    let staging_object = update_object_init(
                        conn,
                        sobj,
                        parsed_old_id,
                        new_obj_id,
                        parsed_col_id,
                        creator_uuid,
                        Some(endpoint_uuid),
                        request.reupload,
                        request.is_specification,
                    )?;

                    Ok(staging_object)
                })?;

            Ok(UpdateObjectResponse {
                object_id: staging_object.id.to_string(),
                staging_id: staging_object.id.to_string(),
                collection_id: parsed_col_id.to_string(),
            })
        } else {
            Err(ArunaError::InvalidRequest(
                "Staging object must be provided".to_string(),
            ))
        }
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
    pub fn get_object(&self, request: &GetObjectByIdRequest) -> Result<ObjectWithUrl, ArunaError> {
        // Check if id in request has valid format
        let object_uuid = diesel_ulid::DieselUlid::from_str(&request.object_id)?;
        let collection_uuid = diesel_ulid::DieselUlid::from_str(&request.collection_id)?;

        // Read object from database
        let (object_dto, obj_paths) =
            self.pg_connection
                .get()?
                .transaction::<(Option<ObjectDto>, Vec<ProtoPath>), Error, _>(|conn| {
                    // Use the helper function to execute the request
                    let object = get_object(&object_uuid, None, &collection_uuid, true, conn)?;
                    let proto_paths = if let Some(obj) = object.clone() {
                        get_paths_proto(
                            &obj.object.shared_revision_id,
                            Some(&collection_uuid),
                            conn,
                        )?
                    } else {
                        Vec::new()
                    };
                    Ok((object, proto_paths))
                })?;

        let proto_object: Option<ProtoObject> = object_dto
            .map(|e| e.try_into())
            .map_or(Ok(None), |r| r.map(Some))?;

        let path_strings = obj_paths
            .iter()
            .map(|p| p.path.clone())
            .collect::<Vec<String>>();

        Ok(ObjectWithUrl {
            object: proto_object,
            url: "".to_string(),
            paths: path_strings,
        })
    }

    /// Returns the object from the database in its gRPC format. This function ignores
    /// whether the specific object has its own reference, and only checks whether any
    /// revision of the object exists in the collection.
    ///
    /// ## Arguments
    ///
    /// * `request` - A gRPC request containing the needed information to get the specific object
    ///
    /// ## Returns
    ///
    /// * `Result<aruna_server::database::models::object::Object, ArunaError>` -
    /// Database model object if the object with the provided id exists in the database.

    pub fn get_object_by_id(
        &self,
        object_uuid: &diesel_ulid::DieselUlid,
        collection_uuid: &diesel_ulid::DieselUlid,
    ) -> Result<ObjectWithUrl, ArunaError> {
        // Read object and paths from database

        let mut backoff = 2;
        let mut transaction_result;
        let mut connection = self.pg_connection.get()?;
        // Insert all defined objects into the database

        loop {
            transaction_result = connection
                .transaction::<(Option<ObjectDto>, Vec<ProtoPath>), ArunaError, _>(|conn| {
                    // Check if object exists in collection
                    if !object_exists_in_collection(conn, object_uuid, collection_uuid, true)? {
                        return Err(ArunaError::InvalidRequest(format!(
                            "Object {object_uuid} does not exist in collection {collection_uuid}."
                        )));
                    }

                    // Try to get object without reference needed and its associated paths
                    let object_dto_option = get_object_ignore_coll(object_uuid, conn)?;

                    let proto_paths = if let Some(object_dto) = object_dto_option.clone() {
                        get_paths_proto(
                            &object_dto.object.shared_revision_id,
                            Some(collection_uuid),
                            conn,
                        )?
                    } else {
                        Vec::new()
                    };

                    Ok((object_dto_option, proto_paths))
                });

            match &transaction_result {
                Ok(_) => {
                    break;
                }
                Err(err) => match err {
                    ArunaError::DieselError(diesel::result::Error::SerializationError(_))
                    | ArunaError::DieselError(diesel::result::Error::DatabaseError(
                        diesel::result::DatabaseErrorKind::SerializationFailure,
                        _,
                    )) => {
                        thread::sleep(time::Duration::from_millis(backoff as u64));
                        backoff = i32::pow(backoff, 2);
                        if backoff > 100000 {
                            log::warn!("Backoff reached for object fetch retries!");
                            break;
                        }
                    }
                    _ => break,
                },
            }
        }

        let (object_dto, obj_paths) = transaction_result?;

        let proto_object: Option<ProtoObject> = object_dto
            .map(|e| e.try_into())
            .map_or(Ok(None), |r| r.map(Some))?;

        let path_strings = obj_paths
            .iter()
            .map(|p| p.path.clone())
            .collect::<Vec<String>>();

        Ok(ObjectWithUrl {
            object: proto_object,
            url: "".to_string(),
            paths: path_strings,
        })
    }

    ///ToDo: Rust Doc

    pub fn get_primary_object_location(
        &self,
        object_uuid: &diesel_ulid::DieselUlid,
    ) -> Result<ProtoLocation, ArunaError> {
        use crate::database::schema::encryption_keys::dsl as keys_dsl;
        use crate::database::schema::object_locations::dsl as locations_dsl;

        let location = self
            .pg_connection
            .get()?
            .transaction::<ProtoLocation, Error, _>(|conn| {
                let location: ObjectLocation = object_locations
                    .filter(locations_dsl::object_id.eq(&object_uuid))
                    .filter(locations_dsl::is_primary.eq(true))
                    .first::<ObjectLocation>(conn)?;

                // Only query encryption key if object location is encrypted
                let encryption_key: Option<String> = if location.is_encrypted {
                    encryption_keys
                        .filter(keys_dsl::object_id.eq(&object_uuid))
                        .filter(keys_dsl::endpoint_id.eq(&location.endpoint_id))
                        .select(keys_dsl::encryption_key)
                        .first::<String>(conn)
                        .optional()?
                } else {
                    None
                };

                Ok(ProtoLocation {
                    r#type: LocationType::S3 as i32, //ToDo: How to get LocationType? Query Endpoint...
                    bucket: location.bucket,
                    path: location.path,
                    endpoint_id: location.endpoint_id.to_string(),
                    is_compressed: location.is_compressed,
                    is_encrypted: location.is_encrypted,
                    encryption_key: if let Some(enc_key) = encryption_key {
                        enc_key
                    } else {
                        "".to_string()
                    },
                })
            })?;

        Ok(location)
    }

    ///ToDo: Rust Doc

    pub fn get_primary_object_location_with_endpoint(
        &self,
        object_uuid: &diesel_ulid::DieselUlid,
    ) -> Result<(ObjectLocation, Endpoint, Option<EncryptionKey>), ArunaError> {
        use crate::database::schema::encryption_keys::dsl as keys_dsl;

        let location_info =
            self.pg_connection
                .get()?
                .transaction::<(ObjectLocation, Endpoint, Option<EncryptionKey>), ArunaError, _>(
                    |conn| {
                        let location: ObjectLocation = object_locations
                            .filter(database::schema::object_locations::object_id.eq(&object_uuid))
                            .filter(database::schema::object_locations::is_primary.eq(true))
                            .first::<ObjectLocation>(conn)?;

                        let endpoint: Endpoint = endpoints
                            .filter(database::schema::endpoints::id.eq(&location.endpoint_id))
                            .first::<Endpoint>(conn)?;

                        // Only query encryption key if object location is encrypted
                        let encryption_key = if location.is_encrypted {
                            encryption_keys
                                .filter(keys_dsl::object_id.eq(&object_uuid))
                                .filter(keys_dsl::endpoint_id.eq(&endpoint.id))
                                .first::<EncryptionKey>(conn)
                                .optional()?
                        } else {
                            None
                        };

                        if location.is_encrypted && encryption_key.is_none() {
                            return Err(ArunaError::InvalidRequest("".to_string()));
                        }

                        Ok((location, endpoint, encryption_key))
                    },
                )?;

        Ok(location_info)
    }

    /// Get the full location info of the provided object as database objects:
    ///
    /// * Primary object location
    /// * Endpoint associated with the primary location
    /// * Optional encryption key if the location is encrypted
    /// * A vector of all active paths associated with the provided collection
    ///
    /// ## Arguments:
    ///
    /// * `object_uuid: &diesel_ulid::DieselUlid` - Unique object identifier for staging object
    /// * `collection_uuid: &diesel_ulid::DieselUlid` - Unique collection identifier
    ///
    /// ## Returns:
    ///
    /// * `Result<(ObjectLocation, Endpoint, Option<EncryptionKey>, Vec<Path>), ArunaError>` -
    ///     * Primary object location
    ///     * Endpoint associated with the primary location
    ///     * Optional encryption key if the location is encrypted
    ///     * A vector of all active paths associated with the provided collection
    ///
    /// ## Behaviour:
    ///
    /// If the object is empty (i.e. no data was uploaded in any revision) this function will
    /// fail as the object has no location and path in the database.
    ///
    pub fn get_primary_object_location_with_endpoint_and_paths(
        &self,
        object_uuid: &diesel_ulid::DieselUlid,
        collection_uuid: &diesel_ulid::DieselUlid,
    ) -> Result<
        (
            ObjectLocation,
            Endpoint,
            Option<EncryptionKey>,
            Vec<Relation>,
        ),
        ArunaError,
    > {
        use crate::database::schema::encryption_keys::dsl as keys_dsl;
        use crate::database::schema::objects::dsl as objects_dsl;
        use crate::database::schema::relations::dsl as relations_dsl;

        let location_info = self.pg_connection.get()?.transaction::<(
            ObjectLocation,
            Endpoint,
            Option<EncryptionKey>,
            Vec<Relation>,
        ), ArunaError, _>(|conn| {
            // Fetch shared_revision_id of provided object
            let shared_revision_uuid = objects
                .filter(objects_dsl::id.eq(object_uuid))
                .select(objects_dsl::shared_revision_id)
                .first::<diesel_ulid::DieselUlid>(conn)?;

            // Fetch all paths associated with the shared_revision_id
            let all_paths = relations_dsl::relations
                .filter(relations_dsl::collection_id.eq(&collection_uuid))
                .filter(relations_dsl::shared_revision_id.eq(&shared_revision_uuid))
                .load::<Relation>(conn)?;

            // Fetch primary location of the object
            let location: ObjectLocation = object_locations
                .filter(database::schema::object_locations::object_id.eq(&object_uuid))
                .filter(database::schema::object_locations::is_primary.eq(true))
                .first::<ObjectLocation>(conn)?;

            // Fetch endpoint info associated with the primary location
            let endpoint: Endpoint = endpoints
                .filter(database::schema::endpoints::id.eq(&location.endpoint_id))
                .first::<Endpoint>(conn)?;

            // Only query encryption key if object location is encrypted
            let encryption_key = if location.is_encrypted {
                encryption_keys
                    .filter(keys_dsl::object_id.eq(&object_uuid))
                    .filter(keys_dsl::endpoint_id.eq(&endpoint.id))
                    .first::<EncryptionKey>(conn)
                    .optional()?
            } else {
                None
            };

            if location.is_encrypted && encryption_key.is_none() {
                return Err(ArunaError::InvalidRequest("".to_string()));
            }

            Ok((location, endpoint, encryption_key, all_paths))
        })?;

        Ok(location_info)
    }

    /// Get an object with its location for a specific endpoint. The data specific
    /// encryption/decryption key will be returned also if available.
    ///
    /// ## Arguments:
    ///
    ///
    ///
    /// ## Returns:
    ///
    ///
    pub fn get_object_with_location_info(
        &self,
        object_path: &String,
        object_revision: i64,
        endpoint_uuid: &diesel_ulid::DieselUlid,
        token_uuid: diesel_ulid::DieselUlid,
    ) -> Result<(ProtoObject, ObjectLocation, Endpoint, Option<EncryptionKey>), ArunaError> {
        use crate::database::schema::encryption_keys::dsl as keys_dsl;
        use crate::database::schema::endpoints::dsl as endpoints_dsl;
        use crate::database::schema::object_locations::dsl as locations_dsl;

        let location_info = self.pg_connection.get()?.transaction::<(
            ProtoObject,
            ObjectLocation,
            Endpoint,
            Option<EncryptionKey>,
        ), ArunaError, _>(|conn| {
            let (s3bucket, _) = object_path[5..]
                .split_once('/')
                .ok_or(ArunaError::InvalidRequest("Invalid path".to_string()))?;

            let (_, collection_uuid_option) =
                get_project_collection_ids_of_bucket_path(conn, s3bucket.to_string())?;
            let collection_uuid = collection_uuid_option.ok_or(ArunaError::InvalidRequest(
                format!("Collection in path {} does not exist.", object_path),
            ))?;

            // Check permissions
            self.get_checked_user_id_from_token(
                &token_uuid,
                &Context {
                    user_right: UserRights::READ,
                    resource_type: Resources::COLLECTION,
                    resource_id: collection_uuid,
                    admin: false,
                    personal: false,
                    oidc_context: false,
                    allow_service_accounts: true,
                },
            )?;

            let db_object =
                get_object_revision_by_path(conn, object_path, object_revision, None, false)?
                    .ok_or_else(|| {
                        ArunaError::InvalidRequest(format!(
                            "Could not find object for path {object_path}"
                        ))
                    })?;
            let proto_object: ProtoObject =
                if let Some(object_dto) = get_object_ignore_coll(&db_object.id, conn)? {
                    object_dto.try_into()?
                } else {
                    return Err(ArunaError::InvalidRequest(format!(
                        "Could not find object {}",
                        db_object.id
                    )));
                };
            let location: ObjectLocation = object_locations
                .filter(locations_dsl::object_id.eq(&db_object.id))
                .filter(locations_dsl::endpoint_id.eq(&endpoint_uuid))
                .first::<ObjectLocation>(conn)?;

            let endpoint: Endpoint = endpoints
                .filter(endpoints_dsl::id.eq(&location.endpoint_id))
                .first::<Endpoint>(conn)?;

            let sha_hash = proto_object
                .hashes
                .iter()
                .find(|e| e.alg == Hashalgorithm::Sha256 as i32);

            // Only query encryption key if object location is encrypted
            let encryption_key = if location.is_encrypted {
                match encryption_keys
                    .filter(keys_dsl::object_id.eq(&db_object.id))
                    .filter(keys_dsl::endpoint_id.eq(&endpoint.id))
                    .first::<EncryptionKey>(conn)
                    .optional()?
                {
                    Some(k) => Some(k),
                    None => match sha_hash {
                        Some(h) => encryption_keys
                            .filter(keys_dsl::hash.eq(&h.hash))
                            .filter(keys_dsl::endpoint_id.eq(&endpoint.id))
                            .first::<EncryptionKey>(conn)
                            .optional()?,
                        None => None,
                    },
                }
            } else {
                None
            };

            if location.is_encrypted && encryption_key.is_none() {
                return Err(ArunaError::InvalidRequest(
                    "No encryption key found for encrypted location.".to_string(),
                ));
            }

            Ok((proto_object, location, endpoint, encryption_key))
        })?;

        Ok(location_info)
    }

    /// ToDo: Rust Doc
    pub fn get_object_locations(
        &self,
        object_uuid: &diesel_ulid::DieselUlid,
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

    /// Get the encryption key associated with the object, its hash and the specific endpoint.
    /// If the key does not exist a new one will be created.
    ///
    /// ## Arguments:
    ///
    /// * `Request<GetEncryptionKeyRequest>` -
    ///   A gRPC request which contains the information needed to query a specific encryption key.
    ///
    /// ## Returns:
    ///
    /// * `Result<Option<EncryptionKey>, ArunaError>` - Contains the object data encryption/decryption key if found; None else.
    ///
    /// ## Behaviour:
    ///
    /// The encryption key is only fetched for objects with the data class 'Public' or 'Private'. An error is thrown for
    /// objects with the data class 'Confidential' or 'Protected' as they have to provide their encryption/decryption
    /// keys within the request header.
    pub fn get_or_create_encryption_key(
        &self,
        request: &GetOrCreateEncryptionKeyRequest,
    ) -> Result<(Option<EncryptionKey>, bool), ArunaError> {
        use crate::database::schema::encryption_keys::dsl as keys_dsl;

        // Parse endpoint id from request
        let endpoint_uuid = diesel_ulid::DieselUlid::from_str(&request.endpoint_id)?;

        let key_info = self
            .pg_connection
            .get()?
            .transaction::<(Option<EncryptionKey>, bool), ArunaError, _>(|conn| {
                // Path -> Fetch Object
                //      Object != PUBLIC | PRIVATE --> return None
                //      Object == PUBLIC | PRIVATE --> request.hash == encryption_keys.hash --> return encryption key
                let req_object = get_object_revision_by_path(conn, &request.path, -1, None, true)?
                    .ok_or_else(|| {
                        ArunaError::InvalidRequest(format!(
                            "Could not find object for path {}",
                            request.path
                        ))
                    })?;

                // First check for matching object_ids afterwards check
                let (encryption_key, created) = if let Some(is_key) = match encryption_keys
                    .filter(keys_dsl::object_id.eq(&req_object.id))
                    .filter(keys_dsl::endpoint_id.eq(&endpoint_uuid))
                    .first::<EncryptionKey>(conn)
                    .optional()?
                {
                    Some(k) => Some(k),
                    None => {
                        if !request.hash.is_empty() {
                            match encryption_keys
                                .filter(keys_dsl::hash.eq(&request.hash))
                                .filter(keys_dsl::endpoint_id.eq(&endpoint_uuid))
                                .first::<EncryptionKey>(conn)
                                .optional()?
                            {
                                Some(kk) => {
                                    let encryption_key_insert = EncryptionKey {
                                        id: diesel_ulid::DieselUlid::generate(),
                                        hash: Some(request.hash.clone()),
                                        object_id: req_object.id,
                                        endpoint_id: endpoint_uuid,
                                        is_temporary: false,
                                        encryption_key: kk.encryption_key,
                                    };

                                    insert_into(encryption_keys)
                                        .values(&encryption_key_insert)
                                        .execute(conn)?;

                                    Some(encryption_key_insert)
                                }
                                None => None,
                            }
                        } else {
                            None
                        }
                    }
                } {
                    match req_object.dataclass {
                        Dataclass::PUBLIC | Dataclass::PRIVATE => (Some(is_key), false),
                        _ => {
                            let encryption_key_insert = if request.hash.is_empty() {
                                EncryptionKey {
                                    id: diesel_ulid::DieselUlid::generate(),
                                    hash: None,
                                    object_id: req_object.id,
                                    endpoint_id: endpoint_uuid,
                                    is_temporary: true,
                                    encryption_key: Alphanumeric
                                        .sample_string(&mut rand::thread_rng(), 32),
                                }
                            } else {
                                EncryptionKey {
                                    id: diesel_ulid::DieselUlid::generate(),
                                    hash: Some(request.hash.to_string()),
                                    object_id: req_object.id,
                                    endpoint_id: endpoint_uuid,
                                    is_temporary: false,
                                    encryption_key: Alphanumeric
                                        .sample_string(&mut rand::thread_rng(), 32),
                                }
                            };

                            insert_into(encryption_keys)
                                .values(&encryption_key_insert)
                                .execute(conn)?;

                            (Some(encryption_key_insert), true)
                        }
                    }
                } else {
                    let encryption_key_insert = if request.hash.is_empty() {
                        EncryptionKey {
                            id: diesel_ulid::DieselUlid::generate(),
                            hash: None,
                            object_id: req_object.id,
                            endpoint_id: endpoint_uuid,
                            is_temporary: true,
                            encryption_key: Alphanumeric.sample_string(&mut rand::thread_rng(), 32),
                        }
                    } else {
                        EncryptionKey {
                            id: diesel_ulid::DieselUlid::generate(),
                            hash: Some(request.hash.to_string()),
                            object_id: req_object.id,
                            endpoint_id: endpoint_uuid,
                            is_temporary: false,
                            encryption_key: Alphanumeric.sample_string(&mut rand::thread_rng(), 32),
                        }
                    };

                    insert_into(encryption_keys)
                        .values(&encryption_key_insert)
                        .execute(conn)?;

                    (Some(encryption_key_insert), true)
                };

                Ok((encryption_key, created))
            })?;

        Ok(key_info)
    }

    ///ToDo: Rust Doc
    pub fn get_latest_object_revision(
        &self,
        request: GetLatestObjectRevisionRequest,
    ) -> Result<ObjectWithUrl, ArunaError> {
        let parsed_object_id = diesel_ulid::DieselUlid::from_str(&request.object_id)?;
        let parsed_collection_id = diesel_ulid::DieselUlid::from_str(&request.collection_id)?;

        let (object_dto, obj_paths) =
            self.pg_connection
                .get()?
                .transaction::<(Option<ObjectDto>, Vec<ProtoPath>), Error, _>(|conn| {
                    let lat_obj = get_latest_obj(conn, parsed_object_id)?;
                    let obj = get_object(
                        &lat_obj.id,
                        Some(lat_obj.clone()),
                        &parsed_collection_id,
                        false,
                        conn,
                    )?;

                    let proto_paths = if let Some(obj) = obj.clone() {
                        get_paths_proto(
                            &obj.object.shared_revision_id,
                            Some(&parsed_collection_id),
                            conn,
                        )?
                    } else {
                        Vec::new()
                    };
                    Ok((obj, proto_paths))
                })?;

        let proto_object: Option<ProtoObject> = object_dto
            .map(|e| e.try_into())
            .map_or(Ok(None), |r| r.map(Some))?;

        let path_strings = obj_paths
            .iter()
            .map(|p| p.path.clone())
            .collect::<Vec<String>>();

        Ok(ObjectWithUrl {
            object: proto_object,
            url: "".to_string(),
            paths: path_strings,
        })
    }

    ///ToDo: Rust Doc
    ///
    pub fn get_object_revisions(
        &self,
        request: GetObjectRevisionsRequest,
    ) -> Result<Vec<ObjectWithUrl>, ArunaError> {
        let parsed_object_id = diesel_ulid::DieselUlid::from_str(&request.object_id)?;
        let parsed_collection_id = diesel_ulid::DieselUlid::from_str(&request.collection_id)?;

        let all_revs = self
            .pg_connection
            .get()?
            .transaction::<Vec<ObjectWithUrl>, Error, _>(|conn| {
                // This is a safety measure to make sure on revision is referenced in the current collection
                // Otherwise get_object_ignore_coll could be used to break safety measures / permission boundaries
                let all = get_all_revisions(conn, &parsed_object_id)?;
                let all_ids = all.iter().map(|e| e.id).collect::<Vec<_>>();

                let issomewherereferenced = collection_objects
                    .filter(database::schema::collection_objects::object_id.eq_any(&all_ids))
                    .filter(
                        database::schema::collection_objects::collection_id
                            .eq(&parsed_collection_id),
                    )
                    .first::<CollectionObject>(conn)
                    .optional()?;

                // Query and return all revisions
                Ok(if issomewherereferenced.is_some() {
                    let obj_paths = if let Some(first_obj) = all.first() {
                        get_paths_proto(
                            &first_obj.shared_revision_id,
                            Some(&parsed_collection_id),
                            conn,
                        )?
                        .iter()
                        .map(|e| e.path.to_string())
                        .collect::<Vec<String>>()
                    } else {
                        Vec::new()
                    };

                    all.iter()
                        .filter_map(|obj| match get_object_ignore_coll(&obj.id, conn) {
                            Ok(opt) => opt.map(|e| {
                                Ok(ObjectWithUrl {
                                    object: Some(e.try_into()?),
                                    url: "".to_string(),
                                    paths: obj_paths.clone(),
                                })
                            }),
                            Err(e) => Some(Err(e)),
                        })
                        .collect::<Result<Vec<ObjectWithUrl>, _>>()?
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
    ) -> Result<Option<Vec<ObjectWithUrl>>, ArunaError> {
        // Parse the page_request and get pagesize / lastuuid
        let (pagesize, last_uuid) = parse_page_request(request.page_request, 20)?;
        // Parse the query to a `ParsedQuery`
        let parsed_query = parse_query(request.label_id_filter)?;
        // Collection context

        let query_collection_id = diesel_ulid::DieselUlid::from_str(&request.collection_id)?;

        // Execute request
        use crate::database::schema::collection_objects::dsl as colobj;
        use crate::database::schema::object_key_value::dsl as okv;
        use diesel::prelude::*;
        let ret_objects = self
            .pg_connection
            .get()?
            .transaction::<Option<Vec<ObjectWithUrl>>, Error, _>(|conn| {
                // First build a "boxed" base request to which additional parameters can be added later
                let mut base_request = colobj::collection_objects.into_boxed();
                // Filter collection_id
                base_request = base_request.filter(colobj::collection_id.eq(&query_collection_id));

                // Create returnvector of CollectionOverviewsDb
                let mut return_vec: Vec<ObjectWithUrl> = Vec::new();
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
                            let found_objs: Option<Vec<diesel_ulid::DieselUlid>>;
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
                                    .load::<diesel_ulid::DieselUlid>(conn)
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
                            get_object(&s_obj.object_id, None, &query_collection_id, false, conn)?
                        {
                            let proto_paths = get_paths_proto(
                                &obj.object.shared_revision_id,
                                Some(&query_collection_id),
                                conn,
                            )?
                            .iter()
                            .map(|p| p.path.clone())
                            .collect::<Vec<String>>();
                            return_vec.push(ObjectWithUrl {
                                object: Some(obj.try_into()?),
                                url: "".to_string(),
                                paths: proto_paths,
                            });
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
        let object_uuid = diesel_ulid::DieselUlid::from_str(&request.object_id)?;
        let source_collection_uuid = diesel_ulid::DieselUlid::from_str(&request.collection_id)?;
        let target_collection_uuid =
            diesel_ulid::DieselUlid::from_str(&request.target_collection_id)?;

        // Transaction time
        self.pg_connection
            .get()?
            .transaction::<_, ArunaError, _>(|conn| {
                // Return error if target collection already has a version
                if is_collection_versioned(conn, &target_collection_uuid)? {
                    return Err(ArunaError::InvalidRequest(
                        "Adding objects to collection with version is forbidden.".to_string(),
                    ));
                }

                // Get object and collection_object association of provided object uuid
                let original_object = objects
                    .filter(database::schema::objects::id.eq(&object_uuid))
                    .first::<Object>(conn)?;

                let original_reference: Option<CollectionObject> = collection_objects
                    .filter(database::schema::collection_objects::object_id.eq(&object_uuid))
                    .filter(
                        database::schema::collection_objects::collection_id
                            .eq(&source_collection_uuid),
                    )
                    .first::<CollectionObject>(conn)
                    .optional()?;

                let target_reference = if let Some(object_reference) = original_reference {
                    // Check if existing reference is staging object
                    if object_reference.reference_status == ReferenceStatus::STAGING {
                        return Err(ArunaError::InvalidRequest(
                            format!("Cannot create reference of object {object_uuid} while in staging phase.")));
                    }

                    // Auto_update reference can only be created for latest revision to ensure reference consistency
                    //   In the case that existing reference is auto_update == false and is_latest == true -> Overwrite
                    if request.auto_update {
                        if object_reference.is_latest {
                            if !object_reference.auto_update {
                                // Update existing reference to auto_update == true
                                update(collection_objects)
                                    .filter(database::schema::collection_objects::id.eq(&object_reference.id))
                                    .set(database::schema::collection_objects::auto_update.eq(&true))
                                    .execute(conn)?;

                                return Ok(());
                            } // else do nothing
                        } else {
                            return Err(ArunaError::InvalidRequest(
                                "Cannot create auto_update reference for non-latest revision.".to_string(),
                            ));
                        }
                    }

                    CollectionObject {
                        id: diesel_ulid::DieselUlid::generate(),
                        collection_id: target_collection_uuid,
                        object_id: object_uuid,
                        is_latest: object_reference.is_latest,
                        is_specification: object_reference.is_specification,
                        auto_update: request.auto_update,
                        writeable: request.writeable,
                        reference_status: object_reference.reference_status,
                    }
                } else {
                    let all_references = get_all_references(conn, &object_uuid, &true)?;

                    if all_references
                        .iter()
                        .filter(|object_reference| object_reference.writeable)
                        .filter(|object_reference| object_reference.collection_id == source_collection_uuid)
                        .count() < 1
                    {
                        return Err(ArunaError::InvalidRequest(format!("No writeable reference for object {object_uuid} could be found in the source collection.")))
                    }

                    // Check if provided object is latest revision
                    let is_latest_revision = get_latest_obj(conn, object_uuid)?.id == object_uuid;

                    // Can only create auto_update references for latest revision
                    if request.auto_update && !is_latest_revision {
                        return Err(ArunaError::InvalidRequest(
                            "Cannot create auto_update reference for non-latest revision.".to_string(),
                        ));
                    };

                    CollectionObject {
                        id: diesel_ulid::DieselUlid::generate(),
                        collection_id: target_collection_uuid,
                        object_id: object_uuid,
                        is_latest: is_latest_revision,
                        is_specification: false, // Default as this should not be guessed
                        auto_update: request.auto_update,
                        writeable: request.writeable,
                        reference_status: ReferenceStatus::OK,
                    }
                };

                // Insert new object reference
                //   --> Error if specific object revision already has reference in collection
                diesel::insert_into(collection_objects)
                    .values(&target_reference)
                    .get_result::<CollectionObject>(conn)?;

                // Construct path string
                create_relation(&original_object.id, Some(original_object.clone()), &target_collection_uuid, &request.sub_path, conn)?;
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
        object_ulid: &diesel_ulid::DieselUlid,
        with_revisions: bool,
    ) -> Result<GetReferencesResponse, ArunaError> {
        // Transaction time
        let references = self
            .pg_connection
            .get()?
            .transaction::<Vec<ObjectReference>, Error, _>(|conn| {
                let orig_object = objects
                    .filter(database::schema::objects::id.eq(object_ulid))
                    .first::<Object>(conn)?;

                if with_revisions {
                    let all_revisions = objects
                        .filter(
                            database::schema::objects::shared_revision_id
                                .eq(orig_object.shared_revision_id),
                        )
                        .load::<Object>(conn)?;
                    let mapped = all_revisions
                        .iter()
                        .map(|elem| (elem.id, elem.revision_number))
                        .collect::<HashMap<diesel_ulid::DieselUlid, i64>>();

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
                            revision_number: *mapped.get(&elem.object_id).unwrap_or(&0),
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
        object_uuid: &diesel_ulid::DieselUlid,
        collection_uuid: &diesel_ulid::DieselUlid,
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
        creator_uuid: &diesel_ulid::DieselUlid,
    ) -> Result<(ProtoObject, Relation), ArunaError> {
        // Extract (and automagically validate) uuids from request
        let object_uuid = diesel_ulid::DieselUlid::from_str(&request.object_id)?;
        let source_collection_uuid = diesel_ulid::DieselUlid::from_str(&request.collection_id)?;
        let target_collection_uuid =
            diesel_ulid::DieselUlid::from_str(&request.target_collection_id)?;

        // Transaction time
        let (cloned_object, relation) = self
            .pg_connection
            .get()?
            .transaction::<(ProtoObject, Relation), Error, _>(|conn| {
                let (proto_object, _, relation) = clone_object(
                    conn,
                    creator_uuid,
                    object_uuid,
                    source_collection_uuid,
                    target_collection_uuid,
                )?;

                Ok((proto_object, relation))
            })?;

        Ok((cloned_object, relation))
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
        creator_id: diesel_ulid::DieselUlid,
    ) -> Result<Relation, ArunaError> {
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

        let parsed_collection_id = diesel_ulid::DieselUlid::from_str(&request.collection_id)?;
        let parsed_object_id = diesel_ulid::DieselUlid::from_str(&request.object_id)?;

        let relation = self
            .pg_connection
            .get()?
            .transaction::<Relation, ArunaError, _>(|conn| {
                use crate::database::schema::relations::dsl as relations_dsl;

                // Query single relation of object for notification
                let relation = relations
                    .filter(relations_dsl::object_id.eq(&parsed_object_id))
                    .filter(relations_dsl::collection_id.eq(&parsed_collection_id))
                    .first::<Relation>(conn)?;

                // Delete object
                delete_multiple_objects(
                    vec![parsed_object_id],
                    parsed_collection_id,
                    request.force,
                    request.with_revisions,
                    creator_id,
                    conn,
                )?;

                Ok(relation)
            })?;

        Ok(relation)
    }

    pub fn delete_objects(
        &self,
        request: DeleteObjectsRequest,
        creator_id: diesel_ulid::DieselUlid,
    ) -> Result<Vec<Relation>, ArunaError> {
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

        let parsed_collection_id = diesel_ulid::DieselUlid::from_str(&request.collection_id)?;

        // Parse objectids
        let parsed_object_ids = request
            .object_ids
            .iter()
            .map(|objid| diesel_ulid::DieselUlid::from_str(objid))
            .collect::<Result<Vec<_>, _>>()?;

        let obj_relations = self
            .pg_connection
            .get()?
            .transaction::<Vec<Relation>, ArunaError, _>(|conn| {
                use crate::database::schema::relations::dsl as relations_dsl;

                // Query single relation for each object to be deleted (group_by is not viable here)
                let distinct_select = relations
                    .select(Relation::as_select())
                    .filter(relations_dsl::object_id.eq_any(&parsed_object_ids))
                    .filter(relations_dsl::collection_id.eq(&parsed_collection_id))
                    .distinct_on(relations_dsl::object_id)
                    .load::<Relation>(conn)?;

                // Delete objects
                delete_multiple_objects(
                    parsed_object_ids,
                    parsed_collection_id,
                    request.force,
                    request.with_revisions,
                    creator_id,
                    conn,
                )?;

                Ok(distinct_select)
            })?;
        Ok(obj_relations)
    }

    /// ToDo: Rust Doc
    pub fn add_labels_to_object(
        &self,
        request: AddLabelsToObjectRequest,
    ) -> Result<AddLabelsToObjectResponse, ArunaError> {
        let parsed_object_id = diesel_ulid::DieselUlid::from_str(&request.object_id)?;
        let parsed_collection_id = diesel_ulid::DieselUlid::from_str(&request.collection_id)?;

        // Transaction time
        let updated_objects = self
            .pg_connection
            .get()?
            .transaction::<Option<ObjectDto>, ArunaError, _>(|conn| {
                // Check if object reference is writeable
                let reference_opt: Option<CollectionObject> = collection_objects
                    .filter(database::schema::collection_objects::object_id.eq(parsed_object_id))
                    .filter(
                        database::schema::collection_objects::collection_id
                            .eq(parsed_collection_id),
                    )
                    .first::<CollectionObject>(conn)
                    .optional()?;

                if let Some(reference) = reference_opt {
                    if !reference.writeable {
                        return Err(ArunaError::InvalidRequest(
                            "Cannot add labels through read-only reference.".to_string(),
                        ));
                    }
                } else {
                    // Not latest revision
                    return Err(ArunaError::InvalidRequest(
                        "Please add labels to latest revision.".to_string(),
                    ));
                }

                let db_key_values = to_key_values::<ObjectKeyValue>(
                    request.labels_to_add,
                    Vec::new(),
                    parsed_object_id,
                );

                insert_into(object_key_value)
                    .values(&db_key_values)
                    .execute(conn)?;

                get_object(&parsed_object_id, None, &parsed_collection_id, true, conn)
                    .map_err(ArunaError::DieselError)
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
        let parsed_object_id = diesel_ulid::DieselUlid::from_str(&request.object_id)?;
        let parsed_collection_id = diesel_ulid::DieselUlid::from_str(&request.collection_id)?;
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
                        id: diesel_ulid::DieselUlid::generate(),
                        object_id: parsed_object_id,
                        key: elem.key.to_string(),
                        value: elem.value.to_string(),
                        key_value_type: KeyValueType::HOOK,
                    })
                    .collect::<Vec<_>>();

                insert_into(object_key_value)
                    .values(&new_hooks)
                    .execute(conn)?;

                get_object(&parsed_object_id, None, &parsed_collection_id, true, conn)
            })?;

        let mapped = updated_objects
            .map(|e| e.try_into())
            .map_or(Ok(None), |r| r.map(Some))?;

        Ok(SetHooksOfObjectResponse { object: mapped })
    }

    /// ToDo: Rust Doc
    pub fn get_object_path(
        &self,
        request: GetObjectPathRequest,
    ) -> Result<GetObjectPathResponse, ArunaError> {
        // Parse collection and object id
        let obj_id = diesel_ulid::DieselUlid::from_str(&request.object_id)?;
        let col_id = diesel_ulid::DieselUlid::from_str(&request.collection_id)?;

        let db_paths = self
            .pg_connection
            .get()?
            .transaction::<Vec<ProtoPath>, Error, _>(|conn| {
                use crate::database::schema::relations::dsl as relations_dsl;

                // Get all paths depending on include_inactive parameter
                let mut path_query = relations
                    .filter(relations_dsl::collection_id.eq(&col_id))
                    .filter(relations_dsl::object_id.eq(&obj_id))
                    .into_boxed();

                if !request.include_inactive {
                    path_query = path_query.filter(relations_dsl::path_active.eq(&true))
                }

                let obj_relations: Option<Vec<Relation>> = path_query.load::<Relation>(conn).optional()?;

                // Filter paths for active / not active, map to protopath
                match obj_relations {
                    Some(pths) => {
                        Ok(pths.iter().filter_map(|p|
                            // If request indicated include inactive -> use all
                            if request.include_inactive || p.path_active{
                                Some(ProtoPath{ path: relation_as_s3_path(p), visibility: p.path_active })
                            }else{
                                None
                            }
                    ).collect::<Vec<ProtoPath>>())
                    }
                    None => Ok(Vec::new()),
                }
            })?;

        Ok(GetObjectPathResponse {
            object_paths: db_paths,
        })
    }

    /// ToDo: Rust Doc
    pub fn get_object_paths(
        &self,
        request: GetObjectPathsRequest,
    ) -> Result<GetObjectPathsResponse, ArunaError> {
        // Parse collection and object id
        let col_id = diesel_ulid::DieselUlid::from_str(&request.collection_id)?;

        let db_paths = self
            .pg_connection
            .get()?
            .transaction::<Vec<ProtoPath>, Error, _>(|conn| {
                use crate::database::schema::relations::dsl as relations_dsl;

                // Get all paths for collection
                let obj_relations: Option<Vec<Relation>> = relations
                    .filter(relations_dsl::collection_id.eq(col_id))
                    .load::<Relation>(conn)
                    .optional()?;

                // Filter paths fo
                // Filter paths for active / not active, map to protopath
                match obj_relations {
                    Some(pths) => {
                        Ok(pths.iter().filter_map(|p|
                            // If request indicated include inactive -> use all
                            if request.include_inactive || p.path_active{
                                Some(ProtoPath{ path: relation_as_s3_path(p), visibility: p.path_active })
                            }else{
                                None
                            }
                    ).collect::<Vec<ProtoPath>>())
                    }
                    None => Ok(Vec::new()),
                }
            })?;

        Ok(GetObjectPathsResponse {
            object_paths: db_paths,
        })
    }

    /// ToDo: Rust Doc
    pub fn create_object_path(
        &self,
        request: CreateObjectPathRequest,
    ) -> Result<CreateObjectPathResponse, ArunaError> {
        // Parse collection and object id
        let col_id = diesel_ulid::DieselUlid::from_str(&request.collection_id)?;
        let obj_id = diesel_ulid::DieselUlid::from_str(&request.object_id)?;

        let db_path = self
            .pg_connection
            .get()?
            .transaction::<Option<ProtoPath>, ArunaError, _>(|conn| {
                if objects
                    .select(database::schema::objects::object_status)
                    .filter(database::schema::objects::id.eq(&obj_id))
                    .first::<ObjectStatus>(conn)?
                    == ObjectStatus::AVAILABLE
                {
                    // Try create relation containing the new path
                    let relation =
                        create_relation(&obj_id, None, &col_id, &request.sub_path, conn)?;

                    // Return path in s3 format
                    Ok(Some(ProtoPath {
                        path: relation_as_s3_path(&relation),
                        visibility: true,
                    }))
                } else {
                    Err(ArunaError::InvalidRequest(
                        "Cannot create path for object".to_string(),
                    ))
                }
            })?;
        Ok(CreateObjectPathResponse { path: db_path })
    }

    /// ToDo: Rust Doc
    pub fn set_object_path_visibility(
        &self,
        request: SetObjectPathVisibilityRequest,
    ) -> Result<SetObjectPathVisibilityResponse, ArunaError> {
        // Parse collection and object id
        let col_id = diesel_ulid::DieselUlid::from_str(&request.collection_id)?;

        let db_path = self
            .pg_connection
            .get()?
            .transaction::<Option<ProtoPath>, ArunaError, _>(|conn| {
                if !request.path.starts_with("s3://") {
                    return Err(ArunaError::InvalidRequest(
                        "Path does not start with s3://".to_string(),
                    ));
                }

                let (_, s3path) = request.path[5..]
                    .split_once('/')
                    .ok_or(ArunaError::InvalidRequest("Invalid path".to_string()))?;

                let target_id: DieselUlid = relations
                    .filter(database::schema::relations::collection_id.eq(&col_id))
                    .filter(database::schema::relations::path.eq(s3path))
                    .order(database::schema::relations::object_id.desc())
                    .limit(1)
                    .select(database::schema::relations::id)
                    .first::<DieselUlid>(conn)?;

                let res = update(relations)
                    .filter(database::schema::relations::id.eq(&target_id))
                    .set(database::schema::relations::path_active.eq(request.visibility))
                    .get_result::<Relation>(conn)
                    .optional()?;

                Ok(res.map(|p| ProtoPath {
                    path: format!("s3://{}.{}/{}", p.collection_path, p.project_name, p.path),
                    visibility: p.path_active,
                }))
            })?;

        Ok(SetObjectPathVisibilityResponse { path: db_path })
    }

    /// ToDo: Rust Doc
    pub fn get_objects_by_path(
        &self,
        request: GetObjectsByPathRequest,
    ) -> Result<GetObjectsByPathResponse, ArunaError> {
        let proto_objects = self
            .pg_connection
            .get()?
            .transaction::<Vec<ProtoObject>, ArunaError, _>(|conn| {
                if !request.path.starts_with("s3://") {
                    return Err(ArunaError::InvalidRequest(
                        "Path does not start with s3://".to_string(),
                    ));
                }

                let (s3bucket, s3path) = request.path[5..]
                    .split_once('/')
                    .ok_or(ArunaError::InvalidRequest("Invalid path".to_string()))?;

                let (proj_name, col_path) = parse_bucket_path_as_colpath(s3bucket.to_string())?;

                let all_relations: Vec<Relation> = relations
                    .filter(database::schema::relations::path.eq(s3path))
                    .filter(database::schema::relations::project_name.eq(&proj_name))
                    .filter(database::schema::relations::collection_path.eq(&col_path))
                    .load::<Relation>(conn)?;

                if let Some(first_rel) = all_relations.first() {
                    get_objects_by_relations(&all_relations, Some(first_rel.collection_id), conn)?
                        .into_iter()
                        .map(|e| e.try_into())
                        .collect::<Result<Vec<_>, ArunaError>>()
                } else {
                    Ok(Vec::new())
                }
            })?;

        Ok(GetObjectsByPathResponse {
            object: proto_objects,
        })
    }

    /// Fetch the latest revision of an object via its unique path or create a staging object if
    /// the object already exists.
    ///
    /// ## Arguments:
    ///
    /// * `GetOrCreateObjectByPathRequest` -
    ///   The request containing the information needed to fetch or create/update an object.
    ///
    /// ## Returns:
    ///
    /// * `Result<GetOrCreateObjectByPathResponse, Status>` -
    /// The response contains the id of the fetched/created object, the collection id, the objects data class and
    /// if already available the objects hash(es).
    ///
    /// ## Behaviour:
    ///
    /// - If the object exists and no staging object is provided, the found object id will be returned.
    /// - If the object exists and a staging object is provided, an object update will be initiated and the staging object id returned.
    /// - If no object exists and a staging object is provided, an object creation will be initiated and the staging object id returned.
    /// - If no object exists and no staging object is provided, an error will be returned for an invalid request.
    ///
    pub fn get_or_create_object_by_path(
        &self,
        request: GetOrCreateObjectByPathRequest,
    ) -> Result<GetOrCreateObjectByPathResponse, ArunaError> {
        // Parse request and path
        if !request.path.starts_with("s3://") {
            return Err(ArunaError::InvalidRequest(
                "Path does not start with s3://".to_string(),
            ));
        }

        let (s3bucket, _) = request.path[5..]
            .split_once('/')
            .ok_or(ArunaError::InvalidRequest("Invalid path".to_string()))?;

        let access_key = diesel_ulid::DieselUlid::from_str(request.access_key.as_str())
            .map_err(ArunaError::from)?;

        let endpoint_uuid = diesel_ulid::DieselUlid::from_str(request.endpoint_id.as_str())
            .map_err(ArunaError::from)?;

        let response = self
            .pg_connection
            .get()?
            .transaction::<GetOrCreateObjectByPathResponse, ArunaError, _>(|conn| {
                // Fetch collection id to check permissions of request
                let (_, maybe_collection) =
                    get_project_collection_ids_of_bucket_path(conn, s3bucket.to_string())?;

                // Only proceed if collection exists
                let collection_uuid = maybe_collection.ok_or(ArunaError::InvalidRequest(
                    format!("Collection in path {} does not exist.", request.path),
                ))?;

                // Fetch object to check if it exists
                let get_object = get_object_revision_by_path(
                    conn,
                    &request.path,
                    -1,
                    Some(collection_uuid),
                    true,
                )?;

                // Check permissions
                let (creator_uuid, _) = self.get_checked_user_id_from_token(
                    &access_key,
                    &Context {
                        user_right: if get_object.is_some() && request.object.is_none() {
                            UserRights::READ
                        } else {
                            UserRights::APPEND
                        },
                        resource_type: Resources::COLLECTION,
                        resource_id: collection_uuid,
                        admin: false,
                        personal: false,
                        oidc_context: false,
                        allow_service_accounts: true,
                    },
                )?;

                // - If object already exists and no staging object provided -> return
                // - If object already exists and staging object provided    -> update
                // - If object does not exist and staging object provided    -> init
                // - Else error.
                match get_object {
                    Some(fetched_object) => {
                        if let Some(staging_object) = request.object {
                            if fetched_object.object_status == ObjectStatus::INITIALIZING {
                                Ok(GetOrCreateObjectByPathResponse {
                                    object_id: fetched_object.id.to_string(),
                                    collection_id: collection_uuid.to_string(),
                                    dataclass: db_to_grpc_dataclass(&fetched_object.dataclass)
                                        as i32,
                                    hashes: get_object_hashes(conn, &fetched_object.id)?,
                                    revision_number: fetched_object.revision_number,
                                    created: false,
                                })
                            } else if fetched_object.object_status != ObjectStatus::AVAILABLE {
                                Err(ArunaError::InvalidRequest(format!(
                                    "Cannot update object with status {:?}",
                                    fetched_object.object_status
                                )))
                            } else {
                                let staging_object_uuid = diesel_ulid::DieselUlid::generate();
                                let created_object = update_object_init(
                                    conn,
                                    staging_object,
                                    fetched_object.id,
                                    staging_object_uuid,
                                    collection_uuid,
                                    &creator_uuid,
                                    Some(&endpoint_uuid),
                                    true,
                                    false,
                                )?;

                                Ok(GetOrCreateObjectByPathResponse {
                                    object_id: created_object.id.to_string(),
                                    collection_id: collection_uuid.to_string(),
                                    dataclass: db_to_grpc_dataclass(&created_object.dataclass)
                                        as i32,
                                    hashes: get_object_hashes(conn, &fetched_object.id)?,
                                    revision_number: created_object.revision_number,
                                    created: false,
                                })
                            }
                        } else {
                            Ok(GetOrCreateObjectByPathResponse {
                                object_id: fetched_object.id.to_string(),
                                collection_id: collection_uuid.to_string(),
                                dataclass: db_to_grpc_dataclass(&fetched_object.dataclass) as i32,
                                hashes: get_object_hashes(conn, &fetched_object.id)?,
                                revision_number: fetched_object.revision_number,
                                created: false,
                            })
                        }
                    }
                    None => {
                        if let Some(staging_object) = request.object {
                            let staging_object_id = diesel_ulid::DieselUlid::generate();
                            let created_object = create_staging_object(
                                conn,
                                staging_object,
                                &staging_object_id,
                                None,
                                &collection_uuid,
                                &creator_uuid,
                                false,
                                Some(&endpoint_uuid),
                            )?;

                            Ok(GetOrCreateObjectByPathResponse {
                                object_id: created_object.id.to_string(),
                                collection_id: collection_uuid.to_string(),
                                dataclass: db_to_grpc_dataclass(&created_object.dataclass) as i32,
                                hashes: get_object_hashes(conn, &created_object.id)?,
                                revision_number: 0,
                                created: false,
                            })
                        } else {
                            Err(ArunaError::InvalidRequest(
                                "No staging object provided in request for creation.".to_string(),
                            ))
                        }
                    }
                }
            })?;

        Ok(response)
    }

    /// Parses the object path and fetches the project and collection id from the database.
    ///
    /// ## Arguments:
    ///
    /// * `request_path: &String` - The S3 object path
    /// * `bucket_only: bool` - Marker if the provided path is only the bucket part
    ///
    /// ## Returns:
    ///
    /// * `Result<(diesel_ulid::DieselUlid, Option<diesel_ulid::DieselUlid>), ArunaError>` -
    /// The response contains the at least the project id and if present also the collection id.
    /// If the project does not exist an error is returned.
    ///
    pub fn get_project_collection_ids_by_path(
        &self,
        request_path: &str,
        bucket_only: bool,
    ) -> Result<(diesel_ulid::DieselUlid, Option<diesel_ulid::DieselUlid>), ArunaError> {
        let s3bucket = if bucket_only {
            request_path
        } else {
            if !request_path.starts_with("s3://") {
                return Err(ArunaError::InvalidRequest(
                    "Path does not start with s3://".to_string(),
                ));
            }

            let (s3bucket, _) = request_path[5..]
                .split_once('/')
                .ok_or(ArunaError::InvalidRequest("Invalid path".to_string()))?;

            s3bucket
        };

        let result = self.pg_connection.get()?.transaction::<(
            diesel_ulid::DieselUlid,
            Option<diesel_ulid::DieselUlid>,
        ), ArunaError, _>(|conn| {
            get_project_collection_ids_of_bucket_path(conn, s3bucket.to_string())
        })?;

        Ok(result)
    }

    /// Get all relations of a specific object.
    /// Can be optionally filtered by a project and/or collection ulid.
    ///
    /// ## Arguments:
    ///
    /// * `object_ulid` - The object ulid
    /// * `project_filter` - Optional project ulid
    /// * `collection_filter` - Optional collection ulid
    ///
    /// ## Returns:
    ///
    /// * `Result<Vec<Relation>, ArunaError>` -
    /// All queried object relations which can also be empty; Error else.
    ///
    pub fn get_object_relations(
        &self,
        object_ulid: &diesel_ulid::DieselUlid,
        project_filter: Option<&diesel_ulid::DieselUlid>,
        collection_filter: Option<&diesel_ulid::DieselUlid>,
    ) -> Result<Vec<Relation>, ArunaError> {
        use crate::database::schema::relations::dsl as relations_dsl;

        // Create base query
        let mut base_query = relations
            .filter(relations_dsl::object_id.eq(&object_ulid))
            .into_boxed();

        // Filter by project on demand
        if let Some(project_ulid) = project_filter {
            base_query = base_query.filter(relations_dsl::project_id.eq(project_ulid));
        }

        // Filter by collection on demand
        if let Some(collection_ulid) = collection_filter {
            base_query = base_query.filter(relations_dsl::collection_id.eq(collection_ulid));
        }

        // Load and return relations
        let result = self
            .pg_connection
            .get()?
            .transaction::<Vec<Relation>, ArunaError, _>(|conn| {
                Ok(base_query.load::<Relation>(conn)?)
            })?;

        Ok(result)
    }
}

/* ----------------- Section for object specific helper functions ------------------- */
/// Creates a staging object with the provided meta information.
///
/// Warning: This function does not check permissions.
///
/// ## Arguments:
///
/// * `conn: &mut PooledConnection<ConnectionManager<PgConnection>>` - Open database connection
/// * `staging_object: StageObject` - Staging object meta information
/// * `object_uuid: &diesel_ulid::DieselUlid` - Unique object identifier for staging object
/// * `object_hash: Option<ProtoHash>` - Optional initial object hash
/// * `collection_uuid: &diesel_ulid::DieselUlid` - Unique collection identifier
/// * `creator_uuid: &diesel_ulid::DieselUlid` - Unique user identifier
/// * `is_collection_specification: bool` - Mark object as collection specification
///
/// ## Returns:
///
/// * `Result<Object, ArunaError>` - The created staging object
///
#[allow(clippy::too_many_arguments)]
pub fn create_staging_object(
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
    staging_object: StageObject,
    object_uuid: &diesel_ulid::DieselUlid,
    object_hash: Option<ProtoHash>,
    collection_uuid: &diesel_ulid::DieselUlid,
    creator_uuid: &diesel_ulid::DieselUlid,
    is_collection_specification: bool,
    endpoint_uuid: Option<&diesel_ulid::DieselUlid>,
) -> Result<Object, ArunaError> {
    //Define source object from updated request; None if empty
    let source: Option<Source> = match &staging_object.source {
        Some(source) => Some(Source {
            id: diesel_ulid::DieselUlid::generate(),
            link: source.identifier.clone(),
            source_type: SourceType::from_i32(source.source_type)?,
        }),
        _ => None,
    };

    // Define object in database representation
    let object = Object {
        id: *object_uuid,
        shared_revision_id: diesel_ulid::DieselUlid::generate(),
        revision_number: 0,
        filename: staging_object.filename.clone(),
        created_at: chrono::Utc::now().naive_utc(),
        created_by: *creator_uuid,
        content_len: staging_object.content_len,
        object_status: ObjectStatus::INITIALIZING,
        dataclass: grpc_to_db_dataclass(&staging_object.dataclass),
        source_id: source.as_ref().map(|src| src.id),
        origin_id: *object_uuid,
    };

    // Define the join table entry collection <--> object
    let collection_object = CollectionObject {
        id: diesel_ulid::DieselUlid::generate(),
        collection_id: *collection_uuid,
        is_latest: false, // Will be checked on finish
        reference_status: ReferenceStatus::STAGING,
        object_id: object.id,
        auto_update: false, //Note: Finally set with FinishObjectStagingRequest
        is_specification: is_collection_specification,
        writeable: true, //Note: Original object is initially always writeable
    };

    // Define the hash placeholder for the object
    let db_hash = if let Some(proto_hash) = object_hash {
        ApiHash {
            id: diesel_ulid::DieselUlid::generate(),
            hash: proto_hash.hash,
            object_id: object.id,
            hash_type: grpc_to_db_hash_type(&proto_hash.alg)?,
        }
    } else {
        ApiHash {
            id: diesel_ulid::DieselUlid::generate(),
            hash: "".to_string(), //Note: Empty hash will be updated later
            object_id: object.id,
            hash_type: HashType::SHA256, //Note: Default. Will be updated later
        }
    };

    // Convert the object's labels and hooks to their database representation
    let mut key_value_pairs =
        to_key_values::<ObjectKeyValue>(staging_object.labels, staging_object.hooks, *object_uuid);

    // Validate key_values
    if !validate_key_values::<ObjectKeyValue>(key_value_pairs.clone()) {
        return Err(ArunaError::InvalidRequest(
            "labels or hooks are invalid".to_string(),
        ));
    };

    if let Some(sour) = source {
        diesel::insert_into(sources).values(&sour).execute(conn)?;
    }
    diesel::insert_into(objects).values(&object).execute(conn)?;
    diesel::insert_into(hashes).values(&db_hash).execute(conn)?;

    create_relation(
        object_uuid,
        Some(object.clone()),
        collection_uuid,
        &staging_object.sub_path,
        conn,
    )?;

    if let Some(endpoint) = endpoint_uuid {
        key_value_pairs.push(ObjectKeyValue {
            id: diesel_ulid::DieselUlid::generate(),
            object_id: object.id,
            key: "app.aruna-storage.org/endpoint_id".to_string(),
            value: endpoint.to_string(),
            key_value_type: KeyValueType::LABEL,
        });
    }

    diesel::insert_into(object_key_value)
        .values(&key_value_pairs)
        .execute(conn)?;
    diesel::insert_into(collection_objects)
        .values(&collection_object)
        .execute(conn)?;

    Ok(object)
}

/// Creates a staging object with the provided meta information.
///
/// Warning: This function does not check permissions.
///
/// ## Arguments:
///
/// * `conn: &mut PooledConnection<ConnectionManager<PgConnection>>` - Open database connection
/// * `staging_object: StageObject` - Staging object meta information
/// * `object_uuid: &diesel_ulid::DieselUlid` - Unique object identifier for staging object
/// * `collection_uuid: &diesel_ulid::DieselUlid` - Unique collection identifier
/// * `creator_uuid: &diesel_ulid::DieselUlid` - Unique user identifier
///
/// ## Returns:
///
/// * `Result<Object, ArunaError>` - The created staging object
///
#[allow(clippy::too_many_arguments)]
pub fn update_object_init(
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
    staging_object: StageObject,
    current_object_uuid: diesel_ulid::DieselUlid,
    staging_object_uuid: diesel_ulid::DieselUlid,
    collection_uuid: diesel_ulid::DieselUlid,
    creator_uuid: &diesel_ulid::DieselUlid,
    endpoint_uuid: Option<&diesel_ulid::DieselUlid>,
    reupload: bool,
    is_collection_specification: bool,
) -> Result<Object, ArunaError> {
    // Get all references of all revisions of the object
    let all_revision_references = get_all_references(conn, &current_object_uuid, &true)?;

    // Filter references for staging objects and collection specific, writeable references
    let mut staging_references = Vec::new();
    let mut object_references = Vec::new();
    for object_reference in all_revision_references {
        if object_reference.reference_status == ReferenceStatus::STAGING {
            staging_references.push(object_reference);
        } else if object_reference.writeable && object_reference.collection_id == collection_uuid {
            object_references.push(object_reference);
        }
    }

    if staging_references.len() > 1 {
        return Err(ArunaError::InvalidRequest(format!("Object {current_object_uuid} has more than one staging object. This has to be resolved manually.")));
    } else if let Some(staging_reference) = staging_references.first() {
        return if staging_reference.object_id == current_object_uuid {
            update_object_in_place(
                conn,
                &current_object_uuid,
                creator_uuid,
                &collection_uuid,
                endpoint_uuid,
                &staging_object,
            )
        } else {
            Err(ArunaError::InvalidRequest(format!("Object {current_object_uuid} already has a staging object. Concurrent updates are prohibited.")))
        };
    }

    if object_references.is_empty() {
        return Err(ArunaError::InvalidRequest(format!("Object {current_object_uuid} does not have a writeable reference in collection {collection_uuid}")));
    }

    // Get latest revision of the Object to be updated
    let latest = get_latest_obj(conn, current_object_uuid)?;

    // Check if update is performed on latest object revision
    if latest.id != current_object_uuid {
        return Err(ArunaError::InvalidRequest(format!(
            "Updates only allowed on the latest revision: {}",
            latest.id
        )));
    }

    // Define source object from updated request; None if empty
    let source: Option<Source> = match &staging_object.source {
        Some(source) => Some(Source {
            id: diesel_ulid::DieselUlid::generate(),
            link: source.identifier.to_string(),
            source_type: SourceType::from_i32(source.source_type)?,
        }),
        _ => None,
    };

    // Define new Object with updated values
    let new_object = Object {
        id: staging_object_uuid,
        shared_revision_id: latest.shared_revision_id,
        revision_number: latest.revision_number + 1,
        filename: staging_object.filename.to_string(),
        created_at: chrono::Utc::now().naive_utc(),
        created_by: *creator_uuid,
        content_len: staging_object.content_len,
        object_status: ObjectStatus::INITIALIZING, // Is a staging object
        dataclass: grpc_to_db_dataclass(&staging_object.dataclass),
        source_id: source.as_ref().map(|source| source.id),
        origin_id: current_object_uuid,
    };

    // Define new object hashes depending if update contains data re-upload
    let mut new_hashes = Vec::new();
    if reupload {
        // Create new empty hash record which will be updated on object finish/finalize
        for db_hash_type in &[HashType::MD5, HashType::SHA256] {
            new_hashes.push(ApiHash {
                id: diesel_ulid::DieselUlid::generate(),
                hash: "".to_string(),
                object_id: new_object.id,
                hash_type: *db_hash_type,
            })
        }
    } else {
        // Without re-upload just clone hashes of current object
        for mut old_hash in hashes
            .filter(database::schema::hashes::object_id.eq(current_object_uuid))
            .load::<ApiHash>(conn)?
        {
            old_hash.id = diesel_ulid::DieselUlid::generate();
            old_hash.object_id = staging_object_uuid;

            new_hashes.push(old_hash);
        }
    };

    // Define temporary STAGING join table entry collection <-->  staging object
    let collection_object = CollectionObject {
        id: diesel_ulid::DieselUlid::generate(),
        collection_id: collection_uuid,
        is_latest: false, // Will be checked on finish
        reference_status: ReferenceStatus::STAGING,
        object_id: staging_object_uuid,
        auto_update: false, //Note: Finally set with FinishObjectStagingRequest
        is_specification: is_collection_specification,
        writeable: true,
    };

    // Convert the object's labels and hooks to their database representation
    // Clone could be removed if the to_object_key_values method takes borrowed vec instead of moved / owned reference
    let mut key_value_pairs = to_key_values::<ObjectKeyValue>(
        staging_object.labels.clone(),
        staging_object.hooks.clone(),
        staging_object_uuid,
    );

    // Validate key_values
    if !validate_key_values::<ObjectKeyValue>(key_value_pairs.clone()) {
        return Err(ArunaError::InvalidRequest(
            "labels or hooks are invalid".to_string(),
        ));
    };

    let s3path = format!("{}/{}", &staging_object.sub_path, &new_object.filename);

    // Check if relation/path exists for another shared_rev_id
    let exists = relations
        .filter(database::schema::relations::path.eq(&s3path))
        .filter(database::schema::relations::collection_id.eq(&collection_uuid))
        .filter(database::schema::relations::shared_revision_id.ne(&latest.shared_revision_id))
        .first::<Relation>(conn)
        .optional()?;

    // If it already exists -> FAIL
    if exists.is_some() {
        return Err(ArunaError::InvalidRequest(
            "Invalid path, already exists for different object hierarchy".to_string(),
        ));
    }

    if let Some(endpoint) = endpoint_uuid {
        key_value_pairs.push(ObjectKeyValue {
            id: diesel_ulid::DieselUlid::generate(),
            object_id: staging_object_uuid,
            key: "app.aruna-storage.org/endpoint_id".to_string(),
            value: endpoint.to_string(),
            key_value_type: KeyValueType::LABEL,
        });
    }

    // Insert entities which are always created on update
    diesel::insert_into(objects)
        .values(&new_object)
        .execute(conn)?;
    diesel::insert_into(hashes)
        .values(&new_hashes)
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

    // Create relation for staging object
    create_relation(
        &new_object.id,
        Some(new_object.clone()),
        &collection_uuid,
        &staging_object.sub_path,
        conn,
    )?;

    Ok(new_object)
}

/// This functions checks if the specific object has any reference in the provided collection.
///
/// ## Arguments:
///
/// * `conn: &mut PooledConnection<ConnectionManager<PgConnection>>` - Database connection
/// * `object_uuid: &diesel_ulid::DieselUlid` - Unique object identifier
/// * `collection_uuid: &diesel_ulid::DieselUlid` - Unique collection identifier
/// * `with_revisions: bool` - Flag if object revisions shall be included
///
/// ## Returns:
///
/// `Result<bool, ArunaError>` - True if any reference of the object exists in the collection; False else.
///
pub fn object_exists_in_collection(
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
    object_uuid: &diesel_ulid::DieselUlid,
    collection_uuid: &diesel_ulid::DieselUlid,
    with_revisions: bool,
) -> Result<bool, ArunaError> {
    // Get references depending on the with_revisions parameter
    let references: Vec<CollectionObject> = match with_revisions {
        true => {
            /*
            SELECT col_objs.id FROM collection_objects as col_objs
                WHERE col_objs.collection_id = ''
                AND col_objs.object_id IN (
                    SELECT obj1.id FROM objects as obj1
                        WHERE obj1.shared_revision_id = (
                            SELECT obj2.shared_revision_id FROM objects as obj2
                                WHERE obj2.id = ''
                    );
                );
            */

            // Alias are needed to join a table with itself
            // see: https://docs.diesel.rs/master/diesel/macro.alias.html
            let (orig_obj, other_obj) = diesel::alias!(
                database::schema::objects as orig_obj,
                database::schema::objects as other_obj
            );

            orig_obj
                .filter(
                    orig_obj
                        .field(database::schema::objects::id)
                        .eq(object_uuid),
                )
                .inner_join(
                    other_obj.on(orig_obj
                        .field(database::schema::objects::shared_revision_id)
                        .eq(other_obj.field(database::schema::objects::shared_revision_id))),
                )
                .inner_join(
                    collection_objects.on(other_obj
                        .field(database::schema::objects::id)
                        .eq(database::schema::collection_objects::object_id)),
                )
                .filter(database::schema::collection_objects::collection_id.eq(collection_uuid))
                .select(CollectionObject::as_select())
                .load::<CollectionObject>(conn)?

            /*
            let object_ids = objects
                .filter(database::schema::objects::shared_revision_id.nullable().eq(
                    objects::table()
                        .filter(database::schema::objects::id.eq(object_uuid))
                        .select(database::schema::objects::shared_revision_id)
                        .single_value()
                ))
                .select(database::schema::objects::id)
                .load::<diesel_ulid::DieselUlid>(conn)?;

            // Select all references associated with the object ids in the specified collection
            collection_objects
                .filter(database::schema::collection_objects::collection_id.eq(collection_uuid))
                .filter(database::schema::collection_objects::object_id.eq_any(&object_ids))
                .load::<CollectionObject>(conn)?
            */
        }
        false => {
            // If with revisions is false -> return just all collection_objects belonging to the current object
            collection_objects
                .filter(database::schema::collection_objects::object_id.eq(object_uuid))
                .filter(database::schema::collection_objects::collection_id.eq(collection_uuid))
                .load::<CollectionObject>(conn)?
        }
    };

    Ok(!references.is_empty())
}

/// This is a general helper function that can be use inside already open transactions
/// to update an object in-place without creating a new revision. This should only be
/// used for objects which are still/already in STAGING.
///
/// ## Arguments:
///
/// * `conn: &mut PooledConnection<ConnectionManager<PgConnection>>` - Database connection
/// * `object_uuid: &diesel_ulid::DieselUlid` - Unique object identifier
/// * `stage_object: &StageObject` - Stage object from the init/update request
///
/// ## Returns:
///
/// `Result<aruna_server::database::models::object::Object, Error>` -
/// The Object contains the updated database object
///
pub fn update_object_in_place(
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
    object_uuid: &diesel_ulid::DieselUlid,
    user_uuid: &diesel_ulid::DieselUlid,
    collection_uuid: &diesel_ulid::DieselUlid,
    endpoint_uuid: Option<&diesel_ulid::DieselUlid>,
    stage_object: &StageObject,
) -> Result<Object, ArunaError> {
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
                id: diesel_ulid::DieselUlid::generate(),
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
    let mut key_value_pairs = to_key_values::<ObjectKeyValue>(
        stage_object.labels.clone(),
        stage_object.hooks.clone(),
        *object_uuid,
    );

    // Validate key_values
    if !validate_key_values::<ObjectKeyValue>(key_value_pairs.clone()) {
        return Err(ArunaError::InvalidRequest(
            "labels or hooks are invalid".to_string(),
        ));
    };

    let s3path = format!("{}/{}", &stage_object.sub_path, &stage_object.filename);

    // Check if relation/path exists for another shared_rev_id
    let conflicting_relation = relations
        .filter(database::schema::relations::path.eq(&s3path))
        .filter(database::schema::relations::collection_id.eq(&collection_uuid))
        .filter(database::schema::relations::shared_revision_id.ne(&old_object.shared_revision_id))
        .first::<Relation>(conn)
        .optional()?;

    let (projname, colname, colver): (String, String, Option<CollectionVersion>) = collections
        .filter(database::schema::collections::id.eq(&collection_uuid))
        .inner_join(database::schema::projects::dsl::projects)
        .left_join(database::schema::collection_version::dsl::collection_version)
        .select((
            database::schema::projects::name,
            database::schema::collections::name,
            Option::<CollectionVersion>::as_select(),
        ))
        .first::<(String, String, Option<CollectionVersion>)>(conn)?;

    // If it already exists -> FAIL
    if conflicting_relation.is_some() {
        return Err(ArunaError::InvalidRequest(
            "Invalid path, already exists for different object hierarchy".to_string(),
        ));
    }

    // Create collection path part
    let coll_path = format!(
        "{}.{}",
        match colver {
            Some(v) => {
                format!("{}.{}.{}", v.major, v.minor, v.patch)
            }
            None => "latest".to_string(),
        },
        colname,
    );

    // If it already exists
    if conflicting_relation.is_some() {
        // Check if the existing is not associated with the current shared_revision_id -> Error
        // else -> do nothing
        return Err(ArunaError::InvalidRequest(
            "Invalid path, already exists for different object hierarchy".to_string(),
        ));
    }

    if let Some(endpoint) = endpoint_uuid {
        key_value_pairs.push(ObjectKeyValue {
            id: diesel_ulid::DieselUlid::generate(),
            object_id: *object_uuid,
            key: "app.aruna-storage.org/endpoint_id".to_string(),
            value: endpoint.to_string(),
            key_value_type: KeyValueType::LABEL,
        });
    }

    delete(object_key_value)
        .filter(database::schema::object_key_value::object_id.eq(&object_uuid))
        .execute(conn)?;
    insert_into(object_key_value)
        .values(&key_value_pairs)
        .execute(conn)?;

    // Fetch singleton relation of staging object and modify it for update
    let mut staging_object_relation: Relation = relations
        .filter(database::schema::relations::object_id.eq(&object_uuid))
        .filter(database::schema::relations::collection_id.eq(&collection_uuid))
        .first::<Relation>(conn)?; // There has to be a relation as it is created on staging object init

    staging_object_relation.project_name = projname;
    staging_object_relation.collection_id = *collection_uuid;
    staging_object_relation.collection_path = coll_path;

    update(relations)
        .filter(database::schema::relations::id.eq(&staging_object_relation.id))
        .set(&staging_object_relation)
        .execute(conn)?;

    // Update remaining object meta in-place
    old_object.filename = stage_object.filename.to_string();
    old_object.content_len = stage_object.content_len;
    old_object.created_at = chrono::Utc::now().naive_utc();
    old_object.created_by = *user_uuid;
    old_object.dataclass = grpc_to_db_dataclass(&stage_object.dataclass);

    // Update the object record in the database
    let updated_obj = update(objects)
        .filter(database::schema::objects::id.eq(&object_uuid))
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
/// * `object_uuid: diesel_ulid::DieselUlid` - Unique object identifier
/// * `source_collection_uuid: diesel_ulid::DieselUlid` - Unique source collection identifier
/// * `target_collection_uuid: diesel_ulid::DieselUlid` - Unique target collection identifier
///
/// ## Resturns:
///
/// `Result<use aruna_rust_api::api::storage::models::Object, Error>` -
/// The Object contains the newly created object clone in its gRPC proto format
///
pub fn clone_object(
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
    creator_uuid: &diesel_ulid::DieselUlid,
    object_uuid: diesel_ulid::DieselUlid,
    source_collection_uuid: diesel_ulid::DieselUlid,
    target_collection_uuid: diesel_ulid::DieselUlid,
) -> Result<(ProtoObject, diesel_ulid::DieselUlid, Relation), ArunaError> {
    use crate::database::schema::collection_version::dsl as versions_dsl;
    use crate::database::schema::collections::dsl as collections_dsl;

    // Fetch target collection infos
    let (target_proj_id, target_proj_name, target_col_name, target_col_ver): (
        diesel_ulid::DieselUlid,
        String,
        String,
        Option<CollectionVersion>,
    ) = collections
        .filter(collections_dsl::id.eq(&target_collection_uuid))
        .inner_join(database::schema::projects::dsl::projects)
        .left_join(versions_dsl::collection_version)
        .select((
            database::schema::projects::id,
            database::schema::projects::name,
            database::schema::collections::name,
            Option::<CollectionVersion>::as_select(),
        ))
        .first::<(
            diesel_ulid::DieselUlid,
            String,
            String,
            Option<CollectionVersion>,
        )>(conn)?;

    // Get original object, collection_object reference, key_values, hash and source
    let mut db_object: Object = objects
        .filter(database::schema::objects::id.eq(&object_uuid))
        .first::<Object>(conn)?;

    // Get original collection_object reference
    let mut db_collection_object: CollectionObject = CollectionObject::belonging_to(&db_object)
        .filter(database::schema::collection_objects::collection_id.eq(&source_collection_uuid))
        .first::<CollectionObject>(conn)?;

    // Get key_values
    let mut db_object_key_values: Vec<ObjectKeyValue> =
        ObjectKeyValue::belonging_to(&db_object).load::<ObjectKeyValue>(conn)?;

    // Get object hashes
    let mut db_hashes: Vec<ApiHash> = ApiHash::belonging_to(&db_object).load::<ApiHash>(conn)?;

    // Get object source
    let db_source: Option<Source> = match &db_object.source_id {
        None => None,
        Some(src_id) => Some(
            sources
                .filter(database::schema::sources::id.eq(src_id))
                .first::<Source>(conn)?,
        ),
    };

    // Get object locations
    let mut db_locations = object_locations
        .filter(database::schema::object_locations::object_id.eq(&object_uuid))
        .load::<ObjectLocation>(conn)?;

    // Get source collection relations of object
    let mut object_relations: Vec<Relation> = relations
        .filter(database::schema::relations::object_id.eq(&object_uuid))
        .filter(database::schema::relations::collection_id.eq(&source_collection_uuid))
        .load::<Relation>(conn)?;

    // Modify object
    db_object.id = diesel_ulid::DieselUlid::generate();
    db_object.shared_revision_id = diesel_ulid::DieselUlid::generate();
    db_object.revision_number = 0;
    db_object.origin_id = object_uuid;
    db_object.created_by = *creator_uuid;
    db_object.created_at = chrono::Utc::now().naive_utc();

    // Modify hashes
    for db_hash in &mut db_hashes {
        db_hash.id = diesel_ulid::DieselUlid::generate();
        db_hash.object_id = db_object.id;
    }

    // Modify collection_object reference
    db_collection_object.id = diesel_ulid::DieselUlid::generate();
    db_collection_object.collection_id = target_collection_uuid;
    db_collection_object.object_id = db_object.id;

    // Modify object_key_values
    for kv in &mut db_object_key_values {
        kv.id = diesel_ulid::DieselUlid::generate();
        kv.object_id = db_object.id;
    }

    // Modify object locations
    for location in &mut db_locations {
        location.id = diesel_ulid::DieselUlid::generate();
        location.object_id = db_object.id;
    }

    // Modify relations to target collection
    for relation in &mut object_relations {
        relation.id = diesel_ulid::DieselUlid::generate();
        relation.object_id = db_object.id;
        relation.project_id = target_proj_id;
        relation.project_name = target_proj_name.clone();
        relation.collection_id = target_collection_uuid;
        relation.collection_path = format!(
            "{}.{}",
            if let Some(version) = &target_col_ver {
                format!("{}.{}.{}", version.major, version.minor, version.patch)
            } else {
                "latest".to_string()
            },
            target_col_name,
        );
        relation.shared_revision_id = db_object.shared_revision_id;
    }

    // Insert cloned object, hash, key_Values and references
    insert_into(objects).values(&db_object).execute(conn)?;
    // Insert cloned locations
    insert_into(object_locations)
        .values(&db_locations)
        .execute(conn)?;
    // Insert cloned hash
    insert_into(hashes).values(&db_hashes).execute(conn)?;
    // Insert cloned key_Values
    insert_into(object_key_value)
        .values(&db_object_key_values)
        .execute(conn)?;
    // Insert reference for cloned object
    insert_into(collection_objects)
        .values(&db_collection_object)
        .execute(conn)?;
    // Insert modified relations
    insert_into(relations)
        .values(&object_relations)
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
    Ok((
        ProtoObject {
            id: db_object.id.to_string(),
            filename: db_object.filename,
            labels,
            hooks,
            created: Some(timestamp),
            content_len: db_object.content_len,
            status: db_object.object_status as i32,
            origin: Some(ProtoOrigin {
                id: db_object.origin_id.to_string(),
            }),
            data_class: db_object.dataclass as i32,
            rev_number: db_object.revision_number,
            source: proto_source,
            latest: db_collection_object.is_latest,
            auto_update: db_collection_object.auto_update,
            hashes: db_hashes
                .iter()
                .map(|db_hash| ProtoHash {
                    alg: db_to_grpc_hash_type(&db_hash.hash_type),
                    hash: db_hash.hash.to_string(),
                })
                .collect::<Vec<_>>(),
        },
        db_object.shared_revision_id,
        if let Some(relation) = object_relations.first() {
            relation.clone()
        } else {
            return Err(ArunaError::InvalidRequest(
                "Object did not have any relations ...".to_string(),
            ));
        },
    ))
}

/// This is a helper method that queries the "latest" object based on the current object_uuid.
/// If returned object.id == ref_object_id -> the current object is "latest"
///
/// ## Arguments:
///
/// * `conn: &mut PooledConnection<ConnectionManager<PgConnection>>` - Database connection
/// * `ref_object_id`: `diesel_ulid::DieselUlid` - The Uuid for which the latest Object revision should be found
///
/// ## Returns:
///
/// `Result<use aruna_rust_api::api::storage::models::Object, ArunaError>` -
/// The latest database object or error if the request failed.
///
pub fn get_latest_obj(
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
    ref_object_id: diesel_ulid::DieselUlid,
) -> Result<Object, ArunaError> {
    let shared_id = objects
        .filter(database::schema::objects::id.eq(ref_object_id))
        .select(database::schema::objects::shared_revision_id)
        .first::<diesel_ulid::DieselUlid>(conn)?;

    let latest_object = objects
        .filter(
            database::schema::objects::shared_revision_id
                .eq(&shared_id)
                .and(database::schema::objects::object_status.ne(&ObjectStatus::DELETED))
                .and(database::schema::objects::object_status.ne(&ObjectStatus::TRASH)),
        )
        .order_by(database::schema::objects::revision_number.desc())
        .first::<Object>(conn)?;

    Ok(latest_object)
}

/// This is a helper method that queries the "latest" object revision based on the path of the object.
/// If returned object.id == ref_object_id -> the current object is "latest"
///
/// ## Arguments:
///
/// * `conn: &mut PooledConnection<ConnectionManager<PgConnection>>` - Database connection
/// * `object_path`: `&String` - The fully-qualified S3 object path
/// * `check_collection: Option<diesel_ulid::DieselUlid>` - Validates the collection id if provided
///
/// ## Returns:
///
/// `Result<Object, ArunaError>` - The latest database object revision or error if the request failed.
pub fn get_object_revision_by_path(
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
    object_path: &str,
    _object_revision: i64,
    check_collection: Option<diesel_ulid::DieselUlid>,
    include_staging: bool,
) -> Result<Option<Object>, ArunaError> {
    if !object_path.starts_with("s3://") {
        return Err(ArunaError::InvalidRequest(
            "Path does not start with s3://".to_string(),
        ));
    }

    // Split of "s3://"
    let (s3bucket, s3path) = object_path[5..]
        .split_once('/')
        .ok_or(ArunaError::InvalidRequest("Invalid path".to_string()))?;

    let (projname, colpath) = parse_bucket_path_as_colpath(s3bucket.to_string())?;

    let get_latest_relation: Option<Relation> = relations
        .filter(database::schema::relations::path.eq(s3path))
        .filter(database::schema::relations::project_name.eq(&projname))
        .filter(database::schema::relations::collection_path.eq(&colpath))
        .order_by(database::schema::relations::object_id)
        .first::<Relation>(conn)
        .optional()?;

    // Validate that provided collection id and path collection id matches
    if let Some(check_col_id) = check_collection {
        if let Some(ref latest_rest) = get_latest_relation {
            if check_col_id != latest_rest.collection_id {
                return Err(ArunaError::InvalidRequest(format!(
                    "Path is not part of collection: {check_col_id}"
                )));
            }
        }
    }

    match get_latest_relation {
        Some(ref p) => {
            // Query the existing path
            let mut base_request = objects
                .filter(database::schema::objects::id.eq(p.object_id))
                .into_boxed();

            if !include_staging {
                base_request = base_request.filter(
                    database::schema::objects::object_status
                        .eq(crate::database::models::enums::ObjectStatus::AVAILABLE),
                );
            }

            Ok(base_request.first::<Object>(conn).optional()?)
        }
        None => {
            // Note: This should just be the case if the path does not exist and has nothing to do anymore with staging
            // Err(ArunaError::InvalidRequest("Object not found".to_string()))
            // Note2: This can happen on data proxy GetOrCreateObjectByPath requests...
            Ok(None)
        }
    }
}

/// This is a helper method that queries the project id and collection id based
/// on the names and version in the provided bucket part of an object path.
///
/// ## Arguments:
///
/// * `conn: &mut PooledConnection<ConnectionManager<PgConnection>>` - Database connection
/// * `bucket_path`: `String` - Bucket part of an S3 object path, e.g. `latest.project-name.collection-name`
///
/// ## Returns:
///
/// `Result<(diesel_ulid::DieselUlid, Option<diesel_ulid::DieselUlid>), ArunaError>` -
/// The Ok Result contains the project id and the collection id if the collection exists in the project.
/// If no project exists with the provided name an Error will be returned.
///
pub fn get_project_collection_ids_of_bucket_path(
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
    bucket_path: String,
) -> Result<(diesel_ulid::DieselUlid, Option<diesel_ulid::DieselUlid>), ArunaError> {
    use crate::database::schema::collection_version::dsl as version_dsl;
    use crate::database::schema::collections::dsl as collection_dsl;
    use crate::database::schema::projects::dsl as project_dsl;

    // Parse bucket string
    let (proj_name, collection_name, path_version) = parse_bucket_path(bucket_path)?;

    // Fetch project by its unique name
    let project_uuid = projects
        .filter(project_dsl::name.eq(&proj_name))
        .select(project_dsl::id)
        .first::<diesel_ulid::DieselUlid>(conn)?;

    // Fetch version id if version is not None
    let version_uuid_option: Option<diesel_ulid::DieselUlid> =
        if let Some(coll_version) = path_version {
            collection_version
                .filter(version_dsl::major.eq(coll_version.major as i64))
                .filter(version_dsl::minor.eq(coll_version.minor as i64))
                .filter(version_dsl::patch.eq(coll_version.patch as i64))
                .select(version_dsl::id)
                .first::<diesel_ulid::DieselUlid>(conn)
                .optional()?
        } else {
            None
        };

    // Fetch collection by its unique combination of project_id, name and version
    let mut base_request = collections
        .filter(collection_dsl::project_id.eq(&project_uuid))
        .filter(collection_dsl::name.eq(&collection_name))
        .into_boxed();

    // Cannot directly filter for nullable value with Option
    if let Some(version_uuid) = version_uuid_option {
        base_request = base_request.filter(collection_dsl::version_id.eq(version_uuid));
    } else {
        base_request = base_request.filter(collection_dsl::version_id.is_null());
    }

    let collection_uuid_option: Option<diesel_ulid::DieselUlid> = base_request
        .select(collection_dsl::id)
        .first::<diesel_ulid::DieselUlid>(conn)
        .optional()?;

    Ok((project_uuid, collection_uuid_option))
}

///
///
pub fn delete_staging_object(
    object_uuid: &diesel_ulid::DieselUlid,
    collection_uuid: &diesel_ulid::DieselUlid,
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
) -> Result<(), diesel::result::Error> {
    // Process so that the staging object can be correctly cleared by the cron job:
    //   - Get object
    //   - Delete source, hash and key-value pairs
    //   - Delete reference
    //   - Update object status to TRASH

    // Get staging database object
    let staging_object: Object = objects
        .filter(database::schema::objects::id.eq(object_uuid))
        .first::<Object>(conn)?;

    // Delete source if exists
    if let Some(source_uuid) = staging_object.source_id {
        delete(sources)
            .filter(database::schema::sources::id.eq(&source_uuid))
            .execute(conn)?;
    }

    // Delete all existing hashes of staging object
    delete(hashes)
        .filter(database::schema::hashes::object_id.eq(&object_uuid))
        .execute(conn)?;

    // Delete all key-value pairs of staging object
    let key_values: Vec<ObjectKeyValue> =
        ObjectKeyValue::belonging_to(&staging_object).load::<ObjectKeyValue>(conn)?;
    let key_value_ids = key_values
        .into_iter()
        .map(|key_value| key_value.id)
        .collect::<Vec<_>>();
    delete(object_key_value)
        .filter(database::schema::object_key_value::id.eq_any(&key_value_ids))
        .execute(conn)?;

    // Delete object references
    delete(collection_objects)
        .filter(database::schema::collection_objects::object_id.eq(object_uuid))
        .filter(database::schema::collection_objects::collection_id.eq(collection_uuid))
        .execute(conn)?;

    // Delete staging object relations
    delete(relations)
        .filter(database::schema::relations::object_id.eq(object_uuid))
        .execute(conn)?;

    // Get lowest object revision number
    let lowest_object_revision: Option<i64> = objects
        .select(min(database::schema::objects::revision_number))
        .filter(
            database::schema::objects::shared_revision_id.eq(&staging_object.shared_revision_id),
        )
        .first::<Option<i64>>(conn)?;

    // Update object status to TRASH
    match lowest_object_revision {
        None => Err(Error::NotFound),
        Some(lowest) => {
            update(objects)
                .filter(database::schema::objects::id.eq(object_uuid))
                .set((
                    database::schema::objects::object_status.eq(ObjectStatus::TRASH),
                    database::schema::objects::revision_number.eq(lowest - 1),
                ))
                .execute(conn)?;

            Ok(())
        }
    }
}

/// New deletion function with safety net to assure that object has a writeable reference at every time.
#[deprecated(since = "1.0.0", note = "please use `delete_multiple_objects` instead")]
pub fn _safe_delete_object(
    object_uuid: &diesel_ulid::DieselUlid,
    collection_uuid: &diesel_ulid::DieselUlid,
    with_force: bool,
    with_revisions: bool,
    creator_id: diesel_ulid::DieselUlid,
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
) -> Result<(), ArunaError> {
    // Check if collection is versioned and return ArunaError if true and force == false.
    if is_collection_versioned(conn, collection_uuid)? {
        /*
        if with_force {
            // ToDo: If with_force == true
            //         --> Delete object and update collection with increased version?
        } else {
            return Err(ArunaError::InvalidRequest(
                "Cannot delete objects from versioned collection without force.".to_string(),
            ));
        }
        */

        return Err(ArunaError::InvalidRequest(
            "Deletion of objects from versioned collection is prohibited.".to_string(),
        ));
    }

    // Deletion with revisions
    if with_revisions {
        return if with_force {
            // Check if writeable auto_update == true reference is available in provided collection
            let has_auto_update_reference = get_all_references(conn, object_uuid, &true)?
                .iter()
                .filter(|reference| reference.collection_id == *collection_uuid)
                .filter(|reference| reference.writeable)
                .filter(|reference| reference.auto_update)
                .count()
                > 0;

            if !has_auto_update_reference {
                return Err(ArunaError::InvalidRequest(
                    "Cannot delete all revisions without writeable auto_update reference."
                        .to_string(),
                ));
            }

            // Get all undeleted revisions and sort them descending by their object id
            let mut undeleted_revisions = get_all_revisions(conn, object_uuid)?;
            undeleted_revisions.sort_by(|a, b| b.revision_number.cmp(&a.revision_number)); // Sort descending by revision number

            let undeleted_revision_ids = undeleted_revisions
                .into_iter()
                .filter(|revision| revision.object_status != ObjectStatus::TRASH)
                .filter(|revision| revision.object_status != ObjectStatus::DELETED)
                .map(|revision| revision.id)
                .collect::<Vec<_>>();

            //for revision_id in undeleted_revision_ids {
            delete_multiple_objects(
                undeleted_revision_ids,
                *collection_uuid,
                true,  // Already validated
                false, // Delete revisions individually top-down
                creator_id,
                conn,
            )?;
            //}

            Ok(())
        } else {
            Err(ArunaError::InvalidRequest(
                "Permanent deletion of object with all revisions can only be executed with force."
                    .to_string(),
            ))
        };
    }

    // Check if object has reference in provided collection
    //   if not --> outdated revision or wrong collection id
    return if let Some(object_reference) = collection_objects
        .filter(database::schema::collection_objects::object_id.eq(object_uuid))
        .filter(database::schema::collection_objects::collection_id.eq(collection_uuid))
        .first::<CollectionObject>(conn)
        .optional()?
    {
        // Staging objects can be deleted without further notice, elevation or permissions
        if object_reference.reference_status == ReferenceStatus::STAGING {
            delete_staging_object(object_uuid, collection_uuid, conn)?;

            return Ok(()); // Done.
        }

        // Check if the object to delete is just a read-only reference
        if !object_reference.writeable {
            // Just delete the read-only reference
            delete(collection_objects)
                .filter(database::schema::collection_objects::id.eq(&object_reference.id))
                .execute(conn)?;

            return Ok(()); // Done.
        }

        // Check if reference can be safely deleted without creating a dangling object (None or only read-only references over all revisions left)
        let all_object_references = get_all_references(conn, object_uuid, &true)?;
        let writeable_object_references_amount = all_object_references
            .iter()
            .filter(|reference| reference.writeable)
            .count();

        // Check number of writeable references of Object
        if writeable_object_references_amount > 1 {
            // If object has at least one another writeable reference in any revision or collection:
            //   - Delete read-only references of specific object
            //   - Delete ObjectGroup references of specific object and bump ObjectGroup revisions
            //   - Delete object reference (as another writeable reference exists somewhere)
            let read_only_object_reference_ids = all_object_references
                .iter()
                .filter(|reference| !reference.writeable)
                .filter(|reference| reference.object_id == *object_uuid)
                .map(|reference| reference.id)
                .collect::<Vec<_>>();

            delete(collection_objects)
                .filter(
                    database::schema::collection_objects::id
                        .eq_any(&read_only_object_reference_ids),
                )
                .execute(conn)?;

            delete_object_and_bump_objectgroups(
                &vec![*object_uuid],
                collection_uuid,
                &creator_id,
                conn,
            )?;

            Ok(()) // Done.
        } else {
            // Check if object is the last undeleted revision
            let mut undeleted_revisions = get_all_revisions(conn, object_uuid)?
                .into_iter()
                .filter(|revision| revision.id != *object_uuid)
                .filter(|revision| revision.object_status != ObjectStatus::TRASH)
                .filter(|revision| revision.object_status != ObjectStatus::DELETED)
                .collect::<Vec<_>>();
            undeleted_revisions.sort_by(|a, b| b.revision_number.cmp(&a.revision_number)); // Sort descending by revision number

            // If there are undeleted revisions left:
            if !undeleted_revisions.is_empty() {
                // Handle deletion different for objects with auto_update == true
                if object_reference.auto_update {
                    // If reference is auto_update == true:
                    //   - Delete read-only references of specific object
                    //   - Delete ObjectGroup references of specific object and bump ObjectGroup revisions
                    //   - Update object reference to next lower revision and set object status to TRASH

                    // Delete read-only references of specific object
                    let read_only_object_reference_ids = all_object_references
                        .into_iter()
                        .filter(|reference| !reference.writeable)
                        .filter(|reference| reference.object_id == *object_uuid)
                        .map(|reference| reference.id)
                        .collect::<Vec<_>>();

                    delete(collection_objects)
                        .filter(
                            database::schema::collection_objects::id
                                .eq_any(&read_only_object_reference_ids),
                        )
                        .execute(conn)?;

                    // Delete ObjectGroup references of specific object and bump ObjectGroup revisions
                    delete_objectgroup_references_and_increase_revisions(
                        &vec![*object_uuid],
                        collection_uuid,
                        &creator_id,
                        conn,
                    )?;

                    // Update object reference to next lower available revision and set object status to TRASH
                    let next_lower_revision = undeleted_revisions
                        .first()
                        .ok_or(ArunaError::DieselError(Error::RollbackTransaction))?;

                    update(collection_objects)
                        .filter(database::schema::collection_objects::id.eq(&object_reference.id))
                        .set(
                            database::schema::collection_objects::object_id
                                .eq(&next_lower_revision.id),
                        )
                        .execute(conn)?;

                    // Update object_status to "TRASH"
                    update(objects)
                        .filter(database::schema::objects::id.eq(&object_uuid))
                        .set(database::schema::objects::object_status.eq(ObjectStatus::TRASH))
                        .execute(conn)?;

                    Ok(())
                } else {
                    //ToDo: IS there a better way to handle deletion of objects with only a writeable static reference on another revision?
                    Err(ArunaError::InvalidRequest("Cannot delete last static writeable reference of object with undeleted revisions.".to_string()))
                }
            } else {
                // Check if with_force == true to permanently delete object (No references left and all revisions status == TRASH/DELETED)
                if with_force {
                    // Process:
                    //   - Delete read-only references of specific object
                    //   - Delete ObjectGroup references of specific object and bump ObjectGroup revisions
                    //   - Delete object reference and set object status to TRASH

                    let read_only_object_reference_ids = all_object_references
                        .into_iter()
                        .filter(|reference| !reference.writeable)
                        .filter(|reference| reference.object_id == *object_uuid)
                        .map(|reference| reference.id)
                        .collect::<Vec<_>>();

                    delete(collection_objects)
                        .filter(
                            database::schema::collection_objects::id
                                .eq_any(&read_only_object_reference_ids),
                        )
                        .execute(conn)?;

                    delete_objectgroup_references_and_increase_revisions(
                        &vec![*object_uuid],
                        collection_uuid,
                        &creator_id,
                        conn,
                    )?;

                    // Delete reference as the object is "permanently" deleted
                    delete(collection_objects)
                        .filter(database::schema::collection_objects::id.eq(&object_reference.id))
                        .execute(conn)?;

                    // Update object_status to "TRASH"
                    update(objects)
                        .filter(database::schema::objects::id.eq(&object_uuid))
                        .set(database::schema::objects::object_status.eq(ObjectStatus::TRASH))
                        .execute(conn)?;

                    Ok(()) // Done.
                } else {
                    Err(ArunaError::InvalidRequest(
                        "Use force to permanently delete last existing revision of the object."
                            .to_string(),
                    ))
                }
            }
        }
    } else {
        // Get all writeable references of object and check for collection ids
        let all_object_references = get_all_references(conn, object_uuid, &true)?;
        let writeable_references = all_object_references
            .clone()
            .into_iter()
            .filter(|reference| reference.writeable)
            .collect::<Vec<_>>();

        //Note: For each writeable reference it can be assumed that it is the original.
        //        Does this imply that update/deletion of outdated revisions is also possible from collections
        //        which "only" have a writeable static reference to an object revision?

        // Object with no writeable references at all should never exist
        if writeable_references.is_empty() {
            Err(ArunaError::InvalidRequest(
                "Dangling object detected.".to_string(),
            ))
        } else {
            let writeable_collection_references = writeable_references
                .into_iter()
                .filter(|reference| reference.collection_id == *collection_uuid)
                .collect::<Vec<_>>();

            // Take a look if a writeable reference exists for another revision of the object
            if !writeable_collection_references.is_empty() {
                if writeable_collection_references
                    .iter()
                    .filter(|reference| reference.auto_update)
                    .count()
                    > 0
                {
                    // If writeable reference in collection is auto_update == true:
                    //   - Delete read-only references of specific object
                    //   - Delete ObjectGroup references of specific object and bump ObjectGroup revisions
                    //   - Set ObjectStatus to TRASH
                    let read_only_object_reference_ids = all_object_references
                        .iter()
                        .filter(|reference| !reference.writeable)
                        .filter(|reference| reference.object_id == *object_uuid)
                        .map(|reference| reference.id)
                        .collect::<Vec<_>>();

                    delete(collection_objects)
                        .filter(
                            database::schema::collection_objects::id
                                .eq_any(&read_only_object_reference_ids),
                        )
                        .execute(conn)?;

                    delete_objectgroup_references_and_increase_revisions(
                        &vec![*object_uuid],
                        collection_uuid,
                        &creator_id,
                        conn,
                    )?;

                    // Update object_status to "TRASH"
                    update(objects)
                        .filter(database::schema::objects::id.eq(&object_uuid))
                        .set(database::schema::objects::object_status.eq(ObjectStatus::TRASH))
                        .execute(conn)?;

                    Ok(())
                } else {
                    // If writeable reference in collection is static:
                    //   Assume that we can also manipulate/delete other revisions of the object:
                    //     - Delete read-only references of specific object
                    //     - Delete ObjectGroup references of specific object and bump ObjectGroup revisions
                    //     - Set ObjectStatus to TRASH
                    //   Assume that other revisions are may not be in the manipulation scope
                    //     - Error that you need a writeable "auto_update == true" reference in the collection to delete outdated revisions

                    Err(ArunaError::InvalidRequest(
                        "Cannot delete other revisions without writeable auto_update reference in provided collection."
                            .to_string(),
                    ))
                }
            } else {
                Err(ArunaError::InvalidRequest(
                    "No writeable reference of object in provided collection found.".to_string(),
                ))
            }
        }
    };
}

///
/// This is a helper method that deletes all specified objects (by id)
/// it has two options (with_force) == true => allow for sideeffects in other collections
/// otherwise this method will error
///
///
/// ## Arguments:
///
/// * `conn: &mut PooledConnection<ConnectionManager<PgConnection>>` - Database connection
/// * `object_ids`: `Vec<diesel_ulid::DieselUlid>` - The Uuids of all objects that should be deleted
/// * `coll_id`: diesel_ulid::DieselUlid - The collection where these ids should be deleted from
/// * `with_force`: bool - Are sideeffects allowed ?
///
/// ## Returns:
///
/// `Result<(), ArunaError>` - Will return empty Result or Error
///
pub fn delete_multiple_objects(
    original_object_ids: Vec<diesel_ulid::DieselUlid>,
    coll_id: diesel_ulid::DieselUlid,
    with_force: bool,
    with_revisions: bool,
    creator_id: diesel_ulid::DieselUlid,
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
) -> Result<(), ArunaError> {
    if is_collection_versioned(conn, &coll_id)? {
        return Err(ArunaError::InvalidRequest(
            "Deletion of objects from versioned collection is prohibited.".to_string(),
        ));
    }

    // Query ALL revisions as DB objects
    let object_revisions = {
        let shared_ids = objects
            .filter(database::schema::objects::id.eq_any(&original_object_ids))
            .select(database::schema::objects::shared_revision_id)
            .load::<diesel_ulid::DieselUlid>(conn)?;
        objects
            .filter(database::schema::objects::shared_revision_id.eq_any(&shared_ids))
            .load::<Object>(conn)?
    };

    // Get all object_ids for object_revisions
    // And extract the list of original_database_objects
    let mut original_database_objects = Vec::new();
    let mut object_id_shared_rev_id: HashMap<diesel_ulid::DieselUlid, diesel_ulid::DieselUlid> =
        HashMap::new();

    let object_revision_ids = object_revisions
        .iter()
        .map(|obj_rev| {
            object_id_shared_rev_id.insert(obj_rev.id, obj_rev.shared_revision_id);
            if original_object_ids.contains(&obj_rev.id) {
                original_database_objects.push(obj_rev.clone())
            };
            obj_rev.id
        })
        .collect::<Vec<diesel_ulid::DieselUlid>>();

    // This will query all references and Error if no reference exists
    let references = collection_objects
        .filter(database::schema::collection_objects::object_id.eq_any(&object_revision_ids))
        .load::<CollectionObject>(conn)?;

    // Check if object has writeable reference or not and is referenced at all

    let mut references_to_delete = Vec::new();
    let mut objects_to_trash = Vec::new();

    let mut writeable_objects = Vec::new();
    let mut staging_objects = Vec::new();

    'outer: for original_db_obj in original_database_objects {
        for reference in references.clone() {
            if original_db_obj.id == reference.object_id {
                if reference.writeable {
                    if reference.reference_status == ReferenceStatus::STAGING {
                        staging_objects.push(original_db_obj);
                    } else {
                        writeable_objects.push(original_db_obj);
                    }
                } else {
                    // Read only references can be deleted directly
                    references_to_delete.push(reference);
                }
                continue 'outer;
            }
        }
        return Err(ArunaError::InvalidRequest(format!(
            "Object: {} is not referenced in collection: {}",
            original_db_obj.id, coll_id
        )));
    }

    // Read-only:
    // - force / with_revisions: does not matter
    // Writeable:
    // force / with_revisions
    // 1. + / + :
    // Get all revisions, get all references -> Delete both
    // 2. + / - :
    // Delete all reference to this object set to trash
    // (Check if last reference?)
    // 3. - / + :
    // Delete all references (collection_specific) to these revisions
    // (Check if last reference?)
    // 4. - / - :
    // Delete only the one reference (collection_specific)
    // (Check if last reference?)

    if with_revisions {
        if with_force {
            // Case 1:
            // If with_revision + with_force
            // Delete ALL references to this history
            // Trash all objects in this history
            for writeable_object in writeable_objects.clone() {
                for current_object in object_revisions.clone() {
                    if writeable_object.shared_revision_id == current_object.shared_revision_id {
                        for reference in references.clone() {
                            if reference.object_id == current_object.id {
                                references_to_delete.push(reference.clone())
                            };
                        }
                        objects_to_trash.push(current_object)
                    }
                }
            }
        } else {
            for writeable_object in writeable_objects.clone() {
                let mut potential_trash = Vec::new();
                let mut other_reference_found = false;

                for current_object in object_revisions.clone() {
                    if writeable_object.shared_revision_id == current_object.shared_revision_id {
                        for reference in references.clone() {
                            if reference.object_id == current_object.id {
                                if reference.collection_id == coll_id {
                                    references_to_delete.push(reference.clone())
                                } else {
                                    other_reference_found = true;
                                    potential_trash = Vec::new();
                                }
                            }
                        }

                        if !other_reference_found {
                            potential_trash.push(current_object)
                        }
                    }
                }
                objects_to_trash.append(&mut potential_trash)
            }
        }
    } else if with_force {
        for writeable_object in writeable_objects.clone() {
            let mut potential_trash = Vec::new();
            let mut other_reference_found = false;

            for current_object in object_revisions.clone() {
                if writeable_object.shared_revision_id == current_object.shared_revision_id {
                    // Mark ALL references to this object_id for deletion
                    for reference in references.clone() {
                        if current_object.id == writeable_object.id {
                            references_to_delete.push(reference.clone())
                        } else {
                            other_reference_found = true;
                            potential_trash = Vec::new();
                        }
                    }
                    if !other_reference_found {
                        potential_trash.push(current_object)
                    }
                }
            }
            objects_to_trash.push(writeable_object);
            objects_to_trash.append(&mut potential_trash)
        }
    } else {
        for writeable_object in writeable_objects.clone() {
            let mut potential_trash = Vec::new();
            let mut other_reference_found = false;

            for current_object in object_revisions.clone() {
                if writeable_object.shared_revision_id == current_object.shared_revision_id {
                    // Mark ALL references to this object_id for deletion
                    for reference in references.clone() {
                        if current_object.id == writeable_object.id {
                            if reference.collection_id == coll_id {
                                references_to_delete.push(reference.clone())
                            } else {
                                other_reference_found = true;
                                potential_trash = Vec::new();
                            }
                        }
                    }
                    if !other_reference_found {
                        potential_trash.push(current_object)
                    }
                }
            }
            if !other_reference_found {
                objects_to_trash.push(writeable_object);
            }
            objects_to_trash.append(&mut potential_trash)
        }
    }

    // DELETE staging objects
    for staging_object in staging_objects {
        delete_staging_object(&staging_object.id, &coll_id, conn)?;
    }

    // Extract object_ids from references, ordered by collection_id
    let mut references_per_collection: HashMap<
        diesel_ulid::DieselUlid,
        Vec<diesel_ulid::DieselUlid>,
    > = HashMap::new();

    for ref_to_delete in references_to_delete.clone() {
        if let Some(references_per_coll) =
            references_per_collection.get_mut(&ref_to_delete.collection_id)
        {
            references_per_coll.push(ref_to_delete.object_id)
        } else {
            references_per_collection
                .insert(ref_to_delete.collection_id, vec![ref_to_delete.object_id]);
        }
    }

    // Delete references and bump object_groups from collections
    for (target_collection, objects_per_collection) in references_per_collection {
        delete_object_and_bump_objectgroups(
            &objects_per_collection,
            &target_collection,
            &creator_id,
            conn,
        )?;
    }

    let object_ids_to_trash = objects_to_trash
        .iter()
        .map(|obj| obj.id)
        .collect::<Vec<_>>();

    // Update Object and redact information
    update(objects)
        .filter(database::schema::objects::id.eq_any(&object_ids_to_trash))
        .set((
            database::schema::objects::object_status.eq(ObjectStatus::TRASH),
            database::schema::objects::filename.eq("DELETED"),
            database::schema::objects::content_len.eq(0),
        ))
        .execute(conn)?;

    // Remove all labels
    delete(object_key_value)
        .filter(database::schema::object_key_value::object_id.eq_any(&object_ids_to_trash))
        .execute(conn)?;

    // Add internal labels to indicate when and who has deleted this object
    let update_labels = {
        let mut key_values = Vec::new();
        for obj_to_trash in object_ids_to_trash {
            key_values.push(ObjectKeyValue {
                id: diesel_ulid::DieselUlid::generate(),
                object_id: obj_to_trash,
                key: "app.aruna-storage.org/deleted_at".to_string(),
                value: chrono::Utc::now().naive_utc().to_string(),
                key_value_type: KeyValueType::LABEL,
            });
            key_values.push(ObjectKeyValue {
                id: diesel_ulid::DieselUlid::generate(),
                object_id: obj_to_trash,
                key: "app.aruna-storage.org/deleted_by".to_string(),
                value: creator_id.to_string(),
                key_value_type: KeyValueType::LABEL,
            });
        }
        key_values
    };

    insert_into(object_key_value)
        .values(&update_labels)
        .execute(conn)?;

    let mut shared_revisions_per_collection: HashMap<
        diesel_ulid::DieselUlid,
        HashMap<diesel_ulid::DieselUlid, i32>,
    > = HashMap::new();
    let mut shared_revision_per_collection_to_delete: HashMap<
        diesel_ulid::DieselUlid,
        HashMap<diesel_ulid::DieselUlid, i32>,
    > = HashMap::new();

    let mut relevant_collections = references
        .iter()
        .map(|c| c.collection_id)
        .collect::<Vec<diesel_ulid::DieselUlid>>();
    relevant_collections.sort();
    relevant_collections.dedup();

    for rel_col in relevant_collections {
        for reference in references.clone() {
            if reference.collection_id == rel_col {
                let sh_rev_id = object_id_shared_rev_id
                    .get(&reference.object_id)
                    .ok_or_else(|| {
                        ArunaError::InvalidRequest(format!(
                            "Shared revision can not be found for object_id {} ",
                            reference.object_id
                        ))
                    })?; // Should never occur
                if let Some(sr_id) = shared_revisions_per_collection.get_mut(&rel_col) {
                    if let Some(sr_count) = sr_id.get_mut(sh_rev_id) {
                        *sr_count += 1;
                    } else {
                        sr_id.insert(*sh_rev_id, 1);
                    }
                } else {
                    shared_revisions_per_collection
                        .insert(rel_col, HashMap::from([(*sh_rev_id, 1)]));
                };
            }
        }

        for reference in references_to_delete.clone() {
            if reference.collection_id == rel_col {
                let sh_rev_id = object_id_shared_rev_id
                    .get(&reference.object_id)
                    .ok_or_else(|| {
                        ArunaError::InvalidRequest("Shared revision does not exist".to_string())
                    })?; // Should never occur
                if let Some(sr_id) = shared_revision_per_collection_to_delete.get_mut(&rel_col) {
                    if let Some(sr_count) = sr_id.get_mut(sh_rev_id) {
                        *sr_count += 1;
                    } else {
                        sr_id.insert(*sh_rev_id, 1);
                    }
                } else {
                    shared_revision_per_collection_to_delete
                        .insert(rel_col, HashMap::from([(*sh_rev_id, 1)]));
                };
            }
        }
    }

    for (col_id, shared_counts_deleted) in shared_revision_per_collection_to_delete {
        let mut shared_revisions_to_delete = Vec::new();
        let shared_counts_before =
            shared_revisions_per_collection
                .get(&col_id)
                .ok_or_else(|| {
                    ArunaError::InvalidRequest("Shared revision does not exist".to_string())
                })?;

        for (sh_rev_id, counts_deleted) in shared_counts_deleted {
            if let Some(counts_before) = shared_counts_before.get(&sh_rev_id) {
                if *counts_before - counts_deleted == 0 {
                    shared_revisions_to_delete.push(sh_rev_id);
                }
            }
        }

        delete(relations)
            .filter(database::schema::relations::collection_id.eq(&col_id))
            .filter(
                database::schema::relations::shared_revision_id.eq_any(&shared_revisions_to_delete),
            )
            .execute(conn)?;
    }

    // object_id_shared_rev_id

    Ok(())
}

/// Query all revisions associated to a specific object.
///
/// ## Arguments:
///
/// * `conn: &mut PooledConnection<ConnectionManager<PgConnection>>` - Database connection
/// * `ref_object_id`: `diesel_ulid::DieselUlid` - the Uuid for the object the latest revisions should be determined for
///
/// ## Resturns:
///
/// `Result<Vec<Object>, ArunaError>` -
/// List of all database objects that are revivisons of the original object
///
pub fn get_all_revisions(
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
    ref_object_id: &diesel_ulid::DieselUlid,
) -> Result<Vec<Object>, diesel::result::Error> {
    let shared_id = objects
        .filter(database::schema::objects::id.eq(ref_object_id))
        .select(database::schema::objects::shared_revision_id)
        .first::<diesel_ulid::DieselUlid>(conn)?;

    let all_revision_objects = objects
        .filter(database::schema::objects::shared_revision_id.eq(shared_id))
        .order_by(database::schema::objects::revision_number.asc())
        .load::<Object>(conn)?;

    Ok(all_revision_objects)
}

/// Query all references for a specific object optionally with revisions
///
/// ## Arguments:
///
/// * `conn: &mut PooledConnection<ConnectionManager<PgConnection>>` - Database connection
/// * `ref_object_id`: `diesel_ulid::DieselUlid` - the Uuid for the object the latest revisions should be determined for
/// * `with_revisions`: `bool` - If true all references for all revisions of the provided object will be returned
///
/// ## Resturns:
///
/// `Result<Vec<CollectionObjects>, ArunaError>` -
/// List of all collectionobjects that reference the object with or without all associated revisions
///
pub fn get_all_references(
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
    ref_object_id: &diesel_ulid::DieselUlid,
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
/// * `object_uuid`: `&diesel_ulid::DieselUlid` - The Uuid of the requested object
/// * `collection_uuid` `&diesel_ulid::DieselUlid` - The Uuid of the requesting collection
/// * `include_staging`: bool - Should non finished objects be included
///
/// ## Resturns:
///
/// `Result<use aruna_rust_api::api::storage::models::Object, ArunaError>` -
/// Database representation of an object
///
pub fn get_object(
    object_uuid: &diesel_ulid::DieselUlid,
    queried_object: Option<Object>,
    collection_uuid: &diesel_ulid::DieselUlid,
    include_staging: bool,
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
) -> Result<Option<ObjectDto>, diesel::result::Error> {
    let object: Object = match queried_object {
        Some(o) => o,
        None => objects
            .filter(database::schema::objects::id.eq(&object_uuid))
            .first::<Object>(conn)?,
    };

    let object_key_values = ObjectKeyValue::belonging_to(&object).load::<ObjectKeyValue>(conn)?;
    let (labels, hooks) = from_key_values(object_key_values);

    let object_hashes = hashes
        .filter(database::schema::hashes::object_id.eq(object_uuid))
        .load::<Db_Hash>(conn)?;

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
                object_hashes,
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
/// * `object_uuid`: `&diesel_ulid::DieselUlid` - The Uuid of the requested object
///
/// ## Returns:
///
/// `Result<use aruna_rust_api::api::storage::models::Object, ArunaError>` -
/// Database representation of an object
///
pub fn get_object_ignore_coll(
    object_uuid: &diesel_ulid::DieselUlid,
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
) -> Result<Option<ObjectDto>, diesel::result::Error> {
    let object: Object = objects
        .filter(database::schema::objects::id.eq(&object_uuid))
        .first::<Object>(conn)?;

    let object_key_values = ObjectKeyValue::belonging_to(&object).load::<ObjectKeyValue>(conn)?;
    let (labels, hooks) = from_key_values(object_key_values);

    let object_hash: Vec<ApiHash> = ApiHash::belonging_to(&object).load::<ApiHash>(conn)?;

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
        object_hashes: object_hash,
        source,
        latest,
        update: false, // Always false might not include any collection_info
    }))
}

/// This will query all objects as DTO based on a list of relations
/// Expects the list to include ALL associated relations per collection
pub fn get_objects_by_relations(
    all_relations: &[Relation],
    single_collection: Option<DieselUlid>,
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
) -> Result<Vec<ObjectDto>, diesel::result::Error> {
    let object_uuids = all_relations
        .iter()
        .map(|rel| rel.object_id)
        .unique()
        .collect::<Vec<_>>();

    let objs: Vec<Object> = objects
        .filter(database::schema::objects::id.eq_any(&object_uuids))
        .load::<Object>(conn)?;

    let object_key_values: Vec<Vec<ObjectKeyValue>> = ObjectKeyValue::belonging_to(&objs)
        .load::<ObjectKeyValue>(conn)?
        .grouped_by(&objs);
    let object_hashes: Vec<Vec<ApiHash>> = ApiHash::belonging_to(&objs)
        .load::<ApiHash>(conn)?
        .grouped_by(&objs);

    #[allow(clippy::type_complexity)]
    let zipped: Vec<((Object, Vec<ObjectKeyValue>), Vec<Db_Hash>)> = objs
        .into_iter()
        .zip(object_key_values)
        .zip(object_hashes)
        .collect::<Vec<_>>();

    let mut results = Vec::new();

    for ((obj, kvs), hsh) in zipped {
        let source: Option<Source> = match &obj.source_id {
            None => None,
            Some(src_id) => Some(
                sources
                    .filter(database::schema::sources::id.eq(src_id))
                    .first::<Source>(conn)?,
            ),
        };

        let (labels, hooks) = from_key_values(kvs);

        let (update, latest) = match single_collection {
            Some(cid) => collection_objects
                .filter(database::schema::collection_objects::object_id.eq(&obj.id))
                .filter(database::schema::collection_objects::collection_id.eq(&cid))
                .select((
                    database::schema::collection_objects::auto_update,
                    database::schema::collection_objects::is_latest,
                ))
                .first::<(bool, bool)>(conn)
                .optional()
                .unwrap_or_default()
                .unwrap_or_default(),
            None => (false, false),
        };

        results.push(ObjectDto {
            object: obj,
            labels,
            hooks,
            object_hashes: hsh,
            source,
            latest,
            update,
        })
    }

    results.sort_by(|a, b| b.object.revision_number.cmp(&a.object.revision_number));

    Ok(results)
}

fn delete_object_and_bump_objectgroups(
    deletable_objects_uuids: &Vec<diesel_ulid::DieselUlid>,
    target_collection: &diesel_ulid::DieselUlid,
    creator_id: &diesel_ulid::DieselUlid,
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
) -> Result<(), diesel::result::Error> {
    // Remove object_group_object reference and update object_group
    delete_objectgroup_references_and_increase_revisions(
        deletable_objects_uuids,
        target_collection,
        creator_id,
        conn,
    )?;

    // Remove collection_object reference for specific collection
    delete(collection_objects)
        .filter(database::schema::collection_objects::object_id.eq_any(deletable_objects_uuids))
        .filter(database::schema::collection_objects::collection_id.eq(target_collection))
        .execute(conn)?;
    Ok(())
}

fn delete_objectgroup_references_and_increase_revisions(
    deletable_objects_uuids: &Vec<diesel_ulid::DieselUlid>,
    target_collection: &diesel_ulid::DieselUlid,
    creator_id: &diesel_ulid::DieselUlid,
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

    // Only proceed if at least one object_group_objects reference exists
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

    Ok(())
}

pub fn check_if_obj_in_coll(
    object_ids: &Vec<diesel_ulid::DieselUlid>,
    collection_uuid: &diesel_ulid::DieselUlid,
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

pub fn construct_s3_path(rel: Relation) -> String {
    format!(
        "s3://{}.{}/{}",
        &rel.collection_path, &rel.project_name, &rel.path
    )
}

pub fn get_object_by_path(
    s3path: &str,
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
) -> Result<Option<Relation>, ArunaError> {
    use crate::database::schema::relations::dsl as relations_dsl;

    // s3://latest.my-collection-blup.project-name/my/super/path.txt

    if let Some(stripped) = s3path.strip_prefix("s3://") {
        let (proj_name, coll_name, version) = utils::parse_bucket_path(stripped.to_string())?;

        let col_path = match version {
            Some(v) => format!("{}.{}.{}.{}", v.major, v.minor, v.patch, coll_name),
            None => format!("latest.{}", coll_name),
        };

        let rels: Option<Vec<Relation>> = relations
            .filter(relations_dsl::collection_path.eq(col_path))
            .filter(relations_dsl::project_name.eq(proj_name))
            .load::<Relation>(conn)
            .optional()?;

        match rels {
            Some(mut r) => {
                r.sort_by(|a, b| a.id.cmp(&b.id));
                Ok(r.pop())
            }
            None => Ok(None),
        }
    } else {
        Err(ArunaError::InvalidRequest("Invalid s3path".to_string()))
    }
}

fn _disect_full_object_path(full_path: &str) -> (String, String) {
    let mut all_parts = full_path.split('/').collect::<Vec<_>>();
    let fname = all_parts.pop().unwrap_or_default().to_string();

    let subpath = if all_parts.is_empty() {
        "".to_string()
    } else {
        all_parts.join("/")
    };
    (subpath, fname)
}

/// Helper function that queries the necessary information and inserts a new relation.
/// Ideally this should be executet in a separate transaction.
///
/// Will fail if the subpath is already associated with a different object hierarchy
pub fn create_relation(
    objid: &DieselUlid,
    option_obj: Option<Object>,
    collid: &DieselUlid,
    subpath: &str,
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
) -> Result<Relation, ArunaError> {
    let get_object: Object = match option_obj {
        Some(o) => o,
        None => objects
            .filter(crate::database::schema::objects::id.eq(objid))
            .first::<Object>(conn)?,
    };
    // Join Collection x Projects x CollectionVersion to query all necessary infos at once !
    let (proj_name, project_ulid, collection_name, colver): (
        String,
        DieselUlid,
        String,
        Option<CollectionVersion>,
    ) = collections
        .filter(database::schema::collections::id.eq(&collid))
        .inner_join(database::schema::projects::dsl::projects)
        .left_join(database::schema::collection_version::dsl::collection_version)
        .select((
            database::schema::projects::name,
            database::schema::projects::id,
            database::schema::collections::name,
            Option::<CollectionVersion>::as_select(),
        ))
        .first::<(String, DieselUlid, String, Option<CollectionVersion>)>(conn)?;

    let version_string = match colver {
        Some(v) => {
            format!("{}.{}.{}", v.major, v.minor, v.patch)
        }
        None => "latest".to_string(),
    };

    let object_path = if subpath.is_empty() {
        get_object.filename
    } else {
        if subpath.chars().all(|c| c == '/') {
            return Err(ArunaError::InvalidRequest(
                "Invalid path/name, violates s3 object key naming scheme".to_string(),
            ));
        }
        let stripped_prefix = match subpath.strip_prefix('/') {
            Some(stripped) => stripped,
            None => subpath,
        };
        let stripped_suffix = match stripped_prefix.strip_suffix('/') {
            Some(stripped) => stripped,
            None => stripped_prefix,
        };
        format!("{}/{}", stripped_suffix, get_object.filename)
    };

    if !PATH_SCHEMA.is_match(&object_path) {
        return Err(ArunaError::InvalidRequest(
            "Invalid path/name, violates s3 object key naming scheme".to_string(),
        ));
    }

    let rel = Relation {
        id: DieselUlid::generate(),
        object_id: get_object.id,
        path: object_path.to_string(),
        project_id: project_ulid,
        project_name: proj_name,
        collection_id: *collid,
        collection_path: format!("{}.{}", version_string, collection_name),
        shared_revision_id: get_object.shared_revision_id,
        path_active: true,
    };

    // Check if a similar relation exists
    match relations
        .filter(crate::database::schema::relations::path.eq(&object_path))
        .filter(crate::database::schema::relations::collection_id.eq(&collid))
        .filter(
            crate::database::schema::relations::shared_revision_id
                .ne(get_object.shared_revision_id),
        )
        .first::<Relation>(conn)
        .optional()?
    {
        Some(_) => Err(ArunaError::InvalidRequest(
            "Unable to create path relation for another object hierarchy".to_string(),
        )),
        None => {
            match insert_into(relations)
                .values(&rel)
                .on_conflict_do_nothing()
                .get_result::<Relation>(conn)
                .optional()?
            {
                Some(rel) => Ok(rel),
                None => Ok(relations
                    .filter(crate::database::schema::relations::object_id.eq(&rel.object_id))
                    .filter(
                        crate::database::schema::relations::collection_id.eq(&rel.collection_id),
                    )
                    .filter(crate::database::schema::relations::project_id.eq(&rel.project_id))
                    .first::<Relation>(conn)?),
            }
        }
    }
}

// pub fn create_path_db(
//     s3bucket: &str,
//     s3path: &str,
//     shared_rev_id: &diesel_ulid::DieselUlid,
//     collection_uuid: &diesel_ulid::DieselUlid,
//     conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
// ) -> Result<(), ArunaError> {
//     let path_obj = Path {
//         id: diesel_ulid::DieselUlid::generate(),
//         bucket: s3bucket.into(),
//         path: s3path.into(),
//         shared_revision_id: *shared_rev_id,
//         collection_id: *collection_uuid,
//         created_at: chrono::Utc::now().naive_utc(),
//         active: true,
//     };

//     // Can be removed because of unique constraint on s3bucket/s3path
//     // if (paths
//     //     .filter(database::schema::paths::bucket.eq(s3bucket))
//     //     .filter(database::schema::paths::path.eq(s3path))
//     //     .first::<Path>(conn)
//     //     .optional()?)
//     // .is_none()
//     // {
//     diesel::insert_into(paths)
//         .values(path_obj)
//         .on_conflict_do_nothing()
//         .execute(conn)?;
//     // };

//     Ok(())
// }

pub fn get_paths_proto(
    shared_rev_id: &diesel_ulid::DieselUlid,
    maybe_collection_uuid: Option<&diesel_ulid::DieselUlid>,
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
) -> Result<Vec<ProtoPath>, ArunaError> {
    use crate::database::schema::relations::dsl as rel_dsl;

    // Construct query to fetch paths from
    let mut path_query = relations
        .filter(rel_dsl::shared_revision_id.eq(shared_rev_id))
        .filter(rel_dsl::path_active.eq(&true))
        .order_by(rel_dsl::object_id)
        .into_boxed();

    if let Some(collection_uuid) = maybe_collection_uuid {
        path_query = path_query.filter(rel_dsl::collection_id.eq(collection_uuid))
    }

    let active_paths = path_query.load::<Relation>(conn)?;

    Ok(active_paths
        .iter()
        .map(|pth| ProtoPath {
            path: relation_as_s3_path(pth),
            visibility: pth.path_active,
        })
        .collect::<Vec<_>>())
}

pub fn get_object_hashes(
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
    object_uuid: &diesel_ulid::DieselUlid,
) -> Result<Vec<ProtoHash>, ArunaError> {
    Ok(hashes
        .filter(database::schema::hashes::object_id.eq(&object_uuid))
        .load::<Db_Hash>(conn)?
        .into_iter()
        .map(|e| ProtoHash {
            alg: db_to_grpc_hash_type(&e.hash_type),
            hash: e.hash.to_string(),
        })
        .collect::<Vec<_>>())
}

fn set_object_available(
    conn: &mut PooledConnection<ConnectionManager<PgConnection>>,
    object: &Object,
    coll_uuid: &diesel_ulid::DieselUlid,
    measured_length: Option<i64>,
) -> Result<(), ArunaError> {
    let content_length = match measured_length {
        Some(l) => l,
        None => object.content_len,
    };

    // Update object status to AVAILABLE
    let updated_obj = update(objects)
        .filter(database::schema::objects::id.eq(object.id))
        .set((
            database::schema::objects::object_status.eq(ObjectStatus::AVAILABLE),
            database::schema::objects::content_len.eq(content_length),
        ))
        .get_result::<Object>(conn)?;

    // Delete endpoint label as multipart finish is already running/done at this point
    delete(object_key_value)
        .filter(database::schema::object_key_value::object_id.eq(object.id))
        .filter(database::schema::object_key_value::key.eq("app.aruna-storage.org/endpoint_id"))
        .execute(conn)?;

    let orig_id = updated_obj.origin_id;
    // origin_id != object_id => Update
    if orig_id != object.id {
        // Get all revisions of the object it could be that an older version still has "auto_update" set
        let all_revisions = get_all_revisions(conn, &object.id)?;

        // Filter out the UUIDs
        let all_rev_ids = all_revisions.iter().map(|full| full.id).collect::<Vec<_>>();

        // Get all CollectionObjects that contain any of the all_rev_ids and are auto_update == true
        // Set all auto_updates and is_latest to be false
        let auto_updating_coll_obj: Vec<CollectionObject> = collection_objects
            .filter(database::schema::collection_objects::object_id.eq_any(&all_rev_ids))
            .filter(database::schema::collection_objects::auto_update.eq(true))
            .load::<CollectionObject>(conn)
            .optional()?
            .unwrap_or_default();

        // Filter collection_object references to object's collection
        let auto_update_collection_references = auto_updating_coll_obj
            .iter()
            .filter(|elem| elem.collection_id == *coll_uuid)
            .collect::<Vec<_>>();

        let (auto_update_collection_reference, auto_update_collection_reference_id): (
            Option<CollectionObject>,
            diesel_ulid::DieselUlid,
        ) = match auto_update_collection_references.len() {
            0 => (None, diesel_ulid::DieselUlid::default()),
            1 => (
                Some(auto_update_collection_references[0].clone()),
                auto_update_collection_references[0].clone().id,
            ),
            _ => {
                return Err(ArunaError::InvalidRequest(
                    "More than one revision with auto_update == true".to_string(),
                ));
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
            let affected_object_groups: Option<Vec<diesel_ulid::DieselUlid>> = object_group_objects
                .filter(
                    database::schema::object_group_objects::object_id.eq_any(&auto_updating_obj_id),
                )
                .select(database::schema::object_group_objects::object_group_id)
                .load::<diesel_ulid::DieselUlid>(conn)
                .optional()?;

            match affected_object_groups {
                None => {}
                Some(obj_grp_ids) => {
                    // Bump all revisions for object_groups
                    let new_ogroups =
                        bump_revisisions(&obj_grp_ids, &updated_obj.created_by, conn)?;
                    let new_group_ids =
                        new_ogroups.iter().map(|group| group.id).collect::<Vec<_>>();

                    // Update object_group references
                    update(object_group_objects)
                        .filter(
                            database::schema::object_group_objects::object_group_id
                                .eq_any(&new_group_ids),
                        )
                        .filter(database::schema::object_group_objects::object_id.eq(orig_id))
                        .set(database::schema::object_group_objects::object_id.eq(&object.id))
                        .execute(conn)?;
                }
            }

            // Update Collection_Objects to use the new object_id
            update(collection_objects.filter(
                database::schema::collection_objects::id.eq_any(&auto_updating_coll_obj_id),
            ))
            .set((database::schema::collection_objects::object_id.eq(&object.id),))
            .execute(conn)?;
        }

        // Query the affected object_groups
        let affected_object_groups: Option<Vec<diesel_ulid::DieselUlid>> = object_group_objects
            .filter(database::schema::object_group_objects::object_id.eq(&updated_obj.origin_id))
            .select(database::schema::object_group_objects::object_group_id)
            .load::<diesel_ulid::DieselUlid>(conn)
            .optional()?;

        match affected_object_groups {
            None => {}
            Some(obj_grp_ids) => {
                // Bump all revisions for object_groups
                let new_ogroups = bump_revisisions(&obj_grp_ids, &updated_obj.created_by, conn)?;
                let new_group_ids = new_ogroups.iter().map(|group| group.id).collect::<Vec<_>>();

                // Update object_group references
                update(object_group_objects)
                    .filter(
                        database::schema::object_group_objects::object_group_id
                            .eq_any(&new_group_ids),
                    )
                    .filter(database::schema::object_group_objects::object_id.eq(orig_id))
                    .set(database::schema::object_group_objects::object_id.eq(&object.id))
                    .execute(conn)?;
            }
        }

        // Update inside collection of update object
        match auto_update_collection_reference {
            None => {
                // Update latest staging object reference
                update(collection_objects)
                    .filter(database::schema::collection_objects::object_id.eq(&object.id))
                    .filter(database::schema::collection_objects::collection_id.eq(&coll_uuid))
                    .filter(
                        database::schema::collection_objects::reference_status
                            .eq(&ReferenceStatus::STAGING),
                    )
                    .set((
                        database::schema::collection_objects::object_id.eq(&object.id),
                        database::schema::collection_objects::is_latest.eq(true),
                        database::schema::collection_objects::reference_status
                            .eq(ReferenceStatus::OK),
                        database::schema::collection_objects::auto_update.eq(true),
                    ))
                    .execute(conn)?;
            }
            Some(reference) => {
                // Object is still latest revision on finish --> Normal case
                // Delete staging reference
                delete(collection_objects)
                    .filter(database::schema::collection_objects::object_id.eq(object.id))
                    .filter(database::schema::collection_objects::collection_id.eq(coll_uuid))
                    .filter(
                        database::schema::collection_objects::reference_status
                            .eq(&ReferenceStatus::STAGING),
                    )
                    .execute(conn)?;

                // Update latest staging object reference
                update(collection_objects)
                    .filter(database::schema::collection_objects::id.eq(&reference.id))
                    .filter(database::schema::collection_objects::collection_id.eq(coll_uuid))
                    .set((
                        database::schema::collection_objects::object_id.eq(object.id),
                        database::schema::collection_objects::is_latest.eq(true),
                        database::schema::collection_objects::reference_status
                            .eq(ReferenceStatus::OK),
                        database::schema::collection_objects::auto_update.eq(true),
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
        diesel::update(collection_objects)
            .filter(database::schema::collection_objects::object_id.eq(object.id))
            .set((
                database::schema::collection_objects::is_latest.eq(true),
                database::schema::collection_objects::reference_status.eq(ReferenceStatus::OK),
                database::schema::collection_objects::auto_update.eq(true),
            ))
            .execute(conn)?;
    }

    Ok(())
}
