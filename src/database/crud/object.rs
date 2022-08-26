use chrono::Local;
use diesel::dsl::max;
use diesel::prelude::*;
use diesel::r2d2::ConnectionManager;
use diesel::result::Error;
use r2d2::PooledConnection;

use crate::api::aruna::api::storage::services::v1::{
    CreateObjectReferenceRequest, CreateObjectReferenceResponse, GetObjectsResponse,
};
use crate::error::{ArunaError, GrpcNotFoundError};

use crate::api::aruna::api::storage::{
    internal::v1::{Location as ProtoLocation, LocationType},
    models::v1::{
        Hash as ProtoHash, KeyValue, Object as ProtoObject, Origin as ProtoOrigin,
        Source as ProtoSource,
    },
    services::v1::{
        CloneObjectRequest,
        CloneObjectResponse,
        DeleteObjectRequest,
        DeleteObjectResponse,
        GetObjectByIdRequest,
        GetObjectRevisionsRequest,
        GetObjectRevisionsResponse,
        GetObjectsRequest, //GetObjectsResponse,
        InitializeNewObjectRequest,
        InitializeNewObjectResponse,
        UpdateObjectRequest,
        UpdateObjectResponse,
    },
};

use crate::database;
use crate::database::connection::Database;
use crate::database::crud::utils::{
    from_object_key_values, naivedatetime_to_prost_time, to_object_key_values,
};
use crate::database::models::collection::CollectionObject;
use crate::database::models::enums::{Dataclass, HashType, ObjectStatus, SourceType};
use crate::database::models::object::{
    Endpoint, Hash, Object, ObjectKeyValue, ObjectLocation, Source,
};
use crate::database::schema::{
    collection_objects::dsl::*, endpoints::dsl::*, hashes::dsl::*, object_key_value::dsl::*,
    object_locations::dsl::*, objects::dsl::*, sources::dsl::*,
};

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
        let source: Option<Source> = match &request.source {
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
            dataclass: Dataclass::PRIVATE,
            source_id: source.as_ref().map(|src| src.id),
            origin_id: Some(object_uuid),
        };

        // Define the join table entry collection <--> object
        let collection_object = CollectionObject {
            id: uuid::Uuid::new_v4(),
            collection_id: uuid::Uuid::parse_str(&request.collection_id)?,
            is_latest: true, // TODO: is this really latest ?
            reference_status: database::models::enums::ReferenceStatus::STAGING,
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
            is_primary: false,
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

        // Struct to temporarily hold the database objects
        struct ObjectDto {
            object: Object,
            labels: Vec<KeyValue>,
            hooks: Vec<KeyValue>,
            hash: Hash,
            source: Option<Source>,
            latest: bool,
            update: bool,
        }

        // Read object from database
        let object_dto = self
            .pg_connection
            .get()?
            .transaction::<ObjectDto, Error, _>(|conn| {
                let object: Object = objects
                    .filter(database::schema::objects::id.eq(&object_uuid))
                    .first::<Object>(conn)?;

                let object_key_values =
                    ObjectKeyValue::belonging_to(&object).load::<ObjectKeyValue>(conn)?;
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
                    .filter(
                        database::schema::collection_objects::collection_id.eq(&collection_uuid),
                    )
                    .first::<bool>(conn)?;

                let latest_object_revision: Option<i64> = objects
                    .select(max(database::schema::objects::revision_number))
                    .filter(
                        database::schema::objects::shared_revision_id
                            .eq(&object.shared_revision_id),
                    )
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
            })?;

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
        let proto_object = ProtoObject {
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
        };

        Ok(proto_object)
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

    pub fn get_object_revisions(
        &self,
        _request: GetObjectRevisionsRequest,
    ) -> Result<GetObjectRevisionsResponse, Box<dyn std::error::Error>> {
        todo!()
    }

    pub fn get_objects(
        &self,
        _request: GetObjectsRequest,
    ) -> Result<GetObjectsResponse, Box<dyn std::error::Error>> {
        todo!()
    }

    ///ToDo: Rust Doc
    pub fn update_object(
        &self,
        _request: UpdateObjectRequest,
    ) -> Result<UpdateObjectResponse, ArunaError> {
        todo!()

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

        /*ToDo:
         * ----- ObjectFinishRequest starts here ---------------------------------------------------
         *  - Check if collection_objects reference with object_id+collection_id exists+STAGING exists
         *  - Set revision old_latest+1 (unique constraint shared_revision_id+revision)
         *  - Iterate through all collections_objects of old_latest (join collection)
         *      - Check if collection is pinned version
         *          - True: Do nothing.
         *          - Else:
         *              - Is Object auto_update?
         *                  - True: Replace collection_objects entry with latest
         *                      - Create object group revisions with updated object
         *                          - Update collection_object_groups object_group_ids only for collection_objects with auto_update true
         *                          - Copy object group with revision+1
         *                          - Copy object_group_object references and replace updated object_id
         *                          - Update collection_object_group object_group_ids only for collection_objects with auto_update true with new object_group_id
         *                  - False: set is_latest false
         *  - Set object with latest+1 status AVAILABLE
         *  - Set collection_objects reference latest+1 status OK
         */
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
        request: CloneObjectRequest,
    ) -> Result<CloneObjectResponse, ArunaError> {
        unimplemented!("CloneObjectRequest is missing target_collection_id. If fixed remove macro and uncomment rest of function body.");

    /*
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
    */
    }

    /// This performs a soft delete on the object. Instead of removing it
    /// from the database and storage backend, the status of the object will
    /// be set to DELETED.
    ///
    /// ## Arguments:
    ///
    ///
    /// ## Results:
    ///
    ///
    /// ## Behaviour:
    ///
    ///
    pub fn delete(
        &self,
        _request: DeleteObjectRequest,
    ) -> Result<DeleteObjectResponse, ArunaError> {
        todo!()

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

        //Ok(DeleteObjectResponse{})
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
        _request: DeleteObjectRequest,
    ) -> Result<DeleteObjectResponse, ArunaError> {
        todo!()

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
