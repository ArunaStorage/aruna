use chrono::Local;

use diesel::dsl::max;
use diesel::prelude::*;
use diesel::result::Error;

use crate::api::aruna::api::storage::services::v1::{
    CreateObjectReferenceRequest, CreateObjectReferenceResponse,
};
use crate::error::{ArunaError, GrpcNotFoundError};

use crate::api::aruna::api::storage::{
    internal::v1::{Location as ProtoLocation, LocationType},
    models::v1::{
        Hash as ProtoHash, KeyValue, Object as ProtoObject, Origin as ProtoOrigin,
        Source as ProtoSource,
    },
    services::v1::{
        CloneObjectRequest, CloneObjectResponse, DeleteObjectRequest, DeleteObjectResponse,
        GetObjectByIdRequest, GetObjectRevisionsRequest, GetObjectRevisionsResponse,
        GetObjectsRequest, GetObjectsResponse, InitializeNewObjectRequest,
        InitializeNewObjectResponse, UpdateObjectRequest, UpdateObjectResponse,
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
    pub fn get_location_endpoint(
        &self,
        _location: &ObjectLocation,
    ) -> Result<Vec<(ObjectLocation, Endpoint)>, ArunaError> {
        todo!()
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

    pub fn update_object(
        &self,
        _request: UpdateObjectRequest,
    ) -> Result<UpdateObjectResponse, Box<dyn std::error::Error>> {
        todo!()
    }

    pub fn borrow_object(
        &self,
        _request: CreateObjectReferenceRequest,
    ) -> Result<CreateObjectReferenceResponse, Box<dyn std::error::Error>> {
        todo!()
    }

    pub fn clone_object(
        &self,
        _request: CloneObjectRequest,
    ) -> Result<CloneObjectResponse, Box<dyn std::error::Error>> {
        todo!()
    }

    pub fn delete_object(
        &self,
        _request: DeleteObjectRequest,
    ) -> Result<DeleteObjectResponse, Box<dyn std::error::Error>> {
        todo!()
    }

    //ToDo: Implement higher level database operations
    //      - e.g. get_object_with_labels
    //      - ...
}
