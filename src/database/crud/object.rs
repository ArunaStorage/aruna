use chrono::{Datelike, Local, Timelike};

use diesel::prelude::*;
use diesel::result::Error;
use prost_types::Timestamp;

use crate::api::aruna::api::storage::{
    internal::v1::Location as ProtoLocation,
    models::v1::{
        Hash as ProtoHash,
        Object as ProtoObject,
        Origin as ProtoOrigin,
        Source as ProtoSource,
    },
    services::v1::{
        BorrowObjectRequest, BorrowObjectResponse,
        CloneObjectRequest, CloneObjectResponse,
        DeleteObjectRequest, DeleteObjectResponse,
        GetObjectByIdRequest,
        GetObjectHistoryByIdRequest, GetObjectHistoryByIdResponse,
        GetObjectsRequest, GetObjectsResponse,
        InitializeNewObjectRequest, InitializeNewObjectResponse,
        UpdateObjectRequest, UpdateObjectResponse
    }
};

use crate::database;
use crate::database::connection::Database;
use crate::database::crud::utils::to_object_key_values;
use crate::database::models::collection::CollectionObject;
use crate::database::models::enums::{Dataclass, EndpointType, HashType, ObjectStatus, SourceType};
use crate::database::models::object::{Endpoint, Hash, Object, ObjectLocation, Source};
use crate::database::schema::{
    collection_objects::dsl::*,
    endpoints::dsl::*,
    hashes::dsl::*,
    object_key_value::dsl::*,
    object_locations::dsl::*,
    objects::dsl::*,
    sources::dsl::*,
};
use crate::error::{ArunaError, GrpcNotFoundError};

/// Implementing CRUD+ database operations for Objects
impl Database {
    /// Creates the following records in the database to initialize a new object:
    /// * Source
    /// * Endpoint
    /// * Object incl. join table entry
    /// * ObjectLocation
    /// * HashType
    /// * Hash
    /// * ObjectKeyValue(s)
    ///
    /// ## Arguments
    ///
    /// * `request` - A gRPC request containing the needed information to create a new object
    ///
    /// ## Returns
    ///
    /// This function returns a `Result<(Response<InitializeNewObjectResponse>, Location)`.
    /// The `InitializeNewObjectResponse` is incomplete and missing the staging/upload id of the created object
    /// which in our case should be requested from the data proxy server with the Location object.
    ///
    pub fn create_object(
        &self,
        request: &InitializeNewObjectRequest,
        creator: &uuid::Uuid,
        location: &ProtoLocation,
        upload_id: String
    ) -> Result<InitializeNewObjectResponse, ArunaError> {

        // Check if StageObject is available
        let staging_object = request.object.clone().ok_or(GrpcNotFoundError::STAGEOBJ)?;

        //Define source object from updated request; None if empty
        let source: Option<Source> = match &request.source {
            Some(source) =>
                Some(Source {
                    id: uuid::Uuid::new_v4(),
                    link: source.identifier.clone(),
                    source_type: SourceType::from_i32(source.source_type)?
                }),
            _ => None,
        };

        // Define endpoint object
        let endpoint = Endpoint {
            id: uuid::Uuid::new_v4(),
            endpoint_type: EndpointType::INITIALIZING,
            proxy_hostname: "".to_string(),
            internal_hostname: "".to_string(),
            documentation_path: None,
            is_public: false,
        };

        // Define object in database representation
        let object_uuid = uuid::Uuid::new_v4();
        let object = Object {
            id: object_uuid,
            shared_revision_id: uuid::Uuid::new_v4(),
            revision_number: 0,
            filename: staging_object.filename.clone(),
            created_at: Local::now().naive_local(),
            created_by: creator.clone(),
            content_len: 0,
            object_status: ObjectStatus::INITIALIZING,
            dataclass: Dataclass::PRIVATE,
            source_id: match &source {
                Some(src) => Some(src.id),
                _ => None
            },
            origin_id: Some(object_uuid),
        };

        // Define the join table entry collection <--> object
        let collection_object = CollectionObject {
            id: uuid::Uuid::new_v4(),
            collection_id: uuid::Uuid::parse_str(&request.collection_id)?,
            object_id: object.id.clone(),
            is_specification: false, //Note: Default is false;
            writeable: true //Note: Original object is always writeable for owner
        };

        // Define the initial object location
        let object_location = ObjectLocation {
            id: uuid::Uuid::new_v4(),
            bucket: location.bucket.clone(),
            path: location.path.clone(),
            endpoint_id: endpoint.id.clone(),
            object_id: object.id.clone(),
            is_primary: false,
        };

        // Define the hash placeholder for the object
        let empty_hash = Hash {
            id: uuid::Uuid::new_v4(),
            hash: "".to_string(), //Note: Empty hash will be updated later
            object_id: object.id.clone(),
            hash_type: HashType::MD5, //Note: Default. Will be updated later
        };

        // Convert the object's labels and hooks to their database representation
        let key_value_pairs = to_object_key_values(
            staging_object.labels.clone(),
            staging_object.hooks.clone(),
            object_uuid,
        );

        // Insert all defined objects into the database
        self.pg_connection
            .get()?
            .transaction::<_, Error, _>(|conn| {
                diesel::insert_into(sources).values(&source).execute(conn)?;
                diesel::insert_into(endpoints).values(&endpoint).execute(conn)?;
                diesel::insert_into(objects).values(&object).execute(conn)?;
                diesel::insert_into(object_locations).values(&object_location).execute(conn)?;
                diesel::insert_into(hashes).values(&empty_hash).execute(conn)?;
                diesel::insert_into(object_key_value).values(&key_value_pairs).execute(conn)?;
                diesel::insert_into(collection_objects).values(&collection_object).execute(conn)?;

                Ok(())
            })?;

        // Return already complete gRPC response
        return Ok(
            InitializeNewObjectResponse {
                id: object.id.to_string(),
                staging_id: upload_id.to_string(),
                collection_id: request.collection_id.clone(),
            });
    }

    pub fn get_object(
        &self,
        request: &GetObjectByIdRequest,
    ) -> Result<(ProtoObject, ProtoLocation), ArunaError> {

        // Check if id in request has valid format
        let object_uuid = uuid::Uuid::parse_str(&request.object_id)?;

        // Struct to temporarily hold the database objects
        struct ObjectDto {
            object: Object,
            hash: Hash,
            source: Option<Source>,
            location: ObjectLocation,
            latest: bool
        }

        // Read object from database
        let object_dto = self.pg_connection
            .get()?
            .transaction::<ObjectDto, Error, _>(|conn| {
                //Note: Get Object (with labels and hooks --> None?)
                let object: Object = objects
                    //.select(max(database::schema::objects::revision_number))
                    .filter(database::schema::objects::id.eq(object_uuid))
                    //.filter(database::schema::objects::revision_number.eq(request.revision))  //ToDo: Exact revision or
                    //.order(database::schema::objects::revision_number.desc())                 //ToDo: Latest
                    .first::<Object>(conn)?;

                let object_hash: Hash = Hash::belonging_to(&object)
                    .first::<Hash>(conn)?;

                //Note: Only read primary location of object?
                let location: ObjectLocation = ObjectLocation::belonging_to(&object)
                    .filter(database::schema::object_locations::is_primary.eq(true))
                    .first::<ObjectLocation>(conn)?;

                let source: Option<Source> = match object.source_id {
                    None => None,
                    Some(src_id) =>
                        Some(sources
                            .filter(database::schema::sources::id.eq(src_id))
                            .first::<Source>(conn)?)
                };

                //Ok((object, source, location))
                Ok(ObjectDto {
                    object,
                    hash: object_hash,
                    source,
                    location,
                    latest: false //ToDo: Check if object is latest revision
                })
            })?;

        //ToDo: - Transform to proto source (if available)
        //      - Create proto origin
        //      - Create proto hash + hash type
        //      - Transform to proto object
        //      - Return response with object but without url
        let proto_source = match object_dto.source {
            None => None,
            Some(source) => Some(
                ProtoSource {
                    identifier: source.link,
                    source_type: source.source_type as i32
                }
            )
        };

        // If object id == origin id --> original object
        //Note: OriginType only stored implicitly
        let proto_origin: Option<ProtoOrigin> = match object_dto.object.origin_id {
            None => None,
            Some(origin_uuid) => Some(
                ProtoOrigin {
                    id: origin_uuid.to_string(),
                    r#type: match object_dto.object.id == origin_uuid {
                        true => 1,
                        false => 2
                    }
                }
            )
        };

        // Horrible NaiveDatetime to Timestamp cast ...
        let timestamp = Timestamp::date_time(
            object_dto.object.created_at.year() as i64,
            object_dto.object.created_at.month() as u8,
            object_dto.object.created_at.day() as u8,
            object_dto.object.created_at.hour() as u8,
            object_dto.object.created_at.minute() as u8,
            object_dto.object.created_at.second() as u8
        )?;

        let proto_hash = ProtoHash {
            alg: object_dto.hash.hash_type as i32,
            hash: object_dto.hash.hash
        };

        let proto_object = ProtoObject {
            id: object_dto.object.id.to_string(),
            filename: object_dto.object.filename,
            labels: vec![], //Todo: Individual request, pagination or brainless full list?
            hooks: vec![],  //Todo: Individual request, pagination or brainless full list?
            created: Some(timestamp),
            content_len: object_dto.object.content_len,
            status: object_dto.object.object_status as i32,
            origin: proto_origin, //ToDo: OriginType only implicit?
            data_class: object_dto.object.dataclass as i32,
            hash: Some(proto_hash),
            rev_number: object_dto.object.revision_number,
            source: proto_source,
            latest: object_dto.latest,
            auto_update: false //Note: ?
        };

        let proto_location = ProtoLocation {
            r#type: 0, //ToDo: The fuck is this mapping?
            bucket: object_dto.location.bucket,
            path: object_dto.location.path
        };

        return Ok((proto_object, proto_location));
    }

    pub fn get_object_history(
        &self,
        request: GetObjectHistoryByIdRequest,
    ) -> Result<GetObjectHistoryByIdResponse, Box<dyn std::error::Error>> {
        todo!()
    }

    pub fn get_objects(
        &self,
        request: GetObjectsRequest,
    ) -> Result<GetObjectsResponse, Box<dyn std::error::Error>> {
        todo!()
    }

    pub fn update_object(
        &self,
        request: UpdateObjectRequest,
    ) -> Result<UpdateObjectResponse, Box<dyn std::error::Error>> {
        todo!()
    }

    pub fn borrow_object(
        &self,
        request: BorrowObjectRequest,
    ) -> Result<BorrowObjectResponse, Box<dyn std::error::Error>> {
        todo!()
    }

    pub fn clone_object(
        &self,
        request: CloneObjectRequest,
    ) -> Result<CloneObjectResponse, Box<dyn std::error::Error>> {
        todo!()
    }

    pub fn delete_object(
        &self,
        request: DeleteObjectRequest,
    ) -> Result<DeleteObjectResponse, Box<dyn std::error::Error>> {
        todo!()
    }

    //ToDo: Implement higher level database operations
    //      - e.g. get_object_with_labels
    //      - ...
}
