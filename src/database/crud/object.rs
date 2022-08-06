use std::io::{Error, ErrorKind};
use chrono::Local;
use diesel::{Connection, Insertable, RunQueryDsl};
use diesel::result::Error as diesel_error;

use crate::api::aruna::api::storage::internal::v1::Location;
use crate::api::aruna::api::storage::services::v1::{
    BorrowObjectRequest, BorrowObjectResponse,
    CloneObjectRequest, CloneObjectResponse,
    DeleteObjectRequest, DeleteObjectResponse,
    GetObjectByIdRequest, GetObjectByIdResponse,
    GetObjectHistoryByIdRequest, GetObjectHistoryByIdResponse,
    GetObjectsRequest, GetObjectsResponse,
    InitializeNewObjectRequest, InitializeNewObjectResponse,
    UpdateObjectRequest, UpdateObjectResponse};

use crate::database::connection::Database;
use crate::database::crud::utils::to_object_key_values;
use crate::database::models::collection::CollectionObject;
use crate::database::models::enums::{Dataclass, EndpointType, ObjectStatus, SourceType};
use crate::database::models::object::{Endpoint, Hash, HashType, Object, ObjectLocation, Source};
use crate::database::schema::{
    sources::dsl::*,
    endpoints::dsl::*,
    object_locations::dsl::*,
    objects::dsl::*,
    hash_types::dsl::*,
    hashes::dsl::*,
    object_key_value::dsl::*,
    collection_objects::dsl::*,
};

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
        request: InitializeNewObjectRequest,
        creator: uuid::Uuid,
    ) -> Result<(InitializeNewObjectResponse, Location), Box<dyn std::error::Error>> {

        let staging_object = request.object.ok_or(
            Error::new(ErrorKind::InvalidData, "StageObject cannot be None in request."))?;

        // Define source object
        let source = Source {
            id: uuid::Uuid::new_v4(),
            link: "".to_string(),
            source_type: SourceType::S3, //Note: Cannot derive from request so default is S3
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
            created_by: creator,
            content_len: 0,
            object_status: ObjectStatus::INITIALIZING,
            dataclass: Dataclass::PRIVATE,
            source_id: Some(source.id),
            origin_id: Some(object_uuid),
        };

        // Define the join table entry collection <--> object
        let collection_object = CollectionObject {
            id: uuid::Uuid::new_v4(),
            collection_id: uuid::Uuid::parse_str(&request.collection_id)?,
            object_id: object.id.clone(),
            is_specification: false, //Note: Cannot derive from request so default is false
            writeable: false //Note: Cannot derive from request so default is false
        };

        // Define the initial object location
        let object_location = ObjectLocation {
            id: uuid::Uuid::new_v4(),
            bucket: uuid::Uuid::new_v4().to_string(),
            path: uuid::Uuid::new_v4().to_string(),
            endpoint_id: endpoint.id.clone(),
            object_id: object.id.clone(),
            is_primary: false,
        };

        // Define the default hash type
        let default_hash_type = HashType {
            id: uuid::Uuid::new_v4(),
            name: "MD5".to_string(), //Note: MD5 is just default.
        };

        // Define the hash placeholder for the object
        let empty_hash = Hash {
            id: uuid::Uuid::new_v4(),
            hash: "".to_string(), //Note: Empty hash will be updated later
            object_id: object.id.clone(),
            hash_type: default_hash_type.id,
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
            .transaction::<_, diesel_error, _>(|conn| {
                diesel::insert_into(sources).values(&source).execute(conn)?;
                diesel::insert_into(endpoints).values(&endpoint).execute(conn)?;
                diesel::insert_into(objects).values(&object).execute(conn)?;
                diesel::insert_into(object_locations).values(&object_location).execute(conn)?;
                diesel::insert_into(hash_types).values(&default_hash_type).execute(conn)?;
                diesel::insert_into(hashes).values(&empty_hash).execute(conn)?;
                diesel::insert_into(object_key_value).values(&key_value_pairs).execute(conn)?;
                diesel::insert_into(collection_objects).values(&collection_object).execute(conn)?;

                Ok(())
            })?;

        // If transaction was successful return response and object location for data proxy as tuple
        return Ok(
            (InitializeNewObjectResponse {
                id: object.id.to_string(),
                staging_id: "".to_string(),
                collection_id: request.collection_id.clone(),
            }, Location {
                r#type: 0,
                bucket: object_location.bucket,
                path: object_location.path,
            })
        );
    }

    pub fn get_object(
        &self,
        request: GetObjectByIdRequest,
    ) -> Result<GetObjectByIdResponse, Box<dyn std::error::Error>> {
        todo!()
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
