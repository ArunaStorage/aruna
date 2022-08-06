use crate::api::aruna::api::storage::internal::v1::Location;
use crate::api::aruna::api::storage::services::v1::{BorrowObjectRequest, BorrowObjectResponse, CloneObjectRequest, CloneObjectResponse, DeleteObjectRequest, DeleteObjectResponse, GetObjectByIdRequest, GetObjectByIdResponse, GetObjectHistoryByIdRequest, GetObjectHistoryByIdResponse, GetObjectsRequest, GetObjectsResponse, InitializeNewObjectRequest, InitializeNewObjectResponse, UpdateObjectRequest, UpdateObjectResponse};

use crate::database::connection::Database;

/// Implementing CRUD database operations for the Object struct
impl Database {
    pub fn create_object(
        &self,
        request: InitializeNewObjectRequest,
        creator: uuid::Uuid,
    ) -> Result<(InitializeNewObjectResponse, Location), Box<dyn std::error::Error>> {
        todo!()
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
