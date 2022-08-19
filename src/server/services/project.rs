use super::authz::Authz;
use crate::api::aruna::api::storage::services::v1::user_service_server::UserService;
use crate::api::aruna::api::storage::services::v1::*;
use crate::database::connection::Database;
use crate::error::ArunaError;
use std::sync::Arc;
use tonic::Response;

/// UserService struct
pub struct ProjectServiceImpl {
    database: Arc<Database>,
    authz: Arc<Authz>,
}

/// All general methods for the UserService
/// Currently only new()
impl ProjectServiceImpl {
    /// Create a new UserServiceImpl that can be registered in the gRPC Server
    pub async fn new(db: Arc<Database>, authz: Arc<Authz>) -> Self {
        ProjectServiceImpl {
            database: db,
            authz,
        }
    }
}
