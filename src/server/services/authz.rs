use crate::database::connection::Database;
use crate::database::models::enums::{Resources, UserRights};
use crate::error::ArunaError;
use crate::error::GrpcNotFoundError;
use std::sync::Arc;
use tonic::metadata::MetadataMap;

pub struct Authz {}

/// This struct represents a request "Context" it is used to specify the
/// accessed resource_type and id as well as the needed permissions
pub struct Context {
    pub user_right: UserRights,
    pub resource_type: Resources,
    pub resource_id: uuid::Uuid,
    pub admin: bool,
}

/// Implementations for the Authz struct contain methods to create and check
/// authorizations for the database
impl Authz {
    /// The `authorize` method is used to check if the supplied user token has enough permissions
    /// to fullfill the gRPC request the `db.get_checked_user_id_from_token()` method will check if the token and its
    /// associated permissions permissions are sufficient enough to execute the request
    ///
    /// ## Arguments
    ///
    /// - Arc of Database
    /// - Metadata of the request containing a token
    /// - Context that specifies which ressource is accessed and which permissions are requested
    ///
    /// ## Return
    ///
    /// This returns an Result<UUID> or an Error
    /// If it returns an Error the authorization failed otherwise
    /// the uuid is the user_id of the user that owns the token
    /// this user_id will for example be used to specify the "created_by" field in the database
    ///   
    pub fn authorize(
        db: Arc<Database>,
        metadata: &MetadataMap,
        context: Context,
    ) -> Result<uuid::Uuid, ArunaError> {
        let token = metadata
            .get("Bearer")
            .ok_or(ArunaError::GrpcNotFoundError(
                GrpcNotFoundError::METADATATOKEN,
            ))?
            .to_str()?;

        db.get_checked_user_id_from_token(token, context)
    }
}
