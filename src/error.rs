//! This contains all ErrorTypes for the Aruna application
//! The main errortype is `ArunaError`
use diesel::r2d2::Error as DieselR2d2Error;
use jsonwebtoken::errors::Error as jwterror;
use r2d2::Error as R2d2Error;
use reqwest::Error as Rqwerror;
use std::error::Error as StdError;
use std::fmt::Display;
use tokio::task::JoinError as AsyncJoinError;
use tonic::metadata::errors::ToStrError as TonicToStrError;
use tonic::Status as GrpcError;
use uuid::Error as UuidError;

use diesel::result::Error as DieselError;
use prost_types::TimestampError;

/// The main ArunaError, all Results for Aruna should return this error
/// For this it implements `From` for all other errortypes that may occur in
/// the application
#[derive(Debug)]
pub enum ArunaError {
    DieselError(DieselError),                 // All errors that occur in Diesel
    ConnectionError(ConnectionError),         // All errors that occur in internal Connections
    TypeConversionError(TypeConversionError), // All type conversion errors
    GrpcNotFoundError(GrpcNotFoundError),
    DataProxyError(GrpcError),      // All data proxy errors
    AsyncJoinError(AsyncJoinError), // All missing grpc fields errors
    TimestampError(TimestampError), // All Errors from crude conversions to prost_types::TimestampError
    PERMISSIONDENIED,
    OIDCERROR, // Token with invalid permissions
}

impl Display for ArunaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArunaError::DieselError(diesel_error) => write!(f, "{}", diesel_error),
            ArunaError::ConnectionError(con_error) => write!(f, "{}", con_error),
            ArunaError::TypeConversionError(t_con_error) => write!(f, "{}", t_con_error),
            ArunaError::GrpcNotFoundError(grpc_not_found_error) => {
                write!(f, "{}", grpc_not_found_error)
            }
            ArunaError::DataProxyError(data_proxy_error) => write!(f, "{}", data_proxy_error),
            ArunaError::AsyncJoinError(async_join_error) => write!(f, "{}", async_join_error),
            ArunaError::TimestampError(timestamp_error) => write!(f, "{}", timestamp_error),
            ArunaError::PERMISSIONDENIED => write!(f, "Permission denied"),
            ArunaError::OIDCERROR => write!(f, "Oidc requests failed"),
        }
    }
}

// ---------------------- From<OtherError> impls ---------------------------------

impl From<DieselR2d2Error> for ArunaError {
    fn from(dr2d2error: DieselR2d2Error) -> Self {
        ArunaError::ConnectionError(ConnectionError::DbConnectionError(dr2d2error))
    }
}

impl From<DieselError> for ArunaError {
    fn from(derror: DieselError) -> Self {
        ArunaError::DieselError(derror)
    }
}

impl From<R2d2Error> for ArunaError {
    fn from(cperror: R2d2Error) -> Self {
        ArunaError::ConnectionError(ConnectionError::DbConnectionPoolError(cperror))
    }
}

impl From<UuidError> for ArunaError {
    fn from(_: UuidError) -> Self {
        ArunaError::TypeConversionError(TypeConversionError::UUID)
    }
}

impl From<TonicToStrError> for ArunaError {
    fn from(_: TonicToStrError) -> Self {
        ArunaError::TypeConversionError(TypeConversionError::TONICMETADATATOSTR)
    }
}

impl From<GrpcError> for ArunaError {
    fn from(data_proxy_error: GrpcError) -> Self {
        ArunaError::DataProxyError(data_proxy_error)
    }
}

impl From<AsyncJoinError> for ArunaError {
    fn from(aerror: AsyncJoinError) -> Self {
        ArunaError::AsyncJoinError(aerror)
    }
}

impl From<TimestampError> for ArunaError {
    fn from(timestamp_error: TimestampError) -> Self {
        ArunaError::TimestampError(timestamp_error)
    }
}

impl From<jwterror> for ArunaError {
    fn from(_: jwterror) -> Self {
        ArunaError::TypeConversionError(TypeConversionError::JWT)
    }
}

impl From<Rqwerror> for ArunaError {
    fn from(_: Rqwerror) -> Self {
        ArunaError::OIDCERROR
    }
}

//------------------ Impl to_tonic_status --------------------------------

impl From<ArunaError> for tonic::Status {
    fn from(aerror: ArunaError) -> Self {
        match aerror {
            ArunaError::ConnectionError(_) => tonic::Status::internal("internal server error"),
            ArunaError::DieselError(_) => tonic::Status::internal("internal server error"),
            ArunaError::DataProxyError(_) => tonic::Status::internal("internal data proxy error"),
            ArunaError::TimestampError(_) => tonic::Status::internal("internal server error"),
            ArunaError::TypeConversionError(e) => tonic::Status::invalid_argument(e.to_string()),
            ArunaError::GrpcNotFoundError(e) if e == GrpcNotFoundError::METADATATOKEN => {
                tonic::Status::unauthenticated(e.to_string())
            }
            ArunaError::GrpcNotFoundError(missing) => {
                tonic::Status::invalid_argument(missing.to_string())
            }
            ArunaError::AsyncJoinError(e) => tonic::Status::internal(e.to_string()),
            ArunaError::PERMISSIONDENIED => tonic::Status::permission_denied("Permission denied"),
            ArunaError::OIDCERROR => tonic::Status::internal("Oidc request failed"),
        }
    }
}

//------------------ Impls for own integrated error-types ----------------

impl From<GrpcNotFoundError> for ArunaError {
    fn from(grpc_not_found: GrpcNotFoundError) -> Self {
        ArunaError::GrpcNotFoundError(grpc_not_found)
    }
}

impl From<ConnectionError> for ArunaError {
    fn from(con_error: ConnectionError) -> Self {
        ArunaError::ConnectionError(con_error)
    }
}

impl From<TypeConversionError> for ArunaError {
    fn from(tc_error: TypeConversionError) -> Self {
        ArunaError::TypeConversionError(tc_error)
    }
}

impl StdError for ArunaError {}

// ----------------- Sub-error-types --------------------------------------

/// `ConnectionError` is an Enum that bundles all connection errors, for now r2d2::Error and the corresponding diesel::r2d2::Error
/// All errors indicate a connection problem.
#[derive(Debug)]
pub enum ConnectionError {
    DbConnectionError(DieselR2d2Error),
    DbConnectionPoolError(R2d2Error),
}

impl Display for ConnectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionError::DbConnectionError(con_error) => write!(f, "{}", con_error),
            ConnectionError::DbConnectionPoolError(con_pool_error) => {
                write!(f, "{}", con_pool_error)
            }
        }
    }
}

/// `TypeConversionError` is a wrapper that bundles all errors that can occur when converting types
/// for example converting a `String` to an `UUID` may result in an error
#[derive(Debug)]
pub enum TypeConversionError {
    UUID,
    TONICMETADATATOSTR,
    JWT,
}

impl Display for TypeConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TypeConversionError::UUID => write!(f, "Typeconversion for UUID failed",),
            TypeConversionError::TONICMETADATATOSTR => {
                write!(f, "Typeconversion for gRPC metadata 'to_str' failed")
            }
            TypeConversionError::JWT => write!(f, "Typeconversion for JWT failed",),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum GrpcNotFoundError {
    METADATATOKEN,
    PROJECTID,
    COLLECTIONID,
    STAGEOBJ,
}

impl Display for GrpcNotFoundError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GrpcNotFoundError::METADATATOKEN => write!(f, "Missing Token in Metadata",),
            GrpcNotFoundError::PROJECTID => {
                write!(f, "Missing ProjectId in Request")
            }
            GrpcNotFoundError::COLLECTIONID => write!(f, "Missing CollectionId in Request"),
            GrpcNotFoundError::STAGEOBJ => write!(f, "Missing StageObject in Request"),
        }
    }
}
