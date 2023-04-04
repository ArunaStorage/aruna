use std::sync::Arc;
use tonic::{Code, Request, Response, Status};

use super::authz::Authz;

use crate::database::connection::Database;
use crate::database::models::object::Endpoint;
use crate::error::{ArunaError, TypeConversionError};
use crate::server::services::utils::{format_grpc_request, format_grpc_response};
use aruna_rust_api::api::storage::models::v1::Endpoint as ProtoEndpoint;
use aruna_rust_api::api::storage::services::v1::endpoint_service_server::EndpointService;
use aruna_rust_api::api::storage::services::v1::{
    AddEndpointRequest, AddEndpointResponse, DeleteEndpointRequest, DeleteEndpointResponse,
    GetDefaultEndpointRequest, GetDefaultEndpointResponse, GetEndpointRequest, GetEndpointResponse,
    GetEndpointsRequest, GetEndpointsResponse,
};

// This macro automatically creates the Impl struct with all associated fields
crate::impl_grpc_server!(EndpointServiceImpl, default_endpoint: Endpoint);

///ToDo: Rust Doc
#[tonic::async_trait]
impl EndpointService for EndpointServiceImpl {
    /// Register a new data proxy endpoint.
    ///
    /// ## Arguments:
    ///
    /// * `request`: A gRPC request containing all the needed information to create a new endpoint.
    ///
    /// ## Returns:
    ///
    /// * `Result<Response<AddEndpointResponse>, Status>`:
    ///   - **On success**: Response with the newly generated endpoint including its unique id
    ///   - **On failure**: Status error with failure details
    ///
    async fn add_endpoint(
        &self,
        request: Request<AddEndpointRequest>,
    ) -> Result<Response<AddEndpointResponse>, Status> {
        log::info!("Received AddEndpointRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // For now only admins can add endpoint data
        self.authz.admin_authorize(request.metadata()).await?;

        let inner_request = request.into_inner();
        let (endpoint, pubkey_serial) = self.database.add_endpoint(&inner_request)?;

        // Transform database Endpoint to proto Endpoint
        let mut proto_endpoint = ProtoEndpoint::try_from(endpoint)
            .map_err(|_| ArunaError::TypeConversionError(TypeConversionError::PROTOCONVERSION))?;
        proto_endpoint.is_default = proto_endpoint.id == self.default_endpoint.id.to_string();

        // Return gRPC response after everything succeeded
        let response = Response::new(AddEndpointResponse {
            endpoint: Some(proto_endpoint),
            pubkey_serial,
        });

        log::info!("Sending AddEndpointResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }

    /// Get a specific data proxy endpoint.
    ///
    /// ## Arguments:
    ///
    /// * `request`: A gRPC request containing the id or name of a specific endpoint.
    ///
    /// ## Returns:
    ///
    /// * `Result<Response<GetEndpointResponse>, Status>`:
    ///   - **On success**: Response with the endpoint info
    ///   - **On failure**: Status error with failure details
    ///
    async fn get_endpoint(
        &self,
        request: Request<GetEndpointRequest>,
    ) -> Result<Response<GetEndpointResponse>, Status> {
        log::info!("Received GetEndpointRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // For now only admins can add endpoint data
        self.authz.admin_authorize(request.metadata()).await?;

        // Get Endpoint from database
        let inner_request = request.into_inner();
        let endpoint = (match inner_request.endpoint {
            None =>
                Err(
                    ArunaError::InvalidRequest(
                        "endpoint info in request cannot be empty.".to_string()
                    )
                ),
            Some(ep_info) =>
                match ep_info {
                    aruna_rust_api::api::storage::services::v1::get_endpoint_request::Endpoint::EndpointName(
                        ep_name,
                    ) => self.database.get_endpoint_by_name(ep_name.as_str()),
                    aruna_rust_api::api::storage::services::v1::get_endpoint_request::Endpoint::EndpointId(
                        ep_id,
                    ) => {
                        let ep_uuid = uuid::Uuid
                            ::parse_str(ep_id.as_str())
                            .map_err(ArunaError::from)?;
                        self.database.get_endpoint(&ep_uuid)
                    }
                }
        })?;

        // Transform database Endpoint to proto Endpoint
        let mut proto_endpoint = ProtoEndpoint::try_from(endpoint)
            .map_err(|_| ArunaError::TypeConversionError(TypeConversionError::PROTOCONVERSION))?;
        proto_endpoint.is_default = proto_endpoint.id == self.default_endpoint.id.to_string();

        // Return gRPC response after everything succeeded
        let response = Response::new(GetEndpointResponse {
            endpoint: Some(proto_endpoint),
        });

        log::info!("Sending GetEndpointResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }

    /// Get all public data proxy endpoints.
    ///
    /// ## Arguments:
    ///
    /// * `request`: An empty gRPC request signalling to get all public endpoints.
    ///
    /// ## Returns:
    ///
    /// * `Result<Response<GetEndpointResponse>, Status>`:
    ///   - **On success**: Response with the endpoint info of all public endpoints
    ///   - **On failure**: Status error with failure details
    ///
    async fn get_endpoints(
        &self,
        request: Request<GetEndpointsRequest>,
    ) -> Result<Response<GetEndpointsResponse>, Status> {
        log::info!("Received GetEndpointsRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // For now only admins can add endpoint data
        self.authz.admin_authorize(request.metadata()).await?;

        // Get endpoints from database
        let db_endpoints = self.database.get_endpoints()?;

        // Transform database endpoints to proto endpoints
        let proto_endpoints: Vec<ProtoEndpoint> = db_endpoints
            .into_iter()
            .map(ProtoEndpoint::try_from)
            .collect::<Result<Vec<ProtoEndpoint>, _>>()?;

        // Return gRPC response after everything succeeded
        let response = Response::new(GetEndpointsResponse {
            endpoints: proto_endpoints,
        });

        log::info!("Sending GetEndpointsResponse back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }

    /// Delete a specific data proxy endpoint.
    ///
    /// ## Arguments:
    ///
    /// * `request`: A gRPC request containing a unique endpoint id.
    ///
    /// ## Returns:
    ///
    /// * `Result<Response<GetEndpointResponse>, Status>`:
    ///   - **On success**: Empty response signalling deletion success
    ///   - **On failure**: Status error with failure details
    ///
    async fn delete_endpoint(
        &self,
        request: Request<DeleteEndpointRequest>,
    ) -> Result<Response<DeleteEndpointResponse>, Status> {
        log::info!("Received DeleteEndpointRequest.");
        log::debug!("{}", format_grpc_request(&request));

        Err(Status::new(
            Code::Unimplemented,
            "Not sure about the conditions for an implementation.",
        ))
    }

    /// Get the default data proxy endpoint of the current AOS instance.
    ///
    /// ## Arguments:
    ///
    /// * `request`: An empty gRPC request signalling to get the default endpoint.
    ///
    /// ## Returns:
    ///
    /// * `Result<Response<GetEndpointResponse>, Status>`:
    ///   - **On success**:  Response with the endpoint info of the default endpoint
    ///   - **On failure**: Status error with failure details
    ///
    async fn get_default_endpoint(
        &self,
        request: Request<GetDefaultEndpointRequest>,
    ) -> Result<Response<GetDefaultEndpointResponse>, Status> {
        log::info!("Received GetDefaultEndpointRequest.");
        log::debug!("{}", format_grpc_request(&request));

        // For now only admins can request endpoint data
        self.authz.admin_authorize(request.metadata()).await?;

        // Transform database Endpoint to proto Endpoint
        let mut proto_endpoint = ProtoEndpoint::try_from(self.default_endpoint.clone())
            .map_err(|_| ArunaError::TypeConversionError(TypeConversionError::PROTOCONVERSION))?;

        proto_endpoint.is_default = proto_endpoint.id == self.default_endpoint.id.to_string();

        log::info!("Sending GetDefaultEndpointResponse back to client.");
        Ok(Response::new(GetDefaultEndpointResponse {
            endpoint: Some(proto_endpoint),
        }))
    }
}

// ----- Helper functions for endpoint service implementation ----------
impl TryFrom<Endpoint> for ProtoEndpoint {
    type Error = ArunaError;

    fn try_from(db_endpoint: Endpoint) -> Result<Self, Self::Error> {
        Ok(ProtoEndpoint {
            id: db_endpoint.id.to_string(),
            ep_type: db_endpoint.endpoint_type as i32,
            name: db_endpoint.name.to_string(),
            proxy_hostname: db_endpoint.proxy_hostname.to_string(),
            internal_hostname: db_endpoint.internal_hostname.to_string(),
            documentation_path: match db_endpoint.documentation_path {
                None => "".to_string(),
                Some(path) => path,
            },
            is_public: db_endpoint.is_public,
            is_default: false,
            status: db_endpoint.status as i32, //ToDo: How to to know default from outside?
        })
    }
}
