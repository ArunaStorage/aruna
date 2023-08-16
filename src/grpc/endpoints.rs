use crate::auth::permission_handler::PermissionHandler;
use crate::auth::structs::Context;
use crate::caching::cache::Cache;
use crate::caching::structs::PubKey;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::endpoints_request_types::{CreateEP, DeleteEP, GetEP};
use crate::utils::conversions::get_token_from_md;
use aruna_rust_api::api::storage::models::v2::Endpoint;
use aruna_rust_api::api::storage::services::v2::endpoint_service_server::EndpointService;
use aruna_rust_api::api::storage::services::v2::{
    CreateEndpointRequest, CreateEndpointResponse, DeleteEndpointRequest, DeleteEndpointResponse,
    FullSyncEndpointRequest, FullSyncEndpointResponse, GetDefaultEndpointRequest,
    GetDefaultEndpointResponse, GetEndpointRequest, GetEndpointResponse, GetEndpointsRequest,
    GetEndpointsResponse,
};
use jsonwebtoken::DecodingKey;
use std::sync::Arc;
use tonic::{Request, Response, Result, Status};

crate::impl_grpc_server!(EndpointServiceImpl);

#[tonic::async_trait]
impl EndpointService for EndpointServiceImpl {
    async fn create_endpoint(
        &self,
        request: Request<CreateEndpointRequest>,
    ) -> Result<Response<CreateEndpointResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = CreateEP(request.into_inner());

        let ctx = Context::admin();
        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );
        let (ep, pk) = tonic_invalid!(
            self.database_handler.create_endpoint(request).await,
            "Invalid create endpoint request"
        );

        let test = tonic_invalid!(
            DecodingKey::from_ed_components(&pk.pubkey),
            "Invalid pubkey"
        );

        self.cache
            .add_pubkey(pk.id as i32, PubKey::DataProxy((pk.pubkey, test, ep.id)));

        let result = CreateEndpointResponse {
            endpoint: Some(tonic_internal!(ep.try_into(), "Endpoint conversion error")),
        };

        return_with_log!(result);
    }

    async fn full_sync_endpoint(
        &self,
        _request: Request<FullSyncEndpointRequest>,
    ) -> Result<Response<FullSyncEndpointResponse>> {
        todo!()
    }

    async fn get_endpoint(
        &self,
        request: Request<GetEndpointRequest>,
    ) -> Result<Response<GetEndpointResponse>> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = GetEP(request.into_inner());

        let ctx = Context::admin();
        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );
        let ep = tonic_invalid!(
            self.database_handler.get_endpoint(request).await,
            "No endpoint found"
        );
        let result = GetEndpointResponse {
            endpoint: Some(tonic_internal!(ep.try_into(), "Endpoint conversion error")),
        };

        return_with_log!(result);
    }

    async fn get_endpoints(
        &self,
        request: Request<GetEndpointsRequest>,
    ) -> Result<Response<GetEndpointsResponse>> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let ctx = Context::admin();
        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );
        let eps = tonic_internal!(
            self.database_handler.get_endpoints().await,
            "Internal Database error while retrieving endpoints"
        );

        let response = GetEndpointsResponse {
            endpoints: eps
                .into_iter()
                .map(|ep| -> Result<Endpoint, Status> {
                    ep.try_into()
                        .map_err(|_| Status::internal("Endpoint conversion error"))
                })
                .collect::<Result<Vec<Endpoint>, Status>>()?,
        };
        return_with_log!(response);
    }

    async fn delete_endpoint(
        &self,
        request: Request<DeleteEndpointRequest>,
    ) -> Result<Response<DeleteEndpointResponse>> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = DeleteEP(request.into_inner());
        let ctx = Context::admin();
        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        tonic_internal!(
            self.database_handler.delete_endpoint(request).await,
            "Delete endpoint error"
        );
        return_with_log!(DeleteEndpointResponse {});
    }

    async fn get_default_endpoint(
        &self,
        request: Request<GetDefaultEndpointRequest>,
    ) -> Result<Response<GetDefaultEndpointResponse>> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let ctx = Context::admin();
        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let default = tonic_internal!(
            self.database_handler.get_default_endpoint().await,
            "Default endpoint not found"
        );
        let response = GetDefaultEndpointResponse {
            endpoint: Some(tonic_internal!(
                default.try_into(),
                "Endpoint conversion error"
            )),
        };
        return_with_log!(response);
    }
}
