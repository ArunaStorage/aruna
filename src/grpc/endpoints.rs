use crate::auth::permission_handler::PermissionHandler;
use crate::auth::structs::Context;
use crate::caching::cache::Cache;
use crate::caching::structs::PubKeyEnum;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::endpoints_request_types::{CreateEP, DeleteEP, GetEP};
use crate::utils::conversions::get_token_from_md;
use anyhow::bail;
use aruna_rust_api::api::storage::models::v2::Endpoint;
use aruna_rust_api::api::storage::services::v2::endpoint_service_server::EndpointService;
use aruna_rust_api::api::storage::services::v2::{
    get_endpoint_request, CreateEndpointRequest, CreateEndpointResponse, DeleteEndpointRequest,
    DeleteEndpointResponse, FullSyncEndpointRequest, FullSyncEndpointResponse,
    GetDefaultEndpointRequest, GetDefaultEndpointResponse, GetEndpointRequest, GetEndpointResponse,
    GetEndpointsRequest, GetEndpointsResponse, SetEndpointStatusRequest, SetEndpointStatusResponse,
};
use jsonwebtoken::DecodingKey;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Result, Status};

crate::impl_grpc_server!(EndpointServiceImpl, default_endpoint: String);

#[tonic::async_trait]
impl EndpointService for EndpointServiceImpl {
    type FullSyncEndpointStream = ReceiverStream<Result<FullSyncEndpointResponse, Status>>;

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

        let key = if request.0.pubkey.starts_with("-----BEGIN PUBLIC KEY-----") {
            tonic_invalid!(
                DecodingKey::from_ed_pem(request.0.pubkey.as_bytes()),
                "Invalid pubkey"
            )
        } else {
            let public_pem = format!(
                "-----BEGIN PUBLIC KEY-----{}-----END PUBLIC KEY-----",
                &request.0.pubkey
            );
            tonic_invalid!(
                DecodingKey::from_ed_pem(public_pem.as_bytes()),
                "Invalid pubkey"
            )
        };

        let (ep, pk) = tonic_invalid!(
            self.database_handler.create_endpoint(request).await,
            "Invalid create endpoint request"
        );

        self.cache
            .add_pubkey(pk.id, PubKeyEnum::DataProxy((pk.pubkey, key, ep.id)));

        let result = CreateEndpointResponse {
            endpoint: Some(ep.into()),
        };

        return_with_log!(result);
    }

    async fn full_sync_endpoint(
        &self,
        request: Request<FullSyncEndpointRequest>,
    ) -> Result<Response<Self::FullSyncEndpointStream>> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let user = tonic_auth!(
            self.authorizer
                .check_permissions(&token, vec![Context::proxy()])
                .await,
            "Unauthorized"
        );

        // Create multi-producer single-consumer channel
        let (tx, rx) = mpsc::channel(4);

        // Send messages in batches if present
        let cache_clone = self.cache.clone();
        tokio::spawn(async move {
            for item in cache_clone.get_proxy_cache_iterator(&user) {
                match tx.send(Ok(item.into())).await {
                    Ok(_) => {
                        log::info!("Successfully send stream response")
                    }
                    Err(err) => {
                        bail!("Error while sending stream response: {}", err)
                    }
                };
            }
            Ok(())
        });

        // Create gRPC response
        let grpc_response: Response<
            ReceiverStream<std::result::Result<FullSyncEndpointResponse, Status>>,
        > = Response::new(ReceiverStream::new(rx));

        // Log some stuff and return response
        log::info!("Sending GetEventMessageBatchStreamStreamResponse back to client.");
        log::debug!("{:?}", &grpc_response);
        return Ok(grpc_response);
    }

    async fn get_endpoint(
        &self,
        request: Request<GetEndpointRequest>,
    ) -> Result<Response<GetEndpointResponse>> {
        log_received!(&request);

        // Consumer gRPC request into its parts
        let (request_metadata, _, inner_request) = request.into_parts();

        // Check if provided token authorizes with global admin permissions
        let is_admin = if request_metadata.get("Authorization").is_some() {
            // Extract token and check permissions with empty context
            let token = tonic_auth!(
                get_token_from_md(&request_metadata),
                "Token extraction failed"
            );

            // Check if provided token athorizes for global admin permissions
            let ctx = Context::admin();
            self.authorizer
                .check_permissions(&token, vec![ctx])
                .await
                .is_ok()
        } else {
            false
        };

        // Fetch endpoint from database
        let request = GetEP(inner_request);
        let ep = tonic_invalid!(
            self.database_handler.get_endpoint(request).await,
            "No endpoint found"
        );

        if !ep.is_public && !is_admin {
            return Err(tonic::Status::unauthenticated(
                "Privat endpoint info can only be fetched by administrators",
            ));
        }

        let result = GetEndpointResponse {
            endpoint: Some(ep.into()),
        };

        return_with_log!(result);
    }

    async fn get_endpoints(
        &self,
        request: Request<GetEndpointsRequest>,
    ) -> Result<Response<GetEndpointsResponse>> {
        log_received!(&request);

        // Consumer gRPC request into its parts
        let (request_metadata, _, _) = request.into_parts();

        let is_admin = if request_metadata.get("Authorization").is_some() {
            // Extract token and check permissions with empty context
            let token = tonic_auth!(
                get_token_from_md(&request_metadata),
                "Token extraction failed"
            );

            // Check if provided token athorizes for global admin permissions
            let ctx = Context::admin();
            self.authorizer
                .check_permissions(&token, vec![ctx])
                .await
                .is_ok()
        } else {
            false
        };

        // Fetch endpoints and filter if necessary
        let eps = tonic_internal!(
            self.database_handler.get_endpoints().await,
            "Internal Database error while retrieving endpoints"
        );

        let eps = if is_admin {
            eps
        } else {
            eps.into_iter()
                .filter_map(|e| match e.is_public {
                    true => Some(e),
                    false => None,
                })
                .collect::<Vec<_>>()
        };

        let response = GetEndpointsResponse {
            endpoints: eps
                .into_iter()
                .map(|ep| -> Endpoint { ep.into() })
                .collect::<Vec<Endpoint>>(),
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
            self.database_handler
                .get_endpoint(GetEP(GetEndpointRequest {
                    endpoint: Some(get_endpoint_request::Endpoint::EndpointId(
                        self.default_endpoint.to_string()
                    ))
                }))
                .await,
            "Default endpoint not found"
        );
        let response = GetDefaultEndpointResponse {
            endpoint: Some(default.into()),
        };
        return_with_log!(response);
    }
    async fn set_endpoint_status(
        &self,
        _request: Request<SetEndpointStatusRequest>,
    ) -> Result<Response<SetEndpointStatusResponse>> {
        Err(tonic::Status::unimplemented(
            "SetEndpointStatus is currently not implemented",
        ))
    }
}
