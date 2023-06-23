use crate::server::services::authz::Authz;
use crate::server::services::utils::{format_grpc_request, format_grpc_response};
use crate::{database::connection::Database, error::ArunaError};
use aruna_rust_api::api::internal::v1::{
    internal_bundler_backchannel_service_server::InternalBundlerBackchannelService,
    GetBundlesRequest, GetBundlesResponse,
};
use std::sync::Arc;
use tokio::task;

// This macro automatically creates the Impl struct with all associated fields
crate::impl_grpc_server!(InternalBundlerBackwardsChannelImpl);

#[tonic::async_trait]
impl InternalBundlerBackchannelService for InternalBundlerBackwardsChannelImpl {
    async fn get_bundles(
        &self,
        request: tonic::Request<GetBundlesRequest>,
    ) -> std::result::Result<tonic::Response<GetBundlesResponse>, tonic::Status> {
        log::info!("Received GetBundlesRequest.");
        log::debug!("{}", format_grpc_request(&request));
        let database_clone = self.database.clone();
        let response = tonic::Response::new(
            task::spawn_blocking(move || database_clone.get_all_bundles(request.into_inner()))
                .await
                .map_err(ArunaError::from)??,
        );
        log::info!("Sending GetBundlesRequest back to client.");
        log::debug!("{}", format_grpc_response(&response));
        Ok(response)
    }
}
