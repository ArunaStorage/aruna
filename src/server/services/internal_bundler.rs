use crate::database::connection::Database;
use crate::server::services::authz::Authz;
use aruna_rust_api::api::internal::v1::{
    internal_bundler_backchannel_service_server::InternalBundlerBackchannelService,
    GetBundlesRequest, GetBundlesResponse,
};
use std::sync::Arc;

// This macro automatically creates the Impl struct with all associated fields
crate::impl_grpc_server!(InternalBundlerBackwardsChannelImpl);

#[tonic::async_trait]
impl InternalBundlerBackchannelService for InternalBundlerBackwardsChannelImpl {
    async fn get_bundles(
        &self,
        _request: tonic::Request<GetBundlesRequest>,
    ) -> std::result::Result<tonic::Response<GetBundlesResponse>, tonic::Status> {
        todo!();
    }
}
