use crate::{caching::cache::Cache, replication::replication_handler::ReplicationMessage};
use aruna_rust_api::api::dataproxy::services::v2::{
    dataproxy_service_server::DataproxyService, InitReplicationRequest, InitReplicationResponse,
    RequestReplicationRequest, RequestReplicationResponse,
};
use async_channel::Sender;
use std::sync::Arc;

pub struct DataproxyServiceImpl {
    pub cache: Arc<Cache>,
    pub sender: Sender<ReplicationMessage>,
}

impl DataproxyServiceImpl {
    pub fn new(cache: Arc<Cache>, sender: Sender<ReplicationMessage>) -> Self {
        Self { cache, sender }
    }
}

#[tonic::async_trait]
impl DataproxyService for DataproxyServiceImpl {
    /// PullReplication
    ///
    /// Status: BETA
    ///
    /// Creates a replication request
    async fn request_replication(
        &self,
        _request: tonic::Request<RequestReplicationRequest>,
    ) -> Result<tonic::Response<RequestReplicationResponse>, tonic::Status> {
        // TODO
        // 1. check if proxy has permissions to pull everything
        // 2. send
        Err(tonic::Status::unimplemented("Currently not implemented"))
    }
    /// PushReplication
    ///
    /// Status: BETA
    ///
    /// Provides the necessary url to init replication
    async fn init_replication(
        &self,
        _request: tonic::Request<InitReplicationRequest>,
    ) -> Result<tonic::Response<InitReplicationResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Currently not implemented"))
    }
}
