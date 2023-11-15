use crate::{caching::cache::Cache, replication::replication_handler::ReplicationMessage};
use aruna_rust_api::api::dataproxy::services::v2::{
    dataproxy_service_server::DataproxyService, PullReplicationRequest, PullReplicationResponse,
    PushReplicationRequest, PushReplicationResponse,
};
use async_channel::Sender;
use std::sync::Arc;
use tracing::error;

pub struct DataproxyServiceImpl {
    pub cache: Arc<Cache>,
    pub sender: Sender<ReplicationMessage>,
}

impl DataproxyServiceImpl {
    #[tracing::instrument(level = "trace", skip(cache))]
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
    #[tracing::instrument(level = "trace", skip(self, _request))]
    async fn pull_replication(
        &self,
        _request: tonic::Request<PullReplicationRequest>,
    ) -> Result<tonic::Response<PullReplicationResponse>, tonic::Status> {
        // TODO
        // 1. check if proxy has permissions to pull everything
        // 2. send
        error!("RequestReplication not implemented");
        Err(tonic::Status::unimplemented("Currently not implemented"))
    }

    /// InitReplication
    ///
    /// Status: BETA
    ///
    /// Provides the necessary url to init replication
    #[tracing::instrument(level = "trace", skip(self, _request))]
    async fn push_replication(
        &self,
        _request: tonic::Request<PushReplicationRequest>,
    ) -> Result<tonic::Response<PushReplicationResponse>, tonic::Status> {
        error!("InitReplication not implemented");
        Err(tonic::Status::unimplemented("Currently not implemented"))
    }
}
