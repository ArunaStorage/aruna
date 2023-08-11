use std::sync::{Arc, RwLock};

use aruna_rust_api::api::dataproxy::services::v2::{
    dataproxy_user_service_server::DataproxyUserService, GetCredentialsRequest,
    GetCredentialsResponse, PullReplicaRequest, PullReplicaResponse, PushReplicaRequest,
    PushReplicaResponse, ReplicationStatusRequest, ReplicationStatusResponse,
};

use crate::caching::cache::Cache;

pub struct DataProxyUserService {
    pub cache: Arc<RwLock<Cache>>,
}

impl DataProxyUserService {
    pub fn new(cache: Arc<RwLock<Cache>>) -> Self {
        Self { cache }
    }
}

#[tonic::async_trait]
impl DataproxyUserService for DataProxyUserService {
    /// GetCredentials
    ///
    /// Status: BETA
    ///
    /// Authorized method that needs a aruna-token to exchange for dataproxy
    /// specific S3AccessKey and S3SecretKey
    async fn get_credentials(
        &self,
        _request: tonic::Request<GetCredentialsRequest>,
    ) -> std::result::Result<tonic::Response<GetCredentialsResponse>, tonic::Status> {
        todo!()
    }

    /// PushReplica
    ///
    /// Status: BETA
    ///
    /// Manually transfers a replica to another data-proxy
    async fn push_replica(
        &self,
        _request: tonic::Request<PushReplicaRequest>,
    ) -> std::result::Result<tonic::Response<PushReplicaResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented"))
    }

    /// PullReplica
    ///
    /// Status: BETA
    ///
    /// Manually request data to be transferred to this data-proxy
    async fn pull_replica(
        &self,
        _request: tonic::Request<PullReplicaRequest>,
    ) -> std::result::Result<tonic::Response<PullReplicaResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented"))
    }

    /// PullReplica
    ///
    /// Status: BETA
    ///
    /// Status of the previous replication request
    async fn replication_status(
        &self,
        _request: tonic::Request<ReplicationStatusRequest>,
    ) -> std::result::Result<tonic::Response<ReplicationStatusResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented"))
    }
}
