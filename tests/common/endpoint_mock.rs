use super::test_utils::rand_string;
use anyhow::Result;
use aruna_rust_api::api::dataproxy::services::v2::{
    dataproxy_user_service_server::{DataproxyUserService, DataproxyUserServiceServer},
    GetCredentialsRequest, GetCredentialsResponse, PullReplicaRequest, PullReplicaResponse,
    PushReplicaRequest, PushReplicaResponse, ReplicationStatusRequest, ReplicationStatusResponse,
};
use std::net::SocketAddr;
use tokio::task::AbortHandle;
use tonic::transport::Server;

#[allow(dead_code)]
pub async fn start_server(address: SocketAddr) -> Result<AbortHandle> {
    let task = tokio::spawn(async move {
        Server::builder()
            .add_service(DataproxyUserServiceServer::new(DataProxyServiceImpl::new()))
            .serve(address)
            .await
    });
    let abort_handle = task.abort_handle();
    Ok(abort_handle)
}

pub struct DataProxyServiceImpl {}

#[allow(dead_code)]
impl Default for DataProxyServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

impl DataProxyServiceImpl {
    pub fn new() -> Self {
        DataProxyServiceImpl {}
    }
}

#[tonic::async_trait]
impl DataproxyUserService for DataProxyServiceImpl {
    async fn get_credentials(
        &self,
        _request: tonic::Request<GetCredentialsRequest>,
    ) -> Result<tonic::Response<GetCredentialsResponse>, tonic::Status> {
        let access_key = rand_string(32);
        let secret_key = rand_string(32);
        Ok(tonic::Response::new(GetCredentialsResponse {
            access_key,
            secret_key,
        }))
    }
    async fn push_replica(
        &self,
        _request: tonic::Request<PushReplicaRequest>,
    ) -> Result<tonic::Response<PushReplicaResponse>, tonic::Status> {
        todo!()
    }
    async fn pull_replica(
        &self,
        _request: tonic::Request<PullReplicaRequest>,
    ) -> Result<tonic::Response<PullReplicaResponse>, tonic::Status> {
        todo!()
    }
    async fn replication_status(
        &self,
        _request: tonic::Request<ReplicationStatusRequest>,
    ) -> Result<tonic::Response<ReplicationStatusResponse>, tonic::Status> {
        todo!()
    }
}
