use super::test_utils::rand_string;
use anyhow::Result;
use aruna_rust_api::api::dataproxy::services::v2::{
    dataproxy_user_service_server::{DataproxyUserService, DataproxyUserServiceServer},
    CreateOrUpdateCredentialsRequest, CreateOrUpdateCredentialsResponse, GetCredentialsRequest,
    GetCredentialsResponse, PullReplicaRequest, PullReplicaResponse, PushReplicaRequest,
    PushReplicaResponse, ReplicationStatusRequest, ReplicationStatusResponse,
    RevokeCredentialsRequest, RevokeCredentialsResponse,
};
use std::net::SocketAddr;
use tokio::task::AbortHandle;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

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
        _request: Request<GetCredentialsRequest>,
    ) -> Result<Response<GetCredentialsResponse>, Status> {
        let access_key = rand_string(32);
        let secret_key = rand_string(32);
        Ok(Response::new(GetCredentialsResponse {
            access_key,
            secret_key,
        }))
    }

    async fn create_or_update_credentials(
        &self,
        _request: Request<CreateOrUpdateCredentialsRequest>,
    ) -> std::result::Result<Response<CreateOrUpdateCredentialsResponse>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn revoke_credentials(
        &self,
        _request: Request<RevokeCredentialsRequest>,
    ) -> std::result::Result<Response<RevokeCredentialsResponse>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn push_replica(
        &self,
        _request: Request<PushReplicaRequest>,
    ) -> Result<Response<PushReplicaResponse>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }
    async fn pull_replica(
        &self,
        _request: Request<PullReplicaRequest>,
    ) -> Result<Response<PullReplicaResponse>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }
    async fn replication_status(
        &self,
        _request: Request<ReplicationStatusRequest>,
    ) -> Result<Response<ReplicationStatusResponse>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }
}
