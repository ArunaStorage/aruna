use crate::caching::{auth::get_token_from_md, cache::Cache};
use aruna_rust_api::api::dataproxy::services::v2::{
    dataproxy_user_service_server::DataproxyUserService, GetCredentialsRequest,
    GetCredentialsResponse, PullReplicaRequest, PullReplicaResponse, PushReplicaRequest,
    PushReplicaResponse, ReplicationStatusRequest, ReplicationStatusResponse,
};
use std::sync::Arc;

pub struct DataProxyUserService {
    pub cache: Arc<Cache>,
}

impl DataProxyUserService {
    pub fn new(cache: Arc<Cache>) -> Self {
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
        request: tonic::Request<GetCredentialsRequest>,
    ) -> Result<tonic::Response<GetCredentialsResponse>, tonic::Status> {
        if let Some(a) = self.cache.auth.read().await.as_ref() {
            let token = get_token_from_md(request.metadata())
                .map_err(|e| tonic::Status::unauthenticated(e.to_string()))?;

            let (u, tid) = a.check_permissions(&token).map_err(|e| {
                log::debug!("Error checking permissions: {}", e);
                tonic::Status::unauthenticated("Unable to authenticate user")
            })?;

            if let Some(q_handler) = self.cache.aruna_client.read().await.as_ref() {
                let user = q_handler.get_user(u, "".to_string()).await.map_err(|e| {
                    log::debug!("Error getting user from queue handler {e}");
                    tonic::Status::unauthenticated("Unable to authenticate user")
                })?;

                let (access_key, secret_key) = self
                    .cache
                    .clone()
                    .create_secret(user, tid)
                    .await
                    .map_err(|e| {
                        log::debug!("Error creating secret: {}", e);
                        tonic::Status::unauthenticated("Unable to authenticate user")
                    })?;

                return Ok(tonic::Response::new(GetCredentialsResponse {
                    access_key,
                    secret_key,
                }));
            };
        } else {
            return Err(tonic::Status::unauthenticated(
                "Unable to authenticate user",
            ));
        }

        Err(tonic::Status::unimplemented("Not implemented"))
    }

    /// PushReplica
    ///
    /// Status: BETA
    ///
    /// Manually transfers a replica to another data-proxy
    async fn push_replica(
        &self,
        _request: tonic::Request<PushReplicaRequest>,
    ) -> Result<tonic::Response<PushReplicaResponse>, tonic::Status> {
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
    ) -> Result<tonic::Response<PullReplicaResponse>, tonic::Status> {
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
    ) -> Result<tonic::Response<ReplicationStatusResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not implemented"))
    }
}
