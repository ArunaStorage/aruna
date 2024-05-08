use crate::auth::auth_helpers::get_token_from_md;
use crate::caching::cache::Cache;
use aruna_rust_api::api::dataproxy::services::v2::{
    dataproxy_user_service_server::DataproxyUserService, CreateOrUpdateCredentialsRequest,
    CreateOrUpdateCredentialsResponse, GetCredentialsRequest, GetCredentialsResponse,
    PullReplicaRequest, PullReplicaResponse, PushReplicaRequest, PushReplicaResponse,
    ReplicationStatusRequest, ReplicationStatusResponse, RevokeCredentialsRequest,
    RevokeCredentialsResponse,
};
use std::sync::Arc;
use tracing::error;

pub struct DataproxyUserServiceImpl {
    pub cache: Arc<Cache>,
}

impl DataproxyUserServiceImpl {
    #[tracing::instrument(level = "trace", skip(cache))]
    pub fn new(cache: Arc<Cache>) -> Self {
        Self { cache }
    }
}

#[tonic::async_trait]
impl DataproxyUserService for DataproxyUserServiceImpl {
    #[tracing::instrument(level = "trace", skip(self, request))]
    /// GetCredentials
    ///
    /// Status: BETA
    ///
    /// Authorized method that needs a aruna-token to exchange for dataproxy
    /// specific S3AccessKey and S3SecretKey

    // TODO: UPDATE to two requests one for get and one for create
    async fn get_credentials(
        &self,
        request: tonic::Request<GetCredentialsRequest>,
    ) -> Result<tonic::Response<GetCredentialsResponse>, tonic::Status> {
        return if let Some(a) = self.cache.auth.read().await.as_ref() {
            let token = get_token_from_md(request.metadata()).map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                tonic::Status::unauthenticated(e.to_string())
            })?;

            let (u, tid, pk) = a.check_permissions(&token).map_err(|_| {
                error!(error = "Unable to authenticate user");
                tonic::Status::unauthenticated("Unable to authenticate user")
            })?;

            if pk.is_proxy {
                error!(error = "Proxy token is not allowed");
                return Err(tonic::Status::unauthenticated("Proxy token is not allowed"));
            }

            if let Some(q_handler) = self.cache.aruna_client.read().await.as_ref() {
                let user = q_handler.get_user(u, "".to_string()).await.map_err(|_| {
                    error!(error = "Unable to authenticate user");
                    tonic::Status::unauthenticated("Unable to authenticate user")
                })?;

                let access_key = tid.unwrap_or_else(|| user.id.to_string());
                let secret_key =
                    self.cache
                        .clone()
                        .get_secret(&access_key)
                        .await
                        .map_err(|_| {
                            error!(error = "Unable to authenticate user");
                            tonic::Status::unauthenticated("Unable to authenticate user")
                        })?;

                Ok(tonic::Response::new(GetCredentialsResponse {
                    access_key,
                    secret_key: secret_key.expose().to_string(),
                }))
            } else {
                error!("query handler not available");
                Err(tonic::Status::unauthenticated(
                    "Unable to authenticate user",
                ))
            }
        } else {
            error!("authentication handler not available");
            Err(tonic::Status::unauthenticated(
                "Unable to authenticate user",
            ))
        };
    }

    /// RevokeCredentials
    ///
    /// Status: BETA
    ///
    /// Authorized method that needs a aruna-token
    /// Revokes the current credentials
    async fn revoke_credentials(
        &self,
        request: tonic::Request<RevokeCredentialsRequest>,
    ) -> Result<tonic::Response<RevokeCredentialsResponse>, tonic::Status> {
        return if let Some(a) = self.cache.auth.read().await.as_ref() {
            let token = get_token_from_md(request.metadata()).map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                tonic::Status::unauthenticated(e.to_string())
            })?;

            let (u, tid, pk) = a.check_permissions(&token).map_err(|_| {
                error!(error = "Unable to authenticate user");
                tonic::Status::unauthenticated("Unable to authenticate user")
            })?;

            if pk.is_proxy {
                error!(error = "Proxy token is not allowed");
                return Err(tonic::Status::unauthenticated("Proxy token is not allowed"));
            }

            if let Some(q_handler) = self.cache.aruna_client.read().await.as_ref() {
                let user = q_handler.get_user(u, "".to_string()).await.map_err(|_| {
                    error!(error = "Unable to authenticate user");
                    tonic::Status::unauthenticated("Unable to authenticate user")
                })?;

                let access_key = tid.unwrap_or_else(|| user.id.to_string());

                self.cache.revoke_secret(&access_key).await.map_err(|_| {
                    error!(error = "Unable to authenticate user");
                    tonic::Status::unauthenticated("Unable to authenticate user")
                })?;

                Ok(tonic::Response::new(RevokeCredentialsResponse {}))
            } else {
                error!("query handler not available");
                Err(tonic::Status::unauthenticated(
                    "Unable to authenticate user",
                ))
            }
        } else {
            error!("authentication handler not available");
            Err(tonic::Status::unauthenticated(
                "Unable to authenticate user",
            ))
        };
    }

    /// CreateOrUpdateCredentials
    ///
    /// Status: BETA
    ///
    /// Authorized method that needs a aruna-token to exchange for dataproxy
    /// specific S3AccessKey and S3SecretKey
    async fn create_or_update_credentials(
        &self,
        request: tonic::Request<CreateOrUpdateCredentialsRequest>,
    ) -> Result<tonic::Response<CreateOrUpdateCredentialsResponse>, tonic::Status> {
        return if let Some(a) = self.cache.auth.read().await.as_ref() {
            let token = get_token_from_md(request.metadata()).map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                tonic::Status::unauthenticated(e.to_string())
            })?;

            let (u, tid, pk) = a.check_permissions(&token).map_err(|_| {
                error!(error = "Unable to authenticate user, check permissions");
                tonic::Status::unauthenticated("Unable to authenticate user")
            })?;

            if pk.is_proxy {
                error!(error = "Proxy token is not allowed");
                return Err(tonic::Status::unauthenticated("Proxy token is not allowed"));
            }

            if let Some(q_handler) = self.cache.aruna_client.read().await.as_ref() {
                let user = q_handler.get_user(u, "".to_string()).await.map_err(|_| {
                    error!(error = "Unable to authenticate user, get user grpc");
                    tonic::Status::unauthenticated("Unable to authenticate user")
                })?;

                let access_key = tid.unwrap_or_else(|| user.id.to_string());

                let (access, secret) = self
                    .cache
                    .create_or_update_secret(&access_key, &u)
                    .await
                    .map_err(|_| {
                        error!(error = "Unable to authenticate user, create or update secret grpc");
                        tonic::Status::unauthenticated("Unable to authenticate user")
                    })?;

                Ok(tonic::Response::new(CreateOrUpdateCredentialsResponse {
                    access_key: access,
                    secret_key: secret,
                }))
            } else {
                error!("query handler not available");
                Err(tonic::Status::unauthenticated(
                    "Unable to authenticate user",
                ))
            }
        } else {
            error!("authentication handler not available");
            Err(tonic::Status::unauthenticated(
                "Unable to authenticate user",
            ))
        };
    }

    #[tracing::instrument(level = "trace", skip(self, _request))]
    /// PushReplica
    ///
    /// Status: BETA
    ///
    /// Manually transfers a replica to another data-proxy
    async fn push_replica(
        &self,
        _request: tonic::Request<PushReplicaRequest>,
    ) -> Result<tonic::Response<PushReplicaResponse>, tonic::Status> {
        error!("PushReplica not implemented");
        Err(tonic::Status::unimplemented("Not implemented"))
    }

    #[tracing::instrument(level = "trace", skip(self, _request))]
    /// PullReplica
    ///
    /// Status: BETA
    ///
    /// Manually request data to be transferred to this data-proxy
    async fn pull_replica(
        &self,
        _request: tonic::Request<PullReplicaRequest>,
    ) -> Result<tonic::Response<PullReplicaResponse>, tonic::Status> {
        error!("PullReplica not implemented");
        Err(tonic::Status::unimplemented("Not implemented"))
    }

    #[tracing::instrument(level = "trace", skip(self, _request))]
    /// PullReplica
    ///
    /// Status: BETA
    ///
    /// Status of the previous replication request
    async fn replication_status(
        &self,
        _request: tonic::Request<ReplicationStatusRequest>,
    ) -> Result<tonic::Response<ReplicationStatusResponse>, tonic::Status> {
        error!("ReplicationStatus not implemented");
        Err(tonic::Status::unimplemented("Not implemented"))
    }
}
