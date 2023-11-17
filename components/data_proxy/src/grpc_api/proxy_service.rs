use crate::{
    caching::{auth::get_token_from_md, cache::Cache},
    helpers::sign_download_url,
    replication::replication_handler::ReplicationMessage,
    trace_err,
};
use aruna_rust_api::api::dataproxy::services::v2::{
    dataproxy_service_server::DataproxyService, PullReplicationRequest, PullReplicationResponse,
    PushReplicationRequest, PushReplicationResponse,
};
use async_channel::Sender;
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use std::{str::FromStr, sync::Arc};
use tracing::{error, trace};

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
    #[tracing::instrument(level = "trace", skip(self, request))]
    async fn pull_replication(
        &self,
        request: tonic::Request<PullReplicationRequest>,
    ) -> Result<tonic::Response<PullReplicationResponse>, tonic::Status> {
        let (metadata, _, request) = request.into_parts();
        let ids = trace_err!(request
            .object_ids
            .iter()
            .map(|id| DieselUlid::from_str(id).map_err(|e| {
                trace!("{e}: Invalid id");
                tonic::Status::invalid_argument("Invalid id provided")
            }))
            .collect::<Result<Vec<DieselUlid>, tonic::Status>>())?;

        // 1. get all objects & endpoints from server
        let mut objects = Vec::new();
        let object_endpoint_map = DashMap::new();
        for id in ids {
            if let Some(o) = self.cache.resources.get(&id) {
                let (_, (object, location)) = o.pair();
                objects.push((object.clone(), location.clone()));
                object_endpoint_map.insert(object.id, object.endpoints.clone());
            }
        }

        // 2. check if proxy has permissions to pull everything
        let url = if let Some(auth) = self.cache.auth.read().await.as_ref() {
            let token = trace_err!(get_token_from_md(&metadata))
                .map_err(|e| tonic::Status::unauthenticated(e.to_string()))?;
            // Returns claims.sub as id -> Can return UserIds or DataproxyIds
            // -> UserIds cannot be found in object.endpoints, so this should be safe
            let (dataproxy_id, _) = trace_err!(auth.check_permissions(&token))
                .map_err(|_| tonic::Status::unauthenticated("DataProxy not authenticated"))?;
            if !object_endpoint_map.iter().all(|map| {
                let (_, eps) = map.pair();
                eps.iter().find(|ep| ep.id == dataproxy_id).is_some()
            }) {
                error!("Unauthorized DataProxy request");
                return Err(tonic::Status::unauthenticated(
                    "DataProxy is not allowed to access requested objects",
                ));
            };

            // TODO
            // 3. sign download url
            // - With this dataproxys access & secret keys, scoped and timed
            //sign_download_url(
            //    auth.self_id.to_string().as_str(),
            //    &auth.self_secret,
            //    true,
            //    "objects",
            //    key,
            //    endpoint,
            //);
        } else {
            error!("authentication handler not available");
            return Err(tonic::Status::unauthenticated(
                "Unable to authenticate user",
            ));
        };

        // TODO
        // 4. Return DataInfos
        error!("RequestReplication not implemented");
        Err(tonic::Status::unimplemented("Currently not implemented"))
    }

    /// PushReplication
    ///
    /// Status: BETA
    ///
    /// Provides the necessary url to init replication
    #[tracing::instrument(level = "trace", skip(self, _request))]
    async fn push_replication(
        &self,
        _request: tonic::Request<PushReplicationRequest>,
    ) -> Result<tonic::Response<PushReplicationResponse>, tonic::Status> {
        // TODO
        // 1. query permissions
        // 2. validate endpoint that tries sending these
        // 3. validate if i need these objects
        // 4. send message to replication handler with DataInfos
        error!("InitReplication not implemented");
        Err(tonic::Status::unimplemented("Currently not implemented"))
    }
}
