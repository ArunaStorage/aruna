use crate::{
    caching::{auth::get_token_from_md, cache::Cache},
    helpers::sign_download_url,
    replication::replication_handler::ReplicationMessage,
    structs::{Endpoint, Object, ObjectType, Origin, TypedRelation, ALL_RIGHTS_RESERVED},
    trace_err,
};
use aruna_rust_api::api::{
    dataproxy::services::v2::{
        dataproxy_service_server::DataproxyService, DataInfo, DataInfos, PullReplicationRequest,
        PullReplicationResponse, PushReplicationRequest, PushReplicationResponse,
    },
    storage::models::v2::{DataClass, Status},
};
use async_channel::Sender;
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use std::collections::HashMap;
use std::{str::FromStr, sync::Arc};
use tracing::{error, trace};

pub struct DataproxyServiceImpl {
    pub cache: Arc<Cache>,
    pub sender: Sender<ReplicationMessage>,
    pub endpoint_url: String,
}

impl DataproxyServiceImpl {
    #[tracing::instrument(level = "trace", skip(cache))]
    pub fn new(
        cache: Arc<Cache>,
        sender: Sender<ReplicationMessage>,
        endpoint_url: String,
    ) -> Self {
        Self {
            cache,
            sender,
            endpoint_url,
        }
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
        trace!("EndpointMap: {:?}", object_endpoint_map);

        // 2. check if proxy has permissions to pull everything
        let (self_id, secret_key) = if let Some(auth) = self.cache.auth.read().await.as_ref() {
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
            (auth.self_id, auth.self_secret.clone())
        } else {
            error!("authentication handler not available");
            return Err(tonic::Status::unauthenticated(
                "Unable to authenticate user",
            ));
        };

        // 3. Create bundle
        let bundle_id = DieselUlid::generate();
        let ep = Endpoint {
            id: self_id,
            variant: crate::structs::SyncVariant::PartialSync(Origin::BundleId(bundle_id)),
            status: None,
        };
        let children = Some(
            objects
                .iter()
                .filter_map(|(o, _)| match o.object_type {
                    ObjectType::Bundle => None,
                    ObjectType::Project => Some(TypedRelation::Project(o.id)),
                    ObjectType::Collection => Some(TypedRelation::Collection(o.id)),
                    ObjectType::Dataset => Some(TypedRelation::Dataset(o.id)),
                    ObjectType::Object => Some(TypedRelation::Object(o.id)),
                })
                .collect(),
        );
        let name = format!("replication_bundle#{bundle_id}");
        let bundler_object = Object {
            id: bundle_id,
            name: name.clone(),
            key_values: Vec::new(),
            object_status: Status::Available,
            data_class: DataClass::Workspace,
            object_type: ObjectType::Bundle,
            hashes: HashMap::default(),
            metadata_license: ALL_RIGHTS_RESERVED.to_string(), // Default for now
            data_license: ALL_RIGHTS_RESERVED.to_string(),     // Default for now
            dynamic: false,
            children,
            parents: None,
            synced: true,
            endpoints: vec![ep],
        };
        let access_key = self_id.to_string();

        // 4. sign download url with dataproxy-user
        let download_url = trace_err!(sign_download_url(
            &access_key,
            &secret_key,
            false, // TODO: This should be dynamic
            "bundles",
            &format!("{}/{}", &bundle_id.to_string(), &name),
            &self.endpoint_url,
        ))
        .map_err(|_| tonic::Status::internal("Failed to presign replication-bundle url"))?;
        trace_err!(self.cache.upsert_object(bundler_object, None).await)
            .map_err(|_| tonic::Status::internal("Replication-bundle upsert failed"))?;

        // 5. Return DataInfos
        Ok(tonic::Response::new(PullReplicationResponse {
            data_infos: Some(DataInfos {
                data_info: vec![DataInfo {
                    object_id: bundle_id.to_string(),
                    download_url,
                    encryption_key: "".to_string(),
                    is_compressed: false,
                }],
            }),
        }))
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
