use crate::{
    auth::auth_helpers::get_token_from_md,
    caching::cache::Cache,
    helpers::sign_download_url,
    structs::{DbPermissionLevel, Endpoint, Object, ObjectType, ALL_RIGHTS_RESERVED},
};
use aruna_rust_api::api::{
    dataproxy::services::v2::{
        bundler_service_server::BundlerService, CreateBundleRequest, CreateBundleResponse,
        DeleteBundleRequest, DeleteBundleResponse,
    },
    storage::models::v2::{DataClass, KeyValue, KeyValueVariant, Status},
};
use diesel_ulid::DieselUlid;
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::Arc,
};
use tracing::{error, trace};

pub struct BundlerServiceImpl {
    pub cache: Arc<Cache>,
    pub endpoint_url: String,
    pub ssl: bool,
}

impl BundlerServiceImpl {
    #[tracing::instrument(level = "trace", skip(cache))]
    pub fn new(cache: Arc<Cache>, endpoint_url: String, ssl: bool) -> Self {
        Self {
            cache,
            endpoint_url,
            ssl,
        }
    }
}

#[tonic::async_trait]
impl BundlerService for BundlerServiceImpl {
    #[tracing::instrument(level = "trace", skip(self, request))]
    async fn create_bundle(
        &self,
        request: tonic::Request<CreateBundleRequest>,
    ) -> Result<tonic::Response<CreateBundleResponse>, tonic::Status> {
        let (trels, access_key, secret_key) = if let Some(a) = self.cache.auth.read().await.as_ref()
        {
            let token = get_token_from_md(request.metadata()).map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                tonic::Status::unauthenticated(e.to_string())
            })?;

            let (u, tid) = a.check_permissions(&token).map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                tonic::Status::unauthenticated(format!("Unable to authenticate user: {}", e))
            })?;
            let access_key = if let Some(t_id) = tid {
                t_id
            } else {
                u.to_string()
            };
            let mut check_vec = Vec::new();
            let mut trels = Vec::new();
            for id in request.get_ref().resource_ids.iter() {
                let ulid = DieselUlid::from_str(id.as_str()).map_err(|_| {
                    error!(id = id.as_str(), msg = "Unable to parse ULID");
                    tonic::Status::invalid_argument("Unable to parse ULID")
                })?;

                let (ids, trel) = self.cache.get_resource_ids_from_id(ulid).map_err(|_| {
                    error!(id = id.as_str(), msg = "Unable to parse ULID");
                    tonic::Status::invalid_argument("Unable to parse ULID")
                })?;
                check_vec.push(ids);
                trels.push(trel);
            }
            let secret_key = a
                .check_ids(&check_vec, &access_key, DbPermissionLevel::Write, true)
                .map_err(|_| {
                    error!(id = ?check_vec, msg = "Unable to authenticate user");
                    tonic::Status::unauthenticated("Unable to authenticate user")
                })?
                .ok_or_else(|| {
                    error!(id = ?check_vec, msg = "Unable to authenticate user");
                    tonic::Status::unauthenticated("Unable to authenticate user")
                })?;

            (trels, access_key, secret_key)
        } else {
            error!("Unable to authenticate user");
            return Err(tonic::Status::unauthenticated(
                "Unable to authenticate user",
            ));
        };

        let request = request.into_inner();

        let kvs = if let Some(expires) = request.expires_at {
            vec![KeyValue {
                key: "app.aruna-storage.org/expires_at".to_string(),
                value: expires.to_string(),
                variant: KeyValueVariant::Label as i32,
            }]
        } else {
            Vec::new()
        };

        let bundle_id = DieselUlid::generate();
        let id = if let Some(auth) = self.cache.auth.read().await.as_ref() {
            auth.self_id
        } else {
            trace!("AuthorizationHandler could not provide self_id");
            return Err(tonic::Status::internal("Internal conversion error"));
        };
        let ep = Endpoint {
            id,
            variant: crate::structs::SyncVariant::PartialSync(false),
            status: None,
        };
        let bundler_object = Object {
            id: bundle_id,
            name: request.filename.clone(),
            key_values: kvs,
            object_status: Status::Available,
            data_class: DataClass::Workspace,
            object_type: ObjectType::Bundle,
            hashes: HashMap::default(),
            metadata_license: ALL_RIGHTS_RESERVED.to_string(), // Default for now
            data_license: ALL_RIGHTS_RESERVED.to_string(),     // Default for now
            dynamic: false,
            children: Some(HashSet::from_iter(trels)),
            parents: None,
            synced: true,
            endpoints: vec![ep],
            created_at: Some(chrono::Utc::now().naive_utc()), // Now for default
        };

        self.cache
            .upsert_object(bundler_object, None)
            .await
            .map_err(|_| {
                error!(error = "Bundle object upsert failed");
                tonic::Status::internal("Bundle object upsert failed")
            })?;

        let response = CreateBundleResponse {
            bundle_url: sign_download_url(
                &access_key,
                &secret_key,
                self.ssl,
                "bundles",
                &format!("{}/{}", &bundle_id.to_string(), request.filename),
                self.endpoint_url.as_str(),
            )
            .map_err(|_| {
                error!(error = "Failed to presign bundle download url");
                tonic::Status::internal("Failed to presign bundle download url")
            })?,
            bundle_id: bundle_id.to_string(),
        };

        Ok(tonic::Response::new(response))
    }

    #[tracing::instrument(level = "trace", skip(self, request))]
    async fn delete_bundle(
        &self,
        request: tonic::Request<DeleteBundleRequest>,
    ) -> Result<tonic::Response<DeleteBundleResponse>, tonic::Status> {
        if let Some(a) = self.cache.auth.read().await.as_ref() {
            let token = get_token_from_md(request.metadata()).map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                tonic::Status::unauthenticated(e.to_string())
            })?;

            let (u, tid) = a.check_permissions(&token).map_err(|_| {
                error!(error = "Unable to authenticate user");
                tonic::Status::unauthenticated("Unable to authenticate user")
            })?;

            let access_key = if let Some(t_id) = tid {
                t_id
            } else {
                u.to_string()
            };

            let user = self
                .cache
                .get_access_key(&access_key)
                .ok_or_else(|| tonic::Status::unauthenticated("Unable to authenticate user"))?;

            let bundle_id =
                DieselUlid::from_str(request.get_ref().bundle_id.as_str()).map_err(|_| {
                    error!(error = "Unable to parse BundleID");
                    tonic::Status::invalid_argument("Unable to parse BundleID")
                })?;

            if let Some(perm) = user.value().permissions.get(&bundle_id) {
                if *perm == DbPermissionLevel::Admin {
                    self.cache.delete_object(bundle_id).await.map_err(|_| {
                        error!(error = "Bundle deletion failed");
                        tonic::Status::internal("Bundle deletion failed")
                    })?;
                    return Ok(tonic::Response::new(DeleteBundleResponse {}));
                }
            }
        }

        error!("Unable to authenticate user");
        return Err(tonic::Status::unauthenticated(
            "Unable to authenticate user",
        ));
    }
}
