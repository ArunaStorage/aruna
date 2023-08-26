use crate::{
    caching::{auth::get_token_from_md, cache::Cache},
    helpers::sign_download_url,
    structs::{DbPermissionLevel, Object, ObjectType},
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

pub struct BundlerServiceImpl {
    pub cache: Arc<Cache>,
    pub endpoint_id: String,
    pub ssl: bool,
}

impl BundlerServiceImpl {
    pub fn _new(cache: Arc<Cache>, endpoint_id: String, ssl: bool) -> Self {
        Self {
            cache,
            endpoint_id,
            ssl,
        }
    }
}

#[tonic::async_trait]
impl BundlerService for BundlerServiceImpl {
    async fn create_bundle(
        &self,
        request: tonic::Request<CreateBundleRequest>,
    ) -> std::result::Result<tonic::Response<CreateBundleResponse>, tonic::Status> {
        let (trels, access_key, secret_key) = if let Some(a) = self.cache.auth.read().await.as_ref()
        {
            let token = get_token_from_md(request.metadata())
                .map_err(|e| tonic::Status::unauthenticated(e.to_string()))?;
            let (u, tid) = a.check_permissions(&token).map_err(|e| {
                log::debug!("Error checking permissions: {}", e);
                tonic::Status::unauthenticated("Unable to authenticate user")
            })?;
            let access_key = if let Some(t_id) = tid {
                t_id
            } else {
                u.to_string()
            };
            let mut check_vec = Vec::new();
            let mut trels = Vec::new();
            for id in request.get_ref().resource_id.iter() {
                let ulid = DieselUlid::from_str(id.as_str()).map_err(|e| {
                    log::debug!("Error parsing ULID: {}", e);
                    tonic::Status::invalid_argument("Unable to parse ULID")
                })?;

                let (ids, trel) = self.cache.get_resource_ids_from_id(ulid).map_err(|e| {
                    log::debug!("Error getting resource ids from id: {}", e);
                    tonic::Status::invalid_argument("Unable to parse ULID")
                })?;
                check_vec.push(ids);
                trels.push(trel);
            }
            let secret_key = a
                .check_ids(
                    &check_vec,
                    &access_key,
                    crate::structs::DbPermissionLevel::Write,
                    true,
                )
                .map_err(|e| {
                    log::debug!("Error checking permissions: {}", e);
                    tonic::Status::unauthenticated("Unable to authenticate user")
                })?
                .ok_or_else(|| tonic::Status::unauthenticated("Unable to authenticate user"))?;

            (trels, access_key, secret_key)
        } else {
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

        let bundler_object = Object {
            id: bundle_id,
            name: request.filename,
            key_values: kvs,
            object_status: Status::Available,
            data_class: DataClass::Workspace,
            object_type: ObjectType::Bundle,
            hashes: HashMap::default(),
            dynamic: false,
            children: Some(HashSet::from_iter(trels)),
            parents: None,
            synced: true,
        };

        self.cache
            .upsert_object(bundler_object, None)
            .await
            .map_err(|e| {
                log::debug!("Error upserting object: {}", e);
                tonic::Status::unauthenticated("Unable to authenticate user")
            })?;

        self.cache
            .add_permission_to_access_key(&access_key, (bundle_id, DbPermissionLevel::Admin))
            .await
            .map_err(|e| {
                log::debug!("Error adding permission to access key: {}", e);
                tonic::Status::unauthenticated("Unable to authenticate user")
            })?;

        let response = CreateBundleResponse {
            bundle_url: sign_download_url(
                &access_key,
                &secret_key,
                self.ssl,
                "objects",
                &bundle_id.to_string(),
                self.endpoint_id.as_str(),
            )
            .map_err(|e| {
                log::debug!("Error signing url: {}", e);
                tonic::Status::unauthenticated("Unable to authenticate user")
            })?,
        };
        Ok(tonic::Response::new(response))
    }
    async fn delete_bundle(
        &self,
        request: tonic::Request<DeleteBundleRequest>,
    ) -> std::result::Result<tonic::Response<DeleteBundleResponse>, tonic::Status> {
        if let Some(a) = self.cache.auth.read().await.as_ref() {
            let token = get_token_from_md(request.metadata())
                .map_err(|e| tonic::Status::unauthenticated(e.to_string()))?;
            let (u, tid) = a.check_permissions(&token).map_err(|e| {
                log::debug!("Error checking permissions: {}", e);
                tonic::Status::unauthenticated("Unable to authenticate user")
            })?;

            let access_key = if let Some(t_id) = tid {
                t_id
            } else {
                u.to_string()
            };

            let user = self.cache.users.get(&access_key).ok_or_else(|| {
                log::debug!("Error getting user from cache");
                tonic::Status::unauthenticated("Unable to authenticate user")
            })?;

            let bundle_id =
                DieselUlid::from_str(request.get_ref().bundle_id.as_str()).map_err(|e| {
                    log::debug!("Error parsing ULID: {}", e);
                    tonic::Status::invalid_argument("Unable to parse BundleID")
                })?;

            if let Some(perm) = user.value().permissions.get(&bundle_id) {
                if *perm == DbPermissionLevel::Admin {
                    return Ok(tonic::Response::new(DeleteBundleResponse {}));
                }
            }
        }
        return Err(tonic::Status::unauthenticated(
            "Unable to authenticate user",
        ));
    }
}
