use crate::{
    auth::auth_helpers::get_token_from_md,
    caching::cache::Cache,
    helpers::sign_download_url,
    structs::{Bundle, DbPermissionLevel},
};
use aruna_rust_api::api::dataproxy::services::v2::{
    bundler_service_server::BundlerService, CreateBundleRequest, CreateBundleResponse,
    DeleteBundleRequest, DeleteBundleResponse,
};
use diesel_ulid::DieselUlid;
use std::{str::FromStr, sync::Arc};
use tracing::error;

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
        if let Some(a) = self.cache.auth.read().await.as_ref() {
            // Query token
            let token = get_token_from_md(request.metadata()).map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                tonic::Status::unauthenticated(e.to_string())
            })?;
            // Check if permissions are valid
            let (u, tid, _) = a.check_permissions(&token).map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                tonic::Status::unauthenticated(format!("Unable to authenticate user"))
            })?;

            // Gather access_key
            let access_key = tid.unwrap_or_else(|| u.to_string());
            let permissions = self.cache.get_key_perms(&access_key).ok_or_else(|| {
                error!("Missing permissions for user");
                tonic::Status::unauthenticated(format!("Unable to authenticate user"))
            })?;

            let request = request.into_inner();

            let res_ids = request
                .resource_ids
                .iter()
                .map(|id| {
                    DieselUlid::from_str(id.as_str()).map_err(|e| {
                        error!(error = ?e, msg = e.to_string());
                        tonic::Status::invalid_argument(format!("Unable to parse resource_id"))
                    })
                })
                .collect::<Result<Vec<DieselUlid>, tonic::Status>>()?;

            for id in &res_ids {
                self.cache
                    .check_access_parents(&permissions, id, DbPermissionLevel::Read)
                    .await
                    .map_err(|e| {
                        error!(error = ?e, msg = e.to_string());
                        tonic::Status::unauthenticated(format!("Unable to authenticate user"))
                    })?;
            }

            let bundle_id = DieselUlid::generate();
            let bundle = Bundle {
                id: bundle_id,
                owner_access_key: access_key.clone(),
                ids: res_ids,
                expires_at: request.expires_at.map(|e| e.into()),
                once: request.once,
            };

            self.cache.add_bundle(bundle);

            let response = CreateBundleResponse {
                bundle_url: sign_download_url(
                    &access_key,
                    &permissions.secret,
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
        } else {
            error!("Unable to authenticate user");
            Err(tonic::Status::unauthenticated(
                "Unable to authenticate user",
            ))
        }
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

            let (u, tid, _) = a.check_permissions(&token).map_err(|_| {
                error!(error = "Unable to authenticate user");
                tonic::Status::unauthenticated("Unable to authenticate user")
            })?;

            let access_key = if let Some(t_id) = tid {
                t_id
            } else {
                u.to_string()
            };

            let user = self.cache.get_key_perms(&access_key).ok_or_else(|| {
                error!("Missing permissions for user");
                tonic::Status::unauthenticated("Unable to authenticate user")
            })?;

            let bundle_id =
                DieselUlid::from_str(request.get_ref().bundle_id.as_str()).map_err(|_| {
                    error!(error = "Unable to parse BundleID");
                    tonic::Status::invalid_argument("Unable to parse BundleID")
                })?;

            self.cache
                .check_delete_bundle(&bundle_id, &user.access_key)
                .map_err(|e| {
                    error!(error = ?e, "Unable to delete bundle");
                    tonic::Status::unauthenticated("Unable to delete bundle")
                })?;
            return Ok(tonic::Response::new(DeleteBundleResponse {}));
        }

        error!("Unable to authenticate user");
        return Err(tonic::Status::unauthenticated(
            "Unable to authenticate user",
        ));
    }
}
