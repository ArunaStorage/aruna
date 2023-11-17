use crate::auth::permission_handler::PermissionHandler;
use crate::auth::structs::Context;
use crate::caching::cache::Cache;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::utils::conversions::get_token_from_md;
use aruna_rust_api::api::storage::services::v2::license_service_server::LicenseService;
use aruna_rust_api::api::storage::services::v2::{
    CreateLicenseRequest, CreateLicenseResponse, GetLicenseRequest, GetLicenseResponse,
    ListLicensesRequest, ListLicensesResponse,
};
use std::sync::Arc;
use tonic::{Request, Response, Result};

crate::impl_grpc_server!(LicensesServiceImpl);

#[tonic::async_trait]
impl LicenseService for LicensesServiceImpl {
    async fn create_license(
        &self,
        request: Request<CreateLicenseRequest>,
    ) -> Result<Response<CreateLicenseResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let request = request.into_inner();
        let ctx = Context::self_ctx();
        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let tag = tonic_internal!(
            self.database_handler.create_license(request).await,
            "Internal license creation error"
        );

        let response = CreateLicenseResponse { tag };
        return_with_log!(response);
    }
    async fn get_license(
        &self,
        request: Request<GetLicenseRequest>,
    ) -> Result<Response<GetLicenseResponse>> {
        log_received!(&request);

        //let token = tonic_auth!(
        //    get_token_from_md(request.metadata()),
        //    "Token authentication error"
        //);

        let request = request.into_inner();
        // let ctx = Context::self_ctx();
        // tonic_auth!(
        //     self.authorizer.check_permissions(&token, vec![ctx]).await,
        //     "Unauthorized"
        // );

        let license = tonic_internal!(
            self.database_handler.get_license(request.tag).await,
            "License fetching error"
        );

        let response = GetLicenseResponse {
            license: Some(license.into()),
        };
        return_with_log!(response);
    }
    async fn list_licenses(
        &self,
        request: Request<ListLicensesRequest>,
    ) -> Result<Response<ListLicensesResponse>> {
        log_received!(&request);

        // let token = tonic_auth!(
        //     get_token_from_md(request.metadata()),
        //     "Token authentication error"
        // );

        // let ctx = Context::self_ctx();
        // tonic_auth!(
        //     self.authorizer.check_permissions(&token, vec![ctx]).await,
        //     "Unauthorized"
        // );

        let licenses = tonic_internal!(
            self.database_handler.list_licenses().await,
            "License fetching error"
        );

        let response = ListLicensesResponse {
            licenses: licenses.into_iter().map(|l| l.into()).collect(),
        };
        return_with_log!(response);
    }
}
