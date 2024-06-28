use crate::auth::permission_handler::PermissionHandler;
use crate::auth::structs::Context;
use crate::caching::cache::Cache;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::utils::grpc_utils::get_token_from_md;
use aruna_rust_api::api::storage::services::v2::storage_status_service_server::StorageStatusService;
use aruna_rust_api::api::storage::services::v2::{
    GetAnnouncementRequest, GetAnnouncementResponse, GetAnnouncementsRequest,
    GetAnnouncementsResponse, GetPubkeysRequest, GetPubkeysResponse, GetStorageStatusRequest,
    GetStorageStatusResponse, GetStorageVersionRequest, GetStorageVersionResponse,
    SetAnnouncementsRequest, SetAnnouncementsResponse,
};
use diesel_ulid::DieselUlid;
use std::str::FromStr;
use std::sync::Arc;
use tonic::Response;

crate::impl_grpc_server!(StorageStatusServiceImpl);

#[tonic::async_trait]
impl StorageStatusService for StorageStatusServiceImpl {
    /// GetStorageVersion
    ///
    /// Status: BETA
    ///
    /// A request to get the current version of the server application
    /// String representation and https://semver.org/
    async fn get_storage_version(
        &self,
        _request: tonic::Request<GetStorageVersionRequest>,
    ) -> Result<Response<GetStorageVersionResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Nothing to see here!"))
    }
    /// GetStorageStatus
    ///
    /// Status: ALPHA
    ///
    /// A request to get the current status of the storage components by location(s)
    async fn get_storage_status(
        &self,
        _request: tonic::Request<GetStorageStatusRequest>,
    ) -> Result<Response<GetStorageStatusResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Nothing to see here!"))
    }

    async fn get_pubkeys(
        &self,
        _request: tonic::Request<GetPubkeysRequest>,
    ) -> Result<Response<GetPubkeysResponse>, tonic::Status> {
        let pubkeys = self.cache.get_pubkeys();

        let response = GetPubkeysResponse { pubkeys };

        Ok(Response::new(response))
    }

    async fn get_announcements(
        &self,
        request: tonic::Request<GetAnnouncementsRequest>,
    ) -> tonic::Result<Response<GetAnnouncementsResponse>> {
        log_received!(&request);

        // Just fetch the announcements from the database
        let announcements = tonic_internal!(
            self.database_handler.get_announcements().await,
            "Get announcements failed"
        );

        // Return all announcements
        let response = GetAnnouncementsResponse { announcements };
        return_with_log!(response);
    }

    async fn get_announcement(
        &self,
        request: tonic::Request<GetAnnouncementRequest>,
    ) -> tonic::Result<Response<GetAnnouncementResponse>> {
        log_received!(&request);

        // Check if provided id is valid
        let announcement_id = tonic_invalid!(
            DieselUlid::from_str(&request.into_inner().id),
            "Invalid announcement id format"
        );

        // Just fetch the announcement from the database
        let announcement = tonic_internal!(
            self.database_handler
                .get_announcement(announcement_id)
                .await,
            "Get announcement failed"
        );

        // Return all announcements
        let response = GetAnnouncementResponse {
            announcement: Some(announcement),
        };
        return_with_log!(response);
    }

    async fn set_announcements(
        &self,
        request: tonic::Request<SetAnnouncementsRequest>,
    ) -> tonic::Result<Response<SetAnnouncementsResponse>> {
        log_received!(&request);

        // Check if user is global admin
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let user_id = tonic_auth!(
            self.authorizer
                .check_permissions(&token, vec![Context::admin()])
                .await,
            "Unauthorized"
        );

        // Upsert/Delete provided announcements
        let announcements = tonic_internal!(
            self.database_handler
                .set_announcements(user_id, request.into_inner())
                .await,
            "Set announcements failed"
        );

        // Return announcements
        let response = SetAnnouncementsResponse { announcements };
        return_with_log!(response);
    }
}
