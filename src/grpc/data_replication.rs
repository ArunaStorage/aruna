use crate::auth::structs::Context;
use crate::caching::cache::Cache;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::{auth::permission_handler::PermissionHandler, utils::conversions::get_token_from_md};
use aruna_rust_api::api::storage::services::v2::partial_replicate_data_request::DataVariant;
//use aruna_rust_api::api::storage::services::v2::partial_replicate_data_request::Response as Resource;
use aruna_rust_api::api::storage::services::v2::{
    data_replication_service_server::DataReplicationService, DeleteReplicationRequest,
    DeleteReplicationResponse, GetReplicationStatusRequest, GetReplicationStatusResponse,
    PartialReplicateDataRequest, PartialReplicateDataResponse, ReplicateProjectDataRequest,
    ReplicateProjectDataResponse, UpdateReplicationStatusRequest, UpdateReplicationStatusResponse,
};
use std::str::FromStr;
use std::sync::Arc;
use tonic::{Request, Response, Result};

crate::impl_grpc_server!(DataReplicationServiceImpl);

#[tonic::async_trait]
impl DataReplicationService for DataReplicationServiceImpl {
    async fn replicate_project_data(
        &self,
        request: Request<ReplicateProjectDataRequest>,
    ) -> Result<Response<ReplicateProjectDataResponse>> {
        log_received!(&request);

        // Consume gRPC request into its parts
        let (metadata, _, request) = request.into_parts();
        let project_id = tonic_invalid!(
            diesel_ulid::DieselUlid::from_str(&request.project_id),
            "Invalid project id"
        );
        // Extract token from request and check permissions
        let token = tonic_auth!(get_token_from_md(&metadata), "Token authentication error");

        // Check if allowed
        let ctx = Context::res_ctx(
            project_id,
            crate::database::enums::DbPermissionLevel::ADMIN,
            false,
        );
        let _user_id = tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let status = tonic_internal!(
            self.database_handler
                .replicate(
                    crate::middlelayer::replication_request_types::ReplicationVariant::Full(
                        request
                    )
                )
                .await,
            "Internal replication error"
        ) as i32;
        let response = ReplicateProjectDataResponse { status };
        return_with_log!(response);
    }
    async fn partial_replicate_data(
        &self,
        request: Request<PartialReplicateDataRequest>,
    ) -> Result<Response<PartialReplicateDataResponse>> {
        log_received!(&request);

        // Consume gRPC request into its parts
        let (metadata, _, request) = request.into_parts();
        let resource_id = match request.data_variant {
            Some(ref res) => match res {
                DataVariant::CollectionId(id) => diesel_ulid::DieselUlid::from_str(id),
                DataVariant::DatasetId(id) => diesel_ulid::DieselUlid::from_str(id),
                DataVariant::ObjectId(id) => diesel_ulid::DieselUlid::from_str(id),
            },
            None => {
                return Err(tonic::Status::invalid_argument("Invalid resource id"));
            }
        };
        let resource_id = tonic_invalid!(resource_id, "Invalid resource id");
        // Extract token from request and check permissions
        let token = tonic_auth!(get_token_from_md(&metadata), "Token authentication error");

        // Check if allowed
        let ctx = Context::res_ctx(
            resource_id,
            crate::database::enums::DbPermissionLevel::ADMIN,
            false,
        );
        let _user_id = tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let status = tonic_internal!(
            self.database_handler
                .replicate(
                    crate::middlelayer::replication_request_types::ReplicationVariant::Partial(
                        request
                    )
                )
                .await,
            "Internal replication error"
        ) as i32;

        let response = PartialReplicateDataResponse { status };
        return_with_log!(response);
    }
    async fn get_replication_status(
        &self,
        _request: Request<GetReplicationStatusRequest>,
    ) -> Result<Response<GetReplicationStatusResponse>> {
        Err(tonic::Status::unimplemented(
            "GetReplicationStatus not yet implemented",
        ))
    }
    async fn update_replication_status(
        &self,
        _request: Request<UpdateReplicationStatusRequest>,
    ) -> Result<Response<UpdateReplicationStatusResponse>> {
        Err(tonic::Status::unimplemented(
            "UpdateReplicationStatus not yet implemented",
        ))
    }
    async fn delete_replication(
        &self,
        _request: Request<DeleteReplicationRequest>,
    ) -> Result<Response<DeleteReplicationResponse>> {
        Err(tonic::Status::unimplemented(
            "DeleteReplicationStatus not yet implemented",
        ))
    }
}
